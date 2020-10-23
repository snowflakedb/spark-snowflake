package net.snowflake.spark.snowflake.io

import java.sql.ResultSet
import java.util.Properties

import net.snowflake.client.jdbc.{ErrorCode, SnowflakeResultSetSerializable, SnowflakeSQLException}
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.spark.snowflake.test.{TestHook, TestHookFlag}
import net.snowflake.spark.snowflake.{
  Conversions,
  ProxyInfo,
  SnowflakeConnectorException,
  SnowflakeTelemetry,
  TelemetryConstValues
}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

object SnowflakeResultSetRDD {
  private[snowflake] val logger = LoggerFactory.getLogger(getClass)
  private[snowflake] val MASTER_LOG_PREFIX = "Spark Connector Master"
  private[snowflake] val WORKER_LOG_PREFIX = "Spark Connector Worker"
}

class SnowflakeResultSetRDD[T: ClassTag](
  schema: StructType,
  sc: SparkContext,
  resultSets: Array[SnowflakeResultSetSerializable],
  proxyInfo: Option[ProxyInfo],
  queryID: String,
  sfFullURL: String
) extends RDD[T](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    ResultIterator[T](
      schema,
      split.asInstanceOf[SnowflakeResultSetPartition].resultSet,
      split.asInstanceOf[SnowflakeResultSetPartition].index,
      proxyInfo,
      queryID,
      sfFullURL
    )

  override protected def getPartitions: Array[Partition] =
    resultSets.zipWithIndex.map {
      case (resultSet, index) =>
        SnowflakeResultSetPartition(resultSet, index)
    }
}

case class ResultIterator[T: ClassTag](
  schema: StructType,
  resultSet: SnowflakeResultSetSerializable,
  partitionIndex: Int,
  proxyInfo: Option[ProxyInfo],
  queryID: String,
  sfFullURL: String
) extends Iterator[T] {
  val jdbcProperties: Properties = {
    val jdbcProperties = new Properties()
    // Set up proxy info if it is configured.
    proxyInfo match {
      case Some(proxyInfoValue) =>
        proxyInfoValue.setProxyForJDBC(jdbcProperties)
      case None =>
    }
    jdbcProperties
  }
  var actualReadRowCount: Long = 0
  val expectedRowCount: Long = resultSet.getRowCount
  val data: ResultSet = {
    try {
      SnowflakeResultSetRDD.logger.info(
        s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: Start reading
           | partition ID:$partitionIndex expectedRowCount=
           | $expectedRowCount
           |""".stripMargin.filter(_ >=
          ' '))

      // Inject Exception for test purpose
      TestHook.raiseExceptionIfTestFlagEnabled(
        TestHookFlag.TH_ARROW_FAIL_OPEN_RESULT_SET,
        "Negative test to raise error when opening a result set"
      )

      resultSet.getResultSet(
        SnowflakeResultSetSerializable
          .ResultSetRetrieveConfig
          .Builder
          .newInstance()
          .setProxyProperties(jdbcProperties)
          .setSfFullURL(sfFullURL)
          .build()
      )
    } catch {
      case th: Throwable => {
        // Send OOB telemetry message if reading failure happens
        SnowflakeTelemetry.sendTelemetryOOB(
          sfFullURL,
          this.getClass.getSimpleName,
          operation = TelemetryConstValues.OPERATION_READ,
          retryCount = 0,
          maxRetryCount = 0,
          success = false,
          proxyInfo.isDefined,
          Some(queryID),
          Some(th))
        // Re-throw the exception
        throw th
      }
    }
  }

  val isIR: Boolean = isInternalRow[T]
  val mapper: ObjectMapper = new ObjectMapper()
  var currentRowNotConsumedYet: Boolean = false

  override def hasNext: Boolean = {
    // In some cases, hasNext() may be called but next() isn't.
    // This will cause the row to be 'skipped'.
    // So currentRowNotConsumedYet is introduced to make hasNext()
    // can be called repeatedly.
    if (currentRowNotConsumedYet) {
      return currentRowNotConsumedYet
    }

    try {
      if (data.next()) {
        // Move to the current row in the ResultSet, but it is not consumed yet.
        currentRowNotConsumedYet = true
        true
      } else {
        SnowflakeResultSetRDD.logger.info(
          s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: Finish reading
             | partition ID:$partitionIndex expectedRowCount=$expectedRowCount
             | actualReadRowCount=$actualReadRowCount
             |""".stripMargin.filter(_ >= ' '))

        // Close the result set.
        data.close()

        // Inject Exception for test purpose
        TestHook.raiseExceptionIfTestFlagEnabled(
          TestHookFlag.TH_ARROW_FAIL_READ_RESULT_SET,
          "Negative test to raise error when retrieve rows"
        )

        if (actualReadRowCount != expectedRowCount) {
          throw new SnowflakeSQLException(ErrorCode.INTERNAL_ERROR,
            s"""The actual read row count $actualReadRowCount is not equal to
               | the expected row count $expectedRowCount for partition
               | ID:$partitionIndex. Related query ID is $queryID
               |""".stripMargin.filter(_ >= ' '))
        }
        false
      }
    } catch {
      case th: Throwable => {
        // Send OOB telemetry message if reading failure happens
        SnowflakeTelemetry.sendTelemetryOOB(
          sfFullURL,
          this.getClass.getSimpleName,
          operation = TelemetryConstValues.OPERATION_READ,
          retryCount = 0,
          maxRetryCount = 0,
          success = false,
          useProxy = proxyInfo.isDefined,
          queryID = Some(queryID),
          throwable = Some(th))
        // Re-throw the exception
        throw th
      }
    }
  }

  override def next(): T = {
    val converted = schema.fields.indices.map(index => {
      data.getObject(index + 1) // check null value, JDBC standard
      if (data.wasNull()) {
        null
      } else {
        schema.fields(index).dataType match {
          case StringType =>
            if (isIR) {
              UTF8String.fromString(data.getString(index + 1))
            } else {
              data.getString(index + 1)
            }
          case _: DecimalType =>
            if (isIR) {
              Decimal(data.getBigDecimal(index + 1))
            } else {
              data.getBigDecimal(index + 1)
            }
          case DoubleType => data.getDouble(index + 1)
          case BooleanType => data.getBoolean(index + 1)
          case _: ArrayType | _: MapType | _: StructType =>
            Conversions.jsonStringToRow[T](
              mapper.readTree(data.getString(index + 1)),
              schema.fields(index).dataType
            )
          case BinaryType =>
            // if (isIR) UTF8String.fromString(data.getString(index + 1))
            // else data.getString(index + 1)
            data.getBytes(index + 1)
          case DateType =>
            if (isIR) {
              DateTimeUtils.fromJavaDate(data.getDate(index + 1))
            } else {
              data.getDate(index + 1)
            }
          case ByteType => data.getByte(index + 1)
          case FloatType => data.getFloat(index + 1)
          case IntegerType => data.getInt(index + 1)
          case LongType => data.getLong(index + 1)
          case TimestampType =>
            if (isIR) {
              DateTimeUtils.fromJavaTimestamp(data.getTimestamp(index + 1))
            } else {
              data.getTimestamp(index + 1)
            }
          case ShortType => data.getShort(index + 1)
          case _ =>
            new UnsupportedOperationException(
              s"Unsupported type: ${schema.fields(index).dataType}"
            )
        }
      }
    })

    // Increase actual read row count
    actualReadRowCount += 1

    // The row is consumed, the iterator can move to next row.
    currentRowNotConsumedYet = false

    if (isIR) InternalRow.fromSeq(converted).asInstanceOf[T]
    else Row.fromSeq(converted).asInstanceOf[T]
  }

  private def isInternalRow[U: ClassTag]: Boolean = {
    val row = implicitly[ClassTag[Row]]
    val internalRow = implicitly[ClassTag[InternalRow]]

    implicitly[ClassTag[U]] match {
      case `row` => false
      case `internalRow` => true
      case _ =>
        throw new SnowflakeConnectorException("Wrong type for convertRow.")
    }
  }
}

private case class SnowflakeResultSetPartition(
  resultSet: SnowflakeResultSetSerializable,
  index: Int
) extends Partition {}
