package net.snowflake.spark.snowflake.io

import java.sql.{ResultSet, Timestamp}

import net.snowflake.client.jdbc.SnowflakeResultSetSerializable
import org.apache.spark.sql.types.BinaryType
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.{ArrayType, BooleanType, ByteType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import net.snowflake.spark.snowflake.{SnowflakeConnectorException, Conversions}

import scala.reflect.ClassTag

class SnowflakeResultSetRDD[T: ClassTag](
                             schema: StructType,
                             sc: SparkContext,
                             resultSets: Array[SnowflakeResultSetSerializable]
                           ) extends RDD[T](sc, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    ResultIterator[T](schema, split.asInstanceOf[SnowflakeResultSetPartition].resultSet)

  override protected def getPartitions: Array[Partition] =
    resultSets.zipWithIndex.map {
      case (resultSet, index) => {
        SnowflakeResultSetPartition(resultSet, index)
      }
    }
}

case class ResultIterator[T: ClassTag](
                                        schema: StructType,
                                        resultSet: SnowflakeResultSetSerializable
                                      ) extends Iterator[T] {
  val data: ResultSet = resultSet.getResultSet()
  val isIR: Boolean = isInternalRow[T]
  val mapper: ObjectMapper = new ObjectMapper()

  override def hasNext: Boolean = data.next()

  override def next(): T = {
    val converted = schema.fields.indices.map(
      index => {
        data.getObject(index + 1) // check null value, JDBC standard
        if (data.wasNull()) null
        else schema.fields(index).dataType match {
          case StringType =>
            if (isIR) UTF8String.fromString(data.getString(index + 1))
            else data.getString(index + 1)
          case dt: DecimalType =>
            if (isIR) Decimal(data.getBigDecimal(index + 1))
            else data.getBigDecimal(index + 1)
          case DoubleType => data.getDouble(index + 1)
          case BooleanType => data.getBoolean(index + 1)
          case _: ArrayType | _: MapType | _: StructType =>
            Conversions.jsonStringToRow[T](
              mapper.readTree(data.getString(index + 1)),
              schema.fields(index).dataType)
          case BinaryType =>
            //if (isIR) UTF8String.fromString(data.getString(index + 1))
            //else data.getString(index + 1)
            data.getBytes(index + 1)
          case DateType =>
            if (isIR) DateTimeUtils.fromJavaDate(data.getDate(index + 1))
            else data.getDate(index + 1)
          case ByteType => data.getByte(index + 1)
          case FloatType => data.getFloat(index + 1)
          case IntegerType => data.getInt(index + 1)
          case LongType => data.getLong(index + 1)
          case TimestampType =>
            if (isIR) DateTimeUtils.fromJavaTimestamp(data.getTimestamp(index + 1))
            else data.getTimestamp(index + 1)
          case ShortType => data.getShort(index + 1)
          case _ => {
            new UnsupportedOperationException(s"Unsupported type: ${schema.fields(index).dataType}")
          }
        }
      }
    )

    if (isIR) InternalRow.fromSeq(converted).asInstanceOf[T]
    else Row.fromSeq(converted).asInstanceOf[T]
  }

  private def isInternalRow[T: ClassTag]: Boolean = {
    val row = implicitly[ClassTag[Row]]
    val internalRow = implicitly[ClassTag[InternalRow]]

    implicitly[ClassTag[T]] match {
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
                                 ) extends Partition {
}