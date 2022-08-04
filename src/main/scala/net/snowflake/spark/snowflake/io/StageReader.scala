package net.snowflake.spark.snowflake.io

import java.sql.Connection

import net.snowflake.client.jdbc.SnowflakeResultSet
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake._
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.slf4j.{Logger, LoggerFactory}
import scala.language.postfixOps
import scala.util.Random

private[snowflake] object StageReader {

  private val mapper: ObjectMapper = new ObjectMapper()
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def readFromStage(sqlContext: SQLContext,
                    params: MergedParameters,
                    statement: SnowflakeSQLStatement,
                    format: SupportedFormat): RDD[String] = {
    val conn = DefaultJDBCWrapper.getConnector(params)
    val (storage, stage) =
      CloudStorageOperations.createStorageClient(params, conn)
    val compress = params.sfCompress
    val compressFormat = if (params.sfCompress) "gzip" else "none"

    Utils.genPrologueSql(params).foreach(x => x.execute(params.bindVariableEnabled)(conn))

    Utils.executePreActions(DefaultJDBCWrapper, conn, params, params.table)

    val prefix = Random.alphanumeric take 10 mkString ""

    val copyStatement = buildUnloadStatement(
      params,
      statement,
      s"@$stage/$prefix/",
      compressFormat,
      format
    )
    logger.info(s"Now executing below command to read from snowflake:\n${copyStatement.toString}")

    val startTime = System.currentTimeMillis()
    val res = try {
      if (params.isExecuteQueryWithSyncMode) {
        copyStatement.execute(params.bindVariableEnabled)(conn)
      } else {
        val asyncRs = copyStatement.executeAsync(bindVariableEnabled = false)(conn)
        val queryID = asyncRs.asInstanceOf[SnowflakeResultSet].getQueryID
        logger.info(s"The query ID for async reading from snowflake with COPY INTO LOCATION is: " +
          s"$queryID; The query ID URL is:\n${params.getQueryIDUrl(queryID)}")
        SparkConnectorContext.addRunningQuery(sqlContext.sparkContext, conn, queryID)
        // Call getMetaData() to wait fot the async query to be done
        // Note: do not call next() to wait for the query to be done because
        // it will change the ResultSet, so getCopyMissedFiles() doesn't work.
        asyncRs.getMetaData
        SparkConnectorContext.removeRunningQuery(sqlContext.sparkContext, conn, queryID)
        asyncRs
      }
    } catch {
      case th: Throwable => {
        // send telemetry message
        SnowflakeTelemetry.sendQueryStatus(
          conn,
          TelemetryConstValues.OPERATION_READ,
          copyStatement.getLastQueryID(),
          TelemetryConstValues.STATUS_FAIL,
          System.currentTimeMillis() - startTime,
          Some(th),
          "Hit exception when reading with COPY INTO LOCATION")
        // Re-throw the exception
        throw th
      }
    }
    val queryID = res.asInstanceOf[SnowflakeResultSet].getQueryID
    Utils.setLastSelectQueryId(queryID)

    // Verify it's the expected format
    val sch = res.getMetaData
    val queryID = res.asInstanceOf[SnowflakeResultSet].getQueryID
    if (sch.getColumnCount >= 3) {
      // Format V1 for COPY INTO LOCATION. The result format is:
      // rows_unloaded    input_bytes    output_bytes
      // Format V2 for COPY INTO LOCATION. The result format is:
      // ROW_COUNT    FILE_NAME     FILE_SIZE
      val thirdColumnName = sch.getColumnName(3)
      val thirdColumnType = sch.getColumnTypeName(3)
      if (("output_bytes".equalsIgnoreCase(thirdColumnName)
        || "FILE_SIZE".equalsIgnoreCase(thirdColumnName))
        && "number".equalsIgnoreCase(thirdColumnType))
      {
        var rowCount: Long = 0
        var dataSize: Long = 0
        while (res.next) {
          rowCount += res.getLong(1)
          dataSize += res.getLong(3)
        }
        sendEgressUsage(conn, queryID, rowCount, dataSize)
      } else {
        logger.warn(
          s"""The result format of COPY INTO LOCATION is not recognized.
             | $thirdColumnName $thirdColumnType""".stripMargin)
      }
    } else {
      logger.warn(
        s"""The result format of COPY INTO LOCATION is not recognized.
           | ${sch.getColumnCount}""".stripMargin)
    }

    Utils.executePostActions(DefaultJDBCWrapper, conn, params, params.table)

    SnowflakeTelemetry.send(conn.getTelemetry)

    val resultRDD = storage.download(
      sqlContext.sparkContext, format, compress, prefix)

    // The connection can't be closed before download because the spark driver
    // needs the connection to acquire the credential for distributed download.
    conn.close()

    resultRDD
  }

  private def buildUnloadStatement(
    params: MergedParameters,
    statement: SnowflakeSQLStatement,
    location: String,
    compression: String,
    format: SupportedFormat = SupportedFormat.CSV
  ): SnowflakeSQLStatement = {

    // Save the last SELECT so it can be inspected
    Utils.setLastSelect(statement.toString)

    val (formatStmt, queryStmt): (SnowflakeSQLStatement, SnowflakeSQLStatement) =
      format match {
        case SupportedFormat.CSV =>
          (
            ConstantString(s"""
                 |FILE_FORMAT = (
                 |    TYPE=CSV
                 |    COMPRESSION='$compression'
                 |    FIELD_DELIMITER='|'
                 |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
                 |    ESCAPE_UNENCLOSED_FIELD = none
                 |    NULL_IF= ()
                 |  )
                 |  """.stripMargin) !,
            ConstantString("FROM (") + statement + ")"
          )
        case SupportedFormat.JSON =>
          (
            ConstantString(s"""
                 |FILE_FORMAT = (
                 |    TYPE=JSON
                 |    COMPRESSION='$compression'
                 |)
                 |""".stripMargin) !,
            ConstantString("FROM (SELECT object_construct(*) FROM (") + statement + "))"
          )
      }

    val result = ConstantString(s"COPY INTO '$location'") + queryStmt +
      formatStmt + "MAX_FILE_SIZE = " + params.s3maxfilesize

    Utils.setLastCopyUnload(result.toString)
    result

  }

  private[snowflake] def sendEgressUsage(conn: Connection,
                                         queryId: String,
                                         rowCount: Long,
                                         bytes: Long): Unit = {
    val metric: ObjectNode = mapper.createObjectNode()
    metric.put(TelemetryFieldNames.OUTPUT_BYTES, bytes)
    metric.put(TelemetryFieldNames.ROW_COUNT, rowCount)
    metric.put(TelemetryFieldNames.QUERY_ID, queryId)
    SnowflakeTelemetry.addCommonFields(metric)

    SnowflakeTelemetry.addLog(
      (TelemetryTypes.SPARK_EGRESS, metric),
      System.currentTimeMillis()
    )
    SnowflakeTelemetry.send(conn.getTelemetry)
    logger.debug(s"Data Egress Usage: $bytes bytes, $rowCount rows".stripMargin)
  }
}
