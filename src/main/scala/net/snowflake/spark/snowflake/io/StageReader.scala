package net.snowflake.spark.snowflake.io

import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake._
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.util.Random

private[io] object StageReader {

  def readFromStage(
                     sqlContext: SQLContext,
                     params: MergedParameters,
                     statement: SnowflakeSQLStatement,
                     format: SupportedFormat
                   ): RDD[String] = {
    val conn = DefaultJDBCWrapper.getConnector(params)
    val (storage, stage) =
      CloudStorageOperations.createStorageClient(params, conn)
    val compress = params.sfCompress
    val compressFormat = if (params.sfCompress) "gzip" else "none"

    Utils.genPrologueSql(params).execute(conn)

    Utils.executePreActions(DefaultJDBCWrapper, conn, params, params.table)

    val prefix = Random.alphanumeric take 10 mkString ""

    val res = buildUnloadStatement(
      params,
      statement,
      s"@$stage/$prefix/",
      compressFormat,
      format).execute(conn)


    // Verify it's the expected format
    val sch = res.getMetaData
    assert(sch.getColumnCount == 3)
    assert(sch.getColumnName(1) == "rows_unloaded")
    assert(sch.getColumnTypeName(1) == "NUMBER") // First record must be in
    val first = res.next()
    assert(first)
    val second = res.next()
    assert(!second)

    Utils.executePostActions(DefaultJDBCWrapper, conn, params, params.table)

    SnowflakeTelemetry.send(conn.getTelemetry)

    storage.download(
      sqlContext.sparkContext,
      format,
      compress,
      prefix
    )
  }


  private def buildUnloadStatement(
                                    params: MergedParameters,
                                    statement: SnowflakeSQLStatement,
                                    location: String,
                                    compression: String,
                                    format: SupportedFormat = SupportedFormat.CSV
                                  ): SnowflakeSQLStatement = {


    // Save the last SELECT so it can be inspected
    Utils.setLastCopyUnload(statement.toString)

    val (formatStmt, queryStmt): (SnowflakeSQLStatement, SnowflakeSQLStatement) =
      format match {
        case SupportedFormat.CSV =>
          (
            ConstantString(
              s"""
                 |FILE_FORMAT = (
                 |    TYPE=CSV
                 |    COMPRESSION='$compression'
                 |    FIELD_DELIMITER='|'
                 |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
                 |    ESCAPE_UNENCLOSED_FIELD = none
                 |    NULL_IF= ()
                 |  )
                 |  """.stripMargin
            ) !,
            ConstantString("FROM (") + statement + ")"
          )
        case SupportedFormat.JSON =>
          (
            ConstantString(
              s"""
                 |FILE_FORMAT = (
                 |    TYPE=JSON
                 |    COMPRESSION='$compression'
                 |)
                 |""".stripMargin
            ) !,
            ConstantString("FROM (SELECT object_construct(*) FROM (") + statement + "))"
          )
      }

    ConstantString(s"COPY INTO '$location'") + queryStmt +
      formatStmt + "MAX_FILE_SIZE = " + params.s3maxfilesize


  }
}
