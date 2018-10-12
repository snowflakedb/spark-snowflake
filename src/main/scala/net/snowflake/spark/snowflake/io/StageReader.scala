package net.snowflake.spark.snowflake.io

import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake._
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

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

    buildUnloadStatement(
      params,
      statement,
      s"@$stage",
      compressFormat,
      format).execute(conn)

    storage.download(
      sqlContext.sparkContext,
      format,
      compress
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
