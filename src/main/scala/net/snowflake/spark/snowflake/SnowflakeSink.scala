package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.io.{CloudStorageOperations, SupportedFormat}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.slf4j.LoggerFactory

class SnowflakeSink(
                     sqlContext: SQLContext,
                     parameters: Map[String, String],
                     partitionColumns: Seq[String],
                     outputMode: OutputMode
                   ) extends Sink{
  private val log = LoggerFactory.getLogger(getClass)

  private val param = Parameters.mergeParameters(parameters)

  //discussion: Do we want to support overwrite mode?
  //In Spark Streaming, there are only three mode append, complete, update
  require(
    outputMode == OutputMode.Append(),
    "Snowflake streaming only supports append mode"
  )

  require(
    param.table.isDefined,
    "Snowflake table name must be specified with the 'dbtable' parameter"
  )

  val conn = DefaultJDBCWrapper.getConnector(param)

  private val prologueSql = Utils.genPrologueSql(param)
  log.debug(prologueSql)
  DefaultJDBCWrapper.executeInterruptibly(conn, prologueSql)

  val (storage, stageName) = CloudStorageOperations.createStorageClient(param, conn)

  private val tableName = param.table.get

  private var pipeName: Option[String] = None

  private var format: Option[SupportedFormat] = None

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    //initialize pipe
    if (pipeName.isEmpty) {
      val schemaSql = DefaultJDBCWrapper.schemaString(data.schema)
      val createTableSql =
        s"""
           |create table if not exists $tableName ($schemaSql)
         """.stripMargin
      log.debug(createTableSql)
      DefaultJDBCWrapper.executeQueryInterruptibly(conn, createTableSql)

      format = Some(
        if(Utils.containVariant(data.schema)) SupportedFormat.JSON
        else SupportedFormat.CSV
      )

      pipeName = Some(s"tmp_spark_pipe_${System.currentTimeMillis()}")

      val formatString =
        format match {
          case Some(SupportedFormat.CSV) =>
            s"""
               |FILE_FORMAT = (
               |    TYPE=CSV
               |    FIELD_DELIMITER='|'
               |    NULL_IF=()
               |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
               |    TIMESTAMP_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF3'
               |  )
           """.stripMargin
          case Some(SupportedFormat.JSON) =>
            s"""
               |FILE_FORMAT = (
               |    TYPE = JSON
               |)
           """.stripMargin
        }
      val createPipeStatement =
        s"""
           |create or replace pipe ${pipeName.get}
           |as copy into $tableName
           |from @$stageName
           |$formatString
           |""".stripMargin
      log.debug(createPipeStatement)
      DefaultJDBCWrapper.executeQueryInterruptibly(conn, createPipeStatement)
    }

    //prepare data
    val rdd = DefaultSnowflakeWriter.dataFrameToRDD(sqlContext, data, param, format.get)

    //write to storage

    rdd.mapPartitions[String](part=>{
      null
    })

  }


}
