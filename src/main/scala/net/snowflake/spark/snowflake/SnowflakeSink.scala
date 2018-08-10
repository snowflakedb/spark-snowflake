package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.io.{CloudStorageOperations, SupportedFormat}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobEnd, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.snowflake.SparkStreamingFunctions.streamingToNonStreaming
import org.slf4j.LoggerFactory

import scala.util.Random

class SnowflakeSink(
                     sqlContext: SQLContext,
                     parameters: Map[String, String],
                     partitionColumns: Seq[String],
                     outputMode: OutputMode
                   ) extends Sink {
  private val STREAMING_OBJECT_PREFIX = "TMP_SPARK"
  private val PIPE_TOKEN = "PIPE"
  private val STAGE_TOKEN = "STAGE"

  //todo: change to a large number
  private val PIPE_LIFETIME: Long = 100

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
    "Snowflake table name must be specified with 'dbtable' parameter"
  )

  val conn = DefaultJDBCWrapper.getConnector(param)

  private implicit lazy val (storage, stageName) =
    CloudStorageOperations
      .createStorageClient(
        param,
        conn,
        false
      )

  private val tableName = param.table.get

  private var pipeName: Option[String] = None

  private var format: SupportedFormat = _

  private implicit lazy val ingestManager =
    SnowflakeIngestConnector.createIngestManager(param, pipeName.get)

  private lazy val report =
    new SnowflakeStreamingReport(
      stageName,
      if(param.rootTempDir.isEmpty) None else Some(param.rootTempDir),
      param.streamingKeepFailedFiles
    )

  /**
    * Create pipe, stage, and storage client
    */
  def init(data: DataFrame): Unit = {

    val schema = data.schema

    report // initialize report

    //create table
    val schemaSql = DefaultJDBCWrapper.schemaString(schema)
    val createTableSql =
      s"""
         |create table if not exists $tableName ($schemaSql)
         """.stripMargin
    log.debug(createTableSql)
    DefaultJDBCWrapper.executeQueryInterruptibly(conn, createTableSql)

    //create pipe
    format =
      if (Utils.containVariant(schema)) SupportedFormat.JSON
      else SupportedFormat.CSV

    pipeName =
      Some(s"${STREAMING_OBJECT_PREFIX}_${PIPE_TOKEN}_${Random.alphanumeric take 10 mkString ""}")

    val formatString =
      format match {
        case SupportedFormat.CSV =>
          s"""
             |FILE_FORMAT = (
             |    TYPE=CSV
             |    FIELD_DELIMITER='|'
             |    NULL_IF=()
             |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
             |    TIMESTAMP_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF3'
             |  )
           """.stripMargin
        case SupportedFormat.JSON =>
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

    data.sparkSession.sparkContext.addSparkListener(
      new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          super.onApplicationEnd(applicationEnd)
          if (report.dropStage) dropStage(stageName)
          dropPipe(pipeName.get)
          println(report)
        }
      }
    )

  }


  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (pipeName.isEmpty) init(data)

    //prepare data
    val rdd =
      DefaultSnowflakeWriter.dataFrameToRDD(
        sqlContext,
        streamingToNonStreaming(sqlContext, data),
        param,
        format)

    //write to storage
    val files =
      CloudStorageOperations.saveToStorage(rdd, format, Some(batchId.toString))

    files.foreach(println)
    val failedFiles = SnowflakeIngestConnector.ingestFiles(files)

    report.addFailedFiles(failedFiles)

    //Purge files
    if(param.streamingKeepFailedFiles)
      CloudStorageOperations.deleteFiles(files.filterNot(failedFiles.toSet))
    else CloudStorageOperations.deleteFiles(files)

  }

  private def dropPipe(pipe: String): Unit = {
    val statement: String = s"drop pipe if exists $pipe"
    DefaultJDBCWrapper.executeQueryInterruptibly(conn, statement)
  }

  private def dropStage(stage: String): Unit = {
    val statement: String = s"drop stage if exists $stage"
    DefaultJDBCWrapper.executeQueryInterruptibly(conn, statement)
  }



}
