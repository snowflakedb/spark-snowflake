package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.io.{CloudStorageOperations, SupportedFormat}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.snowflake.SparkStreamingFunctions.streamingToNonStreaming
import org.slf4j.LoggerFactory

import scala.util.{Random, Try}

class SnowflakeSink(
                     sqlContext: SQLContext,
                     parameters: Map[String, String],
                     partitionColumns: Seq[String],
                     outputMode: OutputMode
                   ) extends Sink {
  private val STREAMING_OBJECT_PREFIX = "TMP_SPARK"
  private val PIPE_TOKEN = "PIPE"

  private val log = LoggerFactory.getLogger(getClass)

  private val param = Parameters.mergeParameters(parameters)

  private var fileFailed: Boolean = false

  private var config: StreamingConfiguration = null

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

  require(
    param.getPublicKeyPath.isDefined && param.getPrivateKeyPath.isDefined,
    "key pair's path must be specified in Snowflake streaming"
  )

  val conn = DefaultJDBCWrapper.getConnector(param)

  private var stageName: String = _

  private implicit lazy val storage =
    if(stageName == null){
      val result = CloudStorageOperations.createStorageClient(param, conn, false)
      stageName = result._2
      result._1
    }
    else CloudStorageOperations.createStorageClientFromStage(param, conn, stageName)

  private val tableName = param.table.get

  private lazy val tableSchema = DefaultJDBCWrapper.resolveTable(conn, tableName.toString)

  private var pipeName: Option[String] = None

  private var format: SupportedFormat = _

  private implicit lazy val ingestManager =
    SnowflakeIngestConnector.createIngestManager(param, pipeName.get)

  private var lastBatchId: Long = -1

  private val compress: Boolean = param.sfCompress

  //if checkpoint unassigned, should never be used
  implicit private lazy val logStorage: LogStorageClient =
    new LogStorageClient(param, conn, param.streamingCheckPoint.get)

  /**
    * Create pipe, stage, and storage client
    */
  def init(data: DataFrame): Unit = {

    val schema = data.schema
    storage // init

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

    val createPipeStatement =
      s"""
         |create or replace pipe ${pipeName.get}
         |as ${copySql(format)}
         |""".stripMargin
    log.debug(createPipeStatement)
    DefaultJDBCWrapper.executeQueryInterruptibly(conn, createPipeStatement)

    data.sparkSession.sparkContext.addSparkListener(
      new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          super.onApplicationEnd(applicationEnd)

          if(param.streamingCheckPoint.isDefined){
            if (!fileFailed) dropStage(stageName)
            if (lastBatchId >= 0)
              StreamingBatchLog.mergeBatchLog(lastBatchId / StreamingBatchLog.GROUP_SIZE)
          }
          dropPipe(pipeName.get)

        }
      }
    )
    config = StreamingConfiguration(stageName, pipeName.get)
    config.save

  }

  /**
    * Generate the COPY SQL command for creating pipe only
    */
  private def copySql(
                       format: SupportedFormat
                     ): String = {

    def getMappingToString(list: Option[List[(Int, String)]]): String =
      format match {
        case SupportedFormat.JSON =>
          val schema = DefaultJDBCWrapper.resolveTable(conn, tableName.name)
          if (list.isEmpty || list.get.isEmpty)
            s"(${schema.fields.map(x => Utils.ensureQuoted(x.name)).mkString(",")})"
          else s"(${list.get.map(x => Utils.ensureQuoted(x._2)).mkString(", ")})"
        case SupportedFormat.CSV =>
          if (list.isEmpty || list.get.isEmpty) ""
          else s"(${list.get.map(x => Utils.ensureQuoted(x._2)).mkString(", ")})"
      }

    def getMappingFromString(list: Option[List[(Int, String)]], from: String): String =
      format match {
        case SupportedFormat.JSON =>
          if (list.isEmpty || list.get.isEmpty) {
            val names =
              tableSchema
                .fields
                .map(x => "parse_json($1):".concat(Utils.ensureQuoted(x.name)))
                .mkString(",")
            s"from (select $names $from tmp)"
          }
          else
            s"from (select ${list.get.map(x => "parse_json($1):".concat(Utils.ensureQuoted(tableSchema(x._1 - 1).name))).mkString(", ")} $from tmp)"
        case SupportedFormat.CSV =>
          if (list.isEmpty || list.get.isEmpty) from
          else
            s"from (select ${list.get.map(x => "tmp.$".concat(Utils.ensureQuoted(x._1.toString))).mkString(", ")} $from tmp)"
      }

    val fromString = s"FROM @$stageName"

    val mappingList: Option[List[(Int, String)]] = param.columnMap match {
      case Some(map) =>
        Some(map.toList.map {
          case (key, value) =>
            try {
              (tableSchema.fieldIndex(key) + 1, value)
            } catch {
              case e: Exception => {
                log.error("Error occurred while column mapping: " + e)
                throw e
              }
            }
        })

      case None => None
    }

    val mappingToString = getMappingToString(mappingList)

    val mappingFromString = getMappingFromString(mappingList, fromString)

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

    s"""
       |COPY INTO $tableName $mappingToString
       |$mappingFromString
       |$formatString
    """.stripMargin.trim
  }


  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    lastBatchId = batchId

    param.streamingCheckPoint match {
      case Some(_) =>
        if(config == null && StreamingConfiguration.logExists){
          config = StreamingConfiguration.loadLog
          stageName = config.getStageName
          pipeName = Some(config.getPipeName)
        }
        val (files, batchLog) =
          if (!StreamingBatchLog.logExists(batchId)) {
            if (pipeName.isEmpty) init(data)

            //prepare data
            val rdd =
              DefaultSnowflakeWriter.dataFrameToRDD(
                sqlContext,
                streamingToNonStreaming(sqlContext, data),
                param,
                format)

            //write to storage
            val fileList =
              CloudStorageOperations
                .saveToStorage(rdd, format, Some(batchId.toString), compress)

            //write file names to log file
            val batchLog = StreamingBatchLog(batchId, fileList)
            batchLog.save

            (fileList, batchLog)
          } else {
            val batchLog = StreamingBatchLog.loadLog(batchId)
            val fileList = batchLog.getFileList
            (fileList, batchLog)
          }

        fileFailed = fileFailed || batchLog.hasFileFailed

        //merge batch logs
        if (batchId % StreamingBatchLog.GROUP_SIZE == 0 && batchId != 0) {
          val groupId: Long = batchId / StreamingBatchLog.GROUP_SIZE - 1
          if (!StreamingFailedFileReport.logExists(groupId))
            StreamingBatchLog.mergeBatchLog(groupId)
        }

        if (batchLog.getLoadedFileList.isEmpty) {
          //todo: time out
          val failedFiles = SnowflakeIngestConnector.ingestFilesAndCheck(files)
          if (failedFiles.nonEmpty) fileFailed = true
          if (fileFailed) batchLog.fileFailed
          val loadedFiles = files.filterNot(failedFiles.toSet)
          batchLog.setLoadedFileNames(loadedFiles)
          batchLog.save

          //Purge files
          CloudStorageOperations.deleteFiles(loadedFiles)
        }
      case None =>
        if (pipeName.isEmpty) init(data)
        //prepare data
        val rdd =
          DefaultSnowflakeWriter.dataFrameToRDD(
            sqlContext,
            streamingToNonStreaming(sqlContext, data),
            param,
            format)
        //write to storage
        val fileList =
          CloudStorageOperations
            .saveToStorage(rdd, format, Some(batchId.toString), compress)
        SnowflakeIngestConnector.ingestFiles(fileList)
    }

  }


  private def pipeExists(pipe: String): Boolean = {
    val statement: String = s"desc pipe $pipe"
    Try {
      DefaultJDBCWrapper.executePreparedQueryInterruptibly(conn, statement)
    }.isSuccess
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
