package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.io.{CloudStorage, CloudStorageOperations, SupportedFormat}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.snowflake.SparkStreamingFunctions.streamingToNonStreaming
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

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

  private val prologueSql = Utils.genPrologueSql(param)
  log.debug(prologueSql)
  DefaultJDBCWrapper.executeInterruptibly(conn, prologueSql)

  private implicit var storage: CloudStorage = _

  private var stageName: String = _

  private val tableName = param.table.get

  private var pipeName: Option[String] = None

  private var format: SupportedFormat = _

  private var pipeCreateTime: Long = _

  private implicit lazy val ingestManager =
    SnowflakeIngestConnector.createIngestManager(param, pipeName.get)

  //drop dead pipes and stages
  scanPipes()
  scanStages()

  /**
    * Create pipe, stage, and storage client
    */
  def init(schema: StructType): Unit = {
    val schemaSql = DefaultJDBCWrapper.schemaString(schema)
    val createTableSql =
      s"""
         |create table if not exists $tableName ($schemaSql)
         """.stripMargin
    log.debug(createTableSql)
    DefaultJDBCWrapper.executeQueryInterruptibly(conn, createTableSql)

    format =
      if (Utils.containVariant(schema)) SupportedFormat.JSON
      else SupportedFormat.CSV


    pipeCreateTime = System.currentTimeMillis()

    val storageInfo =
      CloudStorageOperations
        .createStorageClient(
          param,
          conn,
          false,
          Some(s"${STREAMING_OBJECT_PREFIX}_${STAGE_TOKEN}_${pipeCreateTime}"))

    storage = storageInfo._1
    stageName = storageInfo._2

    pipeName = Some(s"${STREAMING_OBJECT_PREFIX}_${PIPE_TOKEN}_${pipeCreateTime}")

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

  }


  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (pipeName.isEmpty) init(data.schema)

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

    //Purge files
    CloudStorageOperations.deleteFiles(files)

    if(System.currentTimeMillis() - pipeCreateTime > PIPE_LIFETIME * 1000) {
      dropPipe(pipeName.get)
      dropStage(stageName)
      pipeName = None
    }


  }

  private def dropPipe(pipe: String): Unit = {
    val statement: String = s"drop pipe if exists $pipe"
    DefaultJDBCWrapper.executeQueryInterruptibly(conn, statement)
  }

  private def dropStage(stage: String): Unit = {
    val statement: String = s"drop stage if exists $stage"
    DefaultJDBCWrapper.executeQueryInterruptibly(conn, statement)
  }

  /**
    * Scan pipe list, remove all dead pipes
    */
  private def scanPipes(): Unit = {
    val pipePattern = s"^${STREAMING_OBJECT_PREFIX}_${PIPE_TOKEN}_(\\d+)$$".r
    val statement: String = s"show pipes"
    val pipes = DefaultJDBCWrapper.executeQueryInterruptibly(conn, statement)
    while (pipes.next()) {
      val pipe = pipes.getString("name")
      pipe match {
        case pipePattern(time) =>
          val currentTime: Long = System.currentTimeMillis()
          val createTime: Long = time.toLong
          if (currentTime - createTime > PIPE_LIFETIME * 2000) //double lifetime
            dropPipe(pipe)
        case _ =>
      }
    }
  }

  /**
    * Scan stage list, remove all dead stages
    */
  private def scanStages(): Unit = {
    val stagePattern = s"^${STREAMING_OBJECT_PREFIX}_${STAGE_TOKEN}_(\\d+)$$".r
    val statement: String = s"show stages"
    val stages = DefaultJDBCWrapper.executeQueryInterruptibly(conn, statement)
    while (stages.next()) {
      val stage = stages.getString("name")
      stage match {
        case stagePattern(time) =>
          val currentTime: Long = System.currentTimeMillis()
          val createTime: Long = time.toLong
          if (currentTime - createTime > PIPE_LIFETIME * 2000) //double lifetime
            dropStage(stage)
        case _ =>
      }
    }
  }


}
