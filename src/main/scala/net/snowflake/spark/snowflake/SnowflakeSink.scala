package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.io.{CloudStorage, CloudStorageOperations, SupportedFormat}
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerJobEnd}
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

  require(
    param.privateKey.isDefined,
    "Private key must be specified in Snowflake streaming"
  )

  require(
    param.streamingStage.isDefined,
    "Streaming stage name must be specified with 'streaming_stage' parameter"
  )

  require(
    param.rootTempDir.isEmpty,
    "Spark Streaming only supports internal stages, please unset tempDir parameter."
  )

  private implicit val conn = DefaultJDBCWrapper.getConnector(param)

  private val stageName: String = param.streamingStage.get

  private implicit val storage: CloudStorage =
    CloudStorageOperations.createStorageClient(param, conn, false, Some(stageName))._1

  private val tableName = param.table.get

  private lazy val tableSchema = DefaultJDBCWrapper.resolveTable(conn, tableName.toString, param)

  private lazy val pipeName: String = init()

  private lazy val format: SupportedFormat =
    if (Utils.containVariant(schema.get)) SupportedFormat.JSON
    else SupportedFormat.CSV

  private lazy val ingestService: SnowflakeIngestService =
    new SnowflakeIngestService(param, pipeName, storage, conn)


  private val compress: Boolean = param.sfCompress

  private var schema: Option[StructType] = None

  private val streamingStartTime: Long = System.currentTimeMillis()


  //telemetry
  private var lastMetricSendTime: Long = 0
  private val mapper = new ObjectMapper()
  private val metric: ObjectNode = mapper.createObjectNode()

  private val APP_NAME = "application_name"
  private val START_TIME = "start_time"
  private val END_TIME = "end_time"
  private val LOAD_RATE = "load_rate"
  private val DATA_BATCH = "data_batch"

  private val telemetrySendTime: Long = 10 * 60 * 1000 // 10 min

  //streaming start event
  sendStartTelemetry()

  /**
    * Create pipe
    */
  def init(): String = {

    //create table
    conn.createTable(tableName.name, schema.get, param, overwrite = false)

    val pipe = s"${STREAMING_OBJECT_PREFIX}_${PIPE_TOKEN}_$stageName"

    conn.createPipe(pipe, ConstantString(copySql(format))!, true)
    sqlContext.sparkContext.addSparkListener(
      new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          super.onApplicationEnd(applicationEnd)
          ingestService.close()

          //telemetry
          val time = System.currentTimeMillis()
          metric.put(END_TIME, time)
          metric.get(LOAD_RATE).asInstanceOf[ObjectNode].put(END_TIME, time)

          SnowflakeTelemetry.addLog(((TelemetryTypes.SPARK_STREAMING, metric), time))
          SnowflakeTelemetry.send(conn.getTelemetry)

          //streaming termination event
          sendEndTelemetry()
        }
      }
    )
    pipe
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
          val schema = DefaultJDBCWrapper.resolveTable(conn, tableName.name, param)
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

    def registerDataBatchToTelemetry():Unit = {
      val time = System.currentTimeMillis()
      if(lastMetricSendTime == 0){ //init
        metric.put(APP_NAME, (data.sparkSession.sparkContext.appName + streamingStartTime.toString).hashCode) //use hashcode, hide app name
        metric.put(START_TIME, time)
        lastMetricSendTime = time
        val rate = metric.putObject(LOAD_RATE)
        rate.put(START_TIME, time)
        rate.put(DATA_BATCH, 0)
      }
      val rate = metric.get(LOAD_RATE).asInstanceOf[ObjectNode]
      rate.put(DATA_BATCH, rate.get(DATA_BATCH).asInt() + 1)

      if(time - lastMetricSendTime > telemetrySendTime){
        rate.put(END_TIME, time)
        SnowflakeTelemetry.addLog(((TelemetryTypes.SPARK_STREAMING, metric.deepCopy()), time))
        SnowflakeTelemetry.send(conn.getTelemetry)
        lastMetricSendTime = time
        rate.put(START_TIME, time)
        rate.put(DATA_BATCH, 0)
      }

    }

    if(schema.isEmpty) schema = Some(data.schema)

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
    ingestService.ingestFiles(fileList)

    registerDataBatchToTelemetry()
  }

  private def sendStartTelemetry(): Unit = {
    val message: ObjectNode = mapper.createObjectNode()
    message.put(APP_NAME, (sqlContext.sparkSession.sparkContext.appName + streamingStartTime.toString).hashCode)
    message.put(START_TIME, streamingStartTime)

    SnowflakeTelemetry.addLog((TelemetryTypes.SPARK_STREAMING_START, message), streamingStartTime)
    SnowflakeTelemetry.send(conn.getTelemetry)

    log.info("Streaming started")

  }

  private def sendEndTelemetry(): Unit = {
    val endTime: Long = System.currentTimeMillis()
    val message: ObjectNode = mapper.createObjectNode()
    message.put(APP_NAME, (sqlContext.sparkSession.sparkContext.appName + streamingStartTime.toString).hashCode)
    message.put(START_TIME, streamingStartTime)
    message.put(END_TIME, endTime)

    SnowflakeTelemetry.addLog((TelemetryTypes.SPARK_STREAMING_END, message), endTime)
    SnowflakeTelemetry.send(conn.getTelemetry)

    log.info("Streaming stopped")
  }



}
