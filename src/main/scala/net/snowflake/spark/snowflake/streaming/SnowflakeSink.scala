package net.snowflake.spark.snowflake.streaming

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import net.snowflake.spark.snowflake._
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.io.{CloudStorage, CloudStorageOperations, SupportedFormat}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.snowflake.SparkStreamingFunctions.streamingToNonStreaming
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryListener}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
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

  private val stageName: String = {
    val name = param.streamingStage.get
    conn.createStage(name, overwrite = false, temporary = false)
    name
  }

  private implicit val storage: CloudStorage =
    CloudStorageOperations.createStorageClient(param, conn, false, Some(stageName))._1

  private val pipeName: String = s"${STREAMING_OBJECT_PREFIX}_${PIPE_TOKEN}_$stageName"

  private lazy val format: SupportedFormat =
    if (Utils.containVariant(schema.get)) SupportedFormat.JSON
    else SupportedFormat.CSV

  private lazy val ingestService: SnowflakeIngestService = {
    val service = openIngestionService(param, pipeName, format, schema.get, storage, conn)
    init()
    service
  }

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
  def init(): Unit = {
    sqlContext.sparkContext.addSparkListener(
      new SparkListener {
        override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
          super.onApplicationEnd(applicationEnd)
          closeAllIngestionService()

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

    sqlContext.sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {}

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =
        closeIngestionService(pipeName)
    }
    )
  }


  override def addBatch(batchId: Long, data: DataFrame): Unit = {

    def registerDataBatchToTelemetry(): Unit = {
      val time = System.currentTimeMillis()
      if (lastMetricSendTime == 0) { //init
        metric.put(APP_NAME, (data.sparkSession.sparkContext.appName + streamingStartTime.toString).hashCode) //use hashcode, hide app name
        metric.put(START_TIME, time)
        lastMetricSendTime = time
        val rate = metric.putObject(LOAD_RATE)
        rate.put(START_TIME, time)
        rate.put(DATA_BATCH, 0)
      }
      val rate = metric.get(LOAD_RATE).asInstanceOf[ObjectNode]
      rate.put(DATA_BATCH, rate.get(DATA_BATCH).asInt() + 1)

      if (time - lastMetricSendTime > telemetrySendTime) {
        rate.put(END_TIME, time)
        SnowflakeTelemetry.addLog(((TelemetryTypes.SPARK_STREAMING, metric.deepCopy()), time))
        SnowflakeTelemetry.send(conn.getTelemetry)
        lastMetricSendTime = time
        rate.put(START_TIME, time)
        rate.put(DATA_BATCH, 0)
      }

    }

    if (schema.isEmpty) schema = Some(data.schema)
    //prepare data
    val rdd =
      DefaultSnowflakeWriter.dataFrameToRDD(
        sqlContext,
        streamingToNonStreaming(sqlContext, data),
        param,
        format)
    if (!rdd.isEmpty()){
    //write to storage
    val fileList =
      CloudStorageOperations
        .saveToStorage(rdd, format, Some(batchId.toString), compress)
    ingestService.ingestFiles(fileList)

    registerDataBatchToTelemetry()
    }
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
