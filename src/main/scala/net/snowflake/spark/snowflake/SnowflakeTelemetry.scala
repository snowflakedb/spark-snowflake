package net.snowflake.spark.snowflake

import java.io.{PrintWriter, StringWriter}
import java.sql.Connection
import java.util.regex.Pattern
import net.snowflake.client.jdbc.SnowflakeSQLException
import net.snowflake.client.jdbc.telemetry.{Telemetry, TelemetryClient}
import org.apache.spark.sql.catalyst.plans.logical._
import org.slf4j.LoggerFactory
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.client.jdbc.telemetryOOB.{TelemetryEvent, TelemetryService}
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import net.snowflake.spark.snowflake.TelemetryTypes.TelemetryTypes
import org.apache.spark.{SparkConf, SparkEnv, TaskContext}

object SnowflakeTelemetry {

  // type/source/data are first level field names for spark telemetry message.
  // They should not be changed.
  private val TELEMETRY_TYPE_FIELD_NAME = "type"
  private val TELEMETRY_SOURCE_FIELD_NAME = "source"
  private val TELEMETRY_DATA_FIELD_NAME = "data"

  private val TELEMETRY_SOURCE = "spark_connector"
  private val TELEMETRY_OOB_NAME_PREFIX = "spark"

  private var logs: List[(ObjectNode, Long)] = Nil // data and timestamp
  private val logger = LoggerFactory.getLogger(getClass)
  private val mapper = new ObjectMapper()

  private[snowflake] val MB = 1024 * 1024

  private[snowflake] var output: ObjectNode = _

  private var telemetryMessageSender: TelemetryMessageSender = new SnowflakeTelemetryMessageSender

  private[snowflake] def setTelemetryMessageSenderForTest(sender: TelemetryMessageSender)
  : TelemetryMessageSender = {
    val oldSender = telemetryMessageSender
    telemetryMessageSender = sender
    oldSender
  }

  private var cachedSparkApplicationId: Option[String] = None

  private def getSparkApplicationId: String = {
    if (cachedSparkApplicationId.isEmpty) {
      if (SparkEnv.get != null
        && SparkEnv.get.conf != null
        && SparkEnv.get.conf.contains("spark.app.id")) {
        cachedSparkApplicationId = Some(SparkEnv.get.conf.get("spark.app.id"))
        cachedSparkApplicationId.get
      } else {
        s"spark.app.id not set ${System.currentTimeMillis()}}"
      }
    } else {
      cachedSparkApplicationId.get
    }
  }

  private[snowflake] def getSparkLibraries: Seq[String] = {
    try {
      val classNames = Thread.currentThread()
        .getStackTrace
        .reverse // reverse to make caller on the head
        .map(_.getClassName)
        .map(_.replaceAll("\\$", "")) // Replace $ for Scala object class

      val userApplicationPackage = if (classNames.head.contains(".")) {
        classNames.head.split("\\.").dropRight(1).mkString("", ".", ".")
      } else {
        classNames.head
      }
      // Skip known libraries and user's application package
      val knownLibraries = Seq("java.", "jdk.internal.", "scala.",
        "net.snowflake.spark.snowflake.", "org.apache.spark.sql.", userApplicationPackage)

      classNames
        .filterNot(name => knownLibraries.exists(name.startsWith)) // Remove known libraries
        .map { name => // Get package names
          if (name.contains(".")) {
            name.split("\\.").dropRight(1).mkString(".")
          } else {
            name
          }
        }
        .distinct
    } catch {
      case th: Throwable =>
        logger.warn(s"Fail to retrieve spark libraries. reason: ${th.getMessage}")
        Seq.empty
    }
  }

  private val MAX_CACHED_SPARK_PLAN_STATISTIC_COUNT = 1000

  private[snowflake] def addSparkPlanStatistic(statistic: Set[String]): Unit = {
    if (statistic.nonEmpty) {
      this.synchronized {
        if (logs.length >= MAX_CACHED_SPARK_PLAN_STATISTIC_COUNT) {
          return
        }
      }

      val metric: ObjectNode = mapper.createObjectNode()
      val arrayNode = metric.putArray(TelemetryFieldNames.STATISTIC_INFO)
      statistic.foreach(arrayNode.add)
      SnowflakeTelemetry.addCommonFields(metric)

      SnowflakeTelemetry.addLog(
        (TelemetryTypes.SPARK_PLAN_STATISTIC, metric),
        System.currentTimeMillis()
      )
    }
  }

  // Enable OOB (out-of-band) telemetry message service
  TelemetryService.enable()

  // This is used to send OOB telemetry without connection.
  // TelemetryService.getInstance returns a thread local instance
  // and TelemetryService internally may use TelemetryService.getInstance
  // to get instance and use it directly. So it doesn't work to cache
  // a TelemetryService object as a singleton in SnowflakeTelemetry.
  private def getOobTelemetryService(): TelemetryService = {
      val service = TelemetryService.getInstance
      service.setDeployment(TelemetryService.TELEMETRY_SERVER_DEPLOYMENT.PROD)
      service
  }

  def sendClientInfoTelemetry(extraValues: Map[String, String],
                              conn: Connection): Unit = {
    SparkConnectorContext.recordConfig()
    val metric = Utils.getClientInfoJson()
    for ((key, value) <- extraValues) {
      metric.put(key, value)
    }
    addLog(
      (TelemetryTypes.SPARK_CLIENT_INFO, metric),
      System.currentTimeMillis()
    )
    send(conn.getTelemetry)
  }

  def addLog(log: ((TelemetryTypes, ObjectNode), Long)): Unit = {
    logger.debug(s"""
        |Telemetry Output
        |Type: ${log._1._1}
        |Data: ${log._1._2.toString}
      """.stripMargin)

    this.synchronized {
      output = mapper.createObjectNode()
      output.put(TELEMETRY_TYPE_FIELD_NAME, log._1._1.toString)
      output.put(TELEMETRY_SOURCE_FIELD_NAME, TELEMETRY_SOURCE)
      output.set(TELEMETRY_DATA_FIELD_NAME, log._1._2)
      logs = (output, log._2) :: logs
    }
  }

  // Send an OOB telemetry message without connection.
  // This should be used for Spark Executor.
  def sendTelemetryOOB(sfurl: String,
                       senderClass: String,
                       operation: String,
                       retryCount: Int,
                       maxRetryCount: Int,
                       success: Boolean,
                       useProxy: Boolean,
                       queryID: Option[String],
                       throwable: Option[Throwable]): Unit =
  {
    val metric: ObjectNode = mapper.createObjectNode()
    metric.put(TelemetryOOBFields.SPARK_CONNECTOR_VERSION, Utils.VERSION)
    metric.put(TelemetryOOBFields.SFURL, sfurl)
    metric.put(TelemetryOOBFields.SENDER_CLASS, senderClass)
    metric.put(TelemetryOOBFields.OPERATION, operation)
    metric.put(TelemetryOOBFields.RETRY_COUNT, retryCount)
    metric.put(TelemetryOOBFields.MAX_RETRY_COUNT, maxRetryCount)
    metric.put(TelemetryOOBFields.SUCCESS, success)
    metric.put(TelemetryOOBFields.USE_PROXY, useProxy)
    metric.put(TelemetryOOBFields.QUERY_ID, queryID.getOrElse("NA"))
    SnowflakeTelemetry.addCommonFields(metric)
    if (throwable.isDefined) {
      addThrowable(metric, throwable.get)
    } else {
      metric.put(TelemetryOOBFields.EXCEPTION_CLASS_NAME, "NA")
      metric.put(TelemetryOOBFields.EXCEPTION_MESSAGE, "NA")
      metric.put(TelemetryOOBFields.STACKTRACE, "NA")
    }

    // The constructor of TelemetryEvent.LogBuilder uses
    // TelemetryService.getInstance to retrieve the thread safe instance
    // and use it. So getOobTelemetryService() needs to be called before
    // creating the LogBuilder object to make sure TelemetryService has
    // been setup.
    val oobTelemetryService = getOobTelemetryService()
    val logBuilder: TelemetryEvent.LogBuilder = new TelemetryEvent.LogBuilder
    val log: TelemetryEvent = logBuilder
      .withName(s"${TELEMETRY_OOB_NAME_PREFIX}_${operation}_$senderClass")
      .withValue(metric.toString)
      // Below are standard OOB tags
      .withTag(TelemetryOOBTags.CONNECTION_STRING, s"https://$sfurl:443")
      .withTag(TelemetryOOBTags.CTX_ACCOUNT,
        sfurl.substring(0, sfurl.indexOf(".")))
      .withTag(TelemetryOOBTags.CTX_HOST,
        sfurl.substring(sfurl.indexOf(".") + 1))
      .withTag(TelemetryOOBTags.CTX_PORT, "443")
      .withTag(TelemetryOOBTags.CTX_PROTOCAL, "https")
      .withTag(TelemetryOOBTags.CTX_USER, "fake_spark_user")
      // Below are spark connector specific tags
      .withTag(TelemetryOOBTags.SPARK_CONNECTOR_VERSION, Utils.VERSION)
      .withTag(TelemetryOOBTags.SENDER_CLASS_NAME, senderClass)
      .withTag(TelemetryOOBTags.OPERATION, operation)
      .build

    logger.info(s"Send OOB Telemetry message: $senderClass $operation")

    // Send OOB telemetry message.
    oobTelemetryService.report(log)
  }

  def send(telemetry: Telemetry): Unit = {
    var curLogs: List[(ObjectNode, Long)] = Nil
    this.synchronized {
      curLogs = logs
      logs = Nil
    }
    telemetryMessageSender.send(telemetry, curLogs)
  }

  /**
    * Put the pushdown failure telemetry message to internal cache.
    * The message will be sent later in batch.
    *
    * @param plan The logical plan to include the unsupported operations
    * @param exception The pushdown unsupported exception
    */
  def addPushdownFailMessage(plan: LogicalPlan,
                             exception: SnowflakePushdownUnsupportedException)
  : Unit = {
    logger.info(
      s"""Pushdown fails because of operation: ${exception.unsupportedOperation}
         | message: ${exception.getMessage}
         | isKnown: ${exception.isKnownUnsupportedOperation}
           """.stripMargin)

    // Don't send telemetry message for known unsupported operations.
    if (exception.isKnownUnsupportedOperation) {
      return
    }

    val metric: ObjectNode = mapper.createObjectNode()
    metric.put(TelemetryPushdownFailFields.SPARK_CONNECTOR_VERSION, Utils.VERSION)
    metric.put(TelemetryPushdownFailFields.UNSUPPORTED_OPERATION, exception.unsupportedOperation)
    metric.put(TelemetryPushdownFailFields.EXCEPTION_MESSAGE, exception.getMessage)
    metric.put(TelemetryPushdownFailFields.EXCEPTION_DETAILS, exception.details)
    SnowflakeTelemetry.addCommonFields(metric)

    SnowflakeTelemetry.addLog(
      (TelemetryTypes.SPARK_PUSHDOWN_FAIL, metric),
      System.currentTimeMillis()
    )
  }

  /**
    * The resident telemetry message needs to have below 3 fields:
    *    TelemetryFieldNames.EXCEPTION_CLASS_NAME
    *    TelemetryFieldNames.EXCEPTION_MESSAGE
    *    TelemetryFieldNames.STACKTRACE
    *
    * @param metric The telemetry messsage to include the throwable instance
    * @param th The throwable instance
    */
  private[snowflake] def addThrowable(metric: ObjectNode, th: Throwable): Unit = {
    metric.put(TelemetryFieldNames.EXCEPTION_CLASS_NAME, th.getClass.toString)

    // If the throwable is SnowflakeSQLException, send ErrorCode/SQLState/QueryId
    if (th.isInstanceOf[SnowflakeSQLException]) {
      val e = th.asInstanceOf[SnowflakeSQLException]
      val proposedMessage = s"SnowflakeSQLException: ErrorCode=" +
        s"${e.getErrorCode} SQLState=${e.getSQLState} QueryId=${e.getQueryId}"
      metric.put(TelemetryFieldNames.EXCEPTION_MESSAGE, proposedMessage)
      val stringWriter = new StringWriter
      e.printStackTrace(new PrintWriter(stringWriter))
      val stacktraceString = stringWriter.toString.replaceAll(
        Pattern.quote(e.getMessage), proposedMessage)
      metric.put(TelemetryFieldNames.STACKTRACE, stacktraceString)
    } else {
      metric.put(TelemetryFieldNames.EXCEPTION_MESSAGE, th.getMessage)
      val stringWriter = new StringWriter
      th.printStackTrace(new PrintWriter(stringWriter))
      metric.put(TelemetryFieldNames.STACKTRACE, stringWriter.toString)
    }
  }

  private[snowflake] def sendQueryStatus(conn: Connection,
                                         operation: String,
                                         queryId: String,
                                         queryStatus: String,
                                         elapse: Long,
                                         throwable: Option[Throwable],
                                         details: String
                                        ): Unit = {
    try {
      val metric: ObjectNode = mapper.createObjectNode()
      metric.put(TelemetryQueryStatusFields.SPARK_CONNECTOR_VERSION, Utils.VERSION)
      metric.put(TelemetryQueryStatusFields.OPERATION, operation)
      metric.put(TelemetryQueryStatusFields.QUERY_ID, queryId)
      metric.put(TelemetryQueryStatusFields.QUERY_STATUS, queryStatus)
      metric.put(TelemetryQueryStatusFields.ELAPSED_TIME, elapse)
      if (throwable.isDefined) {
        addThrowable(metric, throwable.get)
      }
      metric.put(TelemetryQueryStatusFields.DETAILS, details)
      SnowflakeTelemetry.addCommonFields(metric)

      SnowflakeTelemetry.addLog(
        (TelemetryTypes.SPARK_QUERY_STATUS, metric),
        System.currentTimeMillis()
      )
      SnowflakeTelemetry.send(conn.getTelemetry)
    } catch {
      case th: Throwable =>
        logger.warn(s"Fail to send spark_query_status. reason: ${th.getMessage}")
    }
  }

  private[snowflake] def sendIngressMessage(conn: Connection,
                                            queryId: String,
                                            rowCount: Long,
                                            bytes: Long): Unit = {
    try {
      val metric: ObjectNode = mapper.createObjectNode()
      metric.put(TelemetryFieldNames.QUERY_ID, queryId)
      metric.put(TelemetryFieldNames.INPUT_BYTES, bytes)
      metric.put(TelemetryFieldNames.ROW_COUNT, rowCount)
      SnowflakeTelemetry.addCommonFields(metric)

      SnowflakeTelemetry.addLog(
        (TelemetryTypes.SPARK_INGRESS, metric),
        System.currentTimeMillis()
      )
      SnowflakeTelemetry.send(conn.getTelemetry)
    } catch {
      case th: Throwable =>
        logger.warn(s"Fail to send spark_ingress. reason: ${th.getMessage}")
    }
  }

  // Configuration retrieving is optional for for diagnostic purpose,
  // so it never raises exception.
  private[snowflake] def getClientConfig(): ObjectNode = {
    val metric: ObjectNode = mapper.createObjectNode()

    try {
      // Add versions info
      Utils.addVersionInfo(metric)

      // Add JVM and system basic configuration
      metric.put(TelemetryClientInfoFields.OS_NAME,
        System.getProperty(TelemetryConstValues.JVM_PROPERTY_NAME_OS_NAME))
      val rt = Runtime.getRuntime
      metric.put(TelemetryClientInfoFields.MAX_MEMORY_IN_MB, rt.maxMemory() / MB)
      metric.put(TelemetryClientInfoFields.TOTAL_MEMORY_IN_MB, rt.totalMemory() / MB)
      metric.put(TelemetryClientInfoFields.FREE_MEMORY_IN_MB, rt.freeMemory() / MB)
      metric.put(TelemetryClientInfoFields.CPU_CORES, rt.availableProcessors())

      // Add Spark configuration
      val sparkConf = SparkEnv.get.conf
      SnowflakeTelemetry.addCommonFields(metric)
      metric.put(TelemetryFieldNames.SPARK_LANGUAGE, detectSparkLanguage(sparkConf))
      metric.put(TelemetryClientInfoFields.IS_PYSPARK,
        sparkConf.contains("spark.pyspark.python"))
      val sparkMetric: ObjectNode = mapper.createObjectNode()
      sparkConf.getAll.foreach {
        case (key, value) =>
          if (sparkOptions.contains(key)) {
            sparkMetric.put(key, value)
          } else {
            sparkMetric.put(key, "N/A")
          }
      }
      if (!sparkMetric.isEmpty) {
        metric.set(TelemetryClientInfoFields.SPARK_CONFIG, sparkMetric)
      }
      val sparkLibrariesArray = metric.putArray(TelemetryFieldNames.LIBRARIES)
      SnowflakeTelemetry.getSparkLibraries.foreach(sparkLibrariesArray.add)

      // Add task info if available
      addTaskInfo(metric)
    } catch {
      case _: Throwable => {
        metric
      }
    }
  }

  // The spark options may help diagnosis spark connector issue.
  private val sparkOptions = Set(
    "spark.app.name",
    "spark.app.id",
    "spark.submit.deployMode",
    "spark.jars",
    "spark.master",
    "spark.repl.local.jars",
    // Driver related
    "spark.driver.host",
    "spark.driver.extraJavaOptions",
    "spark.driver.cores",
    // Executor related
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.executor.extraJavaOptions",
    "spark.executor.id",
    // Memory related
    "spark.driver.memory",
    "spark.driver.memoryOverhead",
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.executor.pyspark.memory",
    "spark.python.worker.memory",
    "spark.memory.fraction",
    "spark.memory.storageFraction",
    "spark.memory.offHeap.enabled",
    "spark.memory.offHeap.size",
    // Execution behavior
    "spark.default.parallelism",
    "spark.dynamicAllocation.enabled",
    "spark.dynamicAllocation.initialExecutors",
    "spark.dynamicAllocation.maxExecutors",
    "spark.dynamicAllocation.minExecutors",
    // Misc
    "spark.sql.ansi.enabled",
    "spark.pyspark.driver.python",
    "spark.pyspark.python",
    "spark.sql.session.timeZone"
  )

  // Configuration retrieving is optional for for diagnostic purpose,
  // so it never raises exception.
  private[snowflake] def getTaskInfo(): ObjectNode = {
    val metric: ObjectNode = mapper.createObjectNode()
    try {
      addTaskInfo(metric)
    } catch {
      case _: Throwable => {
        metric
      }
    }
  }

  private def addTaskInfo(metric: ObjectNode): ObjectNode = {
    val task = TaskContext.get()
    if (task != null) {
      metric.put(TelemetryTaskInfoFields.TASK_PARTITION_ID, task.partitionId())
      metric.put(TelemetryTaskInfoFields.TASK_ATTEMPT_ID, task.taskAttemptId())
      metric.put(TelemetryTaskInfoFields.TASK_ATTEMPT_NUMBER, task.attemptNumber())
      metric.put(TelemetryTaskInfoFields.TASK_STAGE_ATTEMPT_NUMBER, task.stageAttemptNumber())
      metric.put(TelemetryTaskInfoFields.TASK_STAGE_ID, task.stageId())
      metric.put(TelemetryTaskInfoFields.THREAD_ID, Thread.currentThread().getId)
    }
    metric
  }

  private[snowflake] def detectSparkLanguage(sparkConf: SparkConf): String = {
    if (sparkConf.contains("spark.r.command")
      || sparkConf.contains("spark.r.driver.command")
      || sparkConf.contains("spark.r.shell.command")) {
      "R"
    } else if (sparkConf.contains("spark.pyspark.python")) {
      "Python"
    } else {
      "Scala"
    }
  }

  private[snowflake] def addCommonFields(metric: ObjectNode): ObjectNode = {
    metric.put(TelemetryFieldNames.SPARK_APPLICATION_ID, getSparkApplicationId)
  }
}

object TelemetryTypes extends Enumeration {
  type TelemetryTypes = Value
  val SPARK_PLAN: Value = Value("spark_plan")
  val SPARK_STREAMING: Value = Value("spark_streaming")
  val SPARK_STREAMING_START: Value = Value("spark_streaming_start")
  val SPARK_STREAMING_END: Value = Value("spark_streaming_end")
  val SPARK_EGRESS: Value = Value("spark_egress")
  val SPARK_INGRESS: Value = Value("spark_ingress")
  val SPARK_CLIENT_INFO: Value = Value("spark_client_info")
  val SPARK_QUERY_STATUS: Value = Value("spark_query_status")
  val SPARK_PUSHDOWN_FAIL: Value = Value("spark_pushdown_fail")
  val SPARK_PLAN_STATISTIC: Value = Value("spark_plan_statistic")
}

// All telemetry message field names have to be defined here
// Important:
//   1. These fields name have been used in released spark connector.
//      change this may cause telemetry message query to be difficult.
//   2. Make sure to review whether existing field names to be qualified
//      before adding new name.
private[snowflake] object TelemetryFieldNames {
  val SPARK_CONNECTOR_VERSION = "spark_connector_version"
  val SPARK_VERSION = "spark_version"
  val APPLICATION_NAME = "application_name"
  val SCALA_VERSION = "scala_version"
  val JAVA_VERSION = "java_version"
  val JDBC_VERSION = "jdbc_version"
  val CERTIFIED_JDBC_VERSION = "certified_jdbc_version"
  val SFURL = "sfurl"
  val OPERATION = "operation"
  val QUERY_ID = "query_id"
  val QUERY_STATUS = "query_status"
  val ELAPSED_TIME = "elapsed_time"
  val EXCEPTION_MESSAGE = "message"
  val EXCEPTION_CLASS_NAME = "exception"
  val STACKTRACE = "stacktrace"
  val DETAILS = "details"
  val SENDER = "sender"
  val RETRY = "retry"
  val MAX_RETRY = "max_retry"
  val SUCCESS = "success"
  val USE_PROXY = "use_proxy"
  val START_TIME = "start_time"
  val END_TIME = "end_time"
  val LOAD_RATE = "load_rate"
  val DATA_BATCH = "data_batch"
  val OUTPUT_BYTES = "output_bytes"
  val ROW_COUNT = "row_count"
  val INPUT_BYTES = "input_bytes"
  val OS_NAME = "os_name"
  val MAX_MEMORY_IN_MB = "max_memory_in_mb"
  val TOTAL_MEMORY_IN_MB = "total_memory_in_mb"
  val FREE_MEMORY_IN_MB = "free_memory_in_mb"
  val CPU_CORES = "cpu_cores"
  val TASK_PARTITION_ID = "task_partition_id"
  val TASK_STAGE_ID = "task_stage_id"
  val TASK_ATTEMPT_ID = "task_attempt_id"
  val TASK_ATTEMPT_NUMBER = "task_attempt_number"
  val TASK_STAGE_ATTEMPT_NUMBER = "task_stage_attempt_number"
  val THREAD_ID = "thread_id"
  val SPARK_CONFIG = "spark_config"
  val SPARK_APPLICATION_ID = "spark_application_id"
  val IS_PYSPARK = "is_pyspark"
  val SPARK_LANGUAGE = "spark_language"
  val LIBRARIES = "libraries"
  val STATISTIC_INFO = "statistic_info"
}

private[snowflake] object TelemetryConstValues {
  val OPERATION_READ = "read"
  val OPERATION_WRITE = "write"
  val STATUS_SUCCESS = "success"
  val STATUS_FAIL = "fail"
  val JVM_PROPERTY_NAME_OS_NAME = "os.name"
}

object TelemetryQueryStatusFields {
  // Spark connector version
  val SPARK_CONNECTOR_VERSION = TelemetryFieldNames.SPARK_CONNECTOR_VERSION
  // The query operation: read vs write
  val OPERATION = TelemetryFieldNames.OPERATION
  // query ID if available
  val QUERY_ID = TelemetryFieldNames.QUERY_ID
  // Query status: fail vs success
  val QUERY_STATUS = TelemetryFieldNames.QUERY_STATUS
  // query execution time
  val ELAPSED_TIME = TelemetryFieldNames.ELAPSED_TIME
  // The error message for the exception
  val EXCEPTION_MESSAGE = TelemetryFieldNames.EXCEPTION_MESSAGE
  // Exception class name
  val EXCEPTION_CLASS_NAME = TelemetryFieldNames.EXCEPTION_CLASS_NAME
  // Exception stacktrace
  val STACKTRACE = TelemetryFieldNames.STACKTRACE
  // The details information about the exception
  val DETAILS = TelemetryFieldNames.DETAILS
}

object TelemetryClientInfoFields {
  // Spark connector version
  val SPARK_CONNECTOR_VERSION = TelemetryFieldNames.SPARK_CONNECTOR_VERSION
  // Spark Version
  val SPARK_VERSION = TelemetryFieldNames.SPARK_VERSION
  // Application name
  val APPLICATION_NAME = TelemetryFieldNames.APPLICATION_NAME
  // Scala version
  val SCALA_VERSION = TelemetryFieldNames.SCALA_VERSION
  // Java version
  val JAVA_VERSION = TelemetryFieldNames.JAVA_VERSION
  // Runtime JDBC version
  val JDBC_VERSION = TelemetryFieldNames.JDBC_VERSION
  // Certified JDBC version
  val CERTIFIED_JDBC_VERSION = TelemetryFieldNames.CERTIFIED_JDBC_VERSION
  // Snowflake URL with account name
  val SFURL = TelemetryFieldNames.SFURL

  // System basic information
  val OS_NAME = TelemetryFieldNames.OS_NAME
  val MAX_MEMORY_IN_MB = TelemetryFieldNames.MAX_MEMORY_IN_MB
  val TOTAL_MEMORY_IN_MB = TelemetryFieldNames.TOTAL_MEMORY_IN_MB
  val FREE_MEMORY_IN_MB = TelemetryFieldNames.FREE_MEMORY_IN_MB
  val CPU_CORES = TelemetryFieldNames.CPU_CORES

  // Spark configuration
  val SPARK_CONFIG = TelemetryFieldNames.SPARK_CONFIG
  val SPARK_APPLICATION_ID = TelemetryFieldNames.SPARK_APPLICATION_ID
  val IS_PYSPARK = TelemetryFieldNames.IS_PYSPARK
}

object TelemetryTaskInfoFields {
  // task information which may be are available on executor.
  val TASK_PARTITION_ID = TelemetryFieldNames.TASK_PARTITION_ID
  val TASK_STAGE_ID = TelemetryFieldNames.TASK_STAGE_ID
  val TASK_ATTEMPT_ID = TelemetryFieldNames.TASK_ATTEMPT_ID
  val TASK_ATTEMPT_NUMBER = TelemetryFieldNames.TASK_ATTEMPT_NUMBER
  val TASK_STAGE_ATTEMPT_NUMBER = TelemetryFieldNames.TASK_STAGE_ATTEMPT_NUMBER
  val THREAD_ID = TelemetryFieldNames.THREAD_ID
}

object TelemetryPushdownFailFields {
  // Spark connector version
  val SPARK_CONNECTOR_VERSION = TelemetryFieldNames.SPARK_CONNECTOR_VERSION
  // The unsupported operation for pushdown
  val UNSUPPORTED_OPERATION = TelemetryFieldNames.OPERATION
  // The error message for the exception
  val EXCEPTION_MESSAGE = TelemetryFieldNames.EXCEPTION_MESSAGE
  // The details information about the exception
  val EXCEPTION_DETAILS = TelemetryFieldNames.DETAILS
}

object TelemetryOOBFields {
  // Spark connector version
  val SPARK_CONNECTOR_VERSION = TelemetryFieldNames.SPARK_CONNECTOR_VERSION
  // The URL to include snowflake account name
  val SFURL = TelemetryFieldNames.SFURL
  // The class to send the message
  val SENDER_CLASS = TelemetryFieldNames.SENDER
  // The operation such as read, write
  val OPERATION = TelemetryFieldNames.OPERATION
  val RETRY_COUNT = TelemetryFieldNames.RETRY
  val MAX_RETRY_COUNT = TelemetryFieldNames.MAX_RETRY
  val SUCCESS = TelemetryFieldNames.SUCCESS
  val USE_PROXY = TelemetryFieldNames.USE_PROXY
  val QUERY_ID = TelemetryFieldNames.QUERY_ID
  // Below 3 fields are the Exception details.
  val EXCEPTION_CLASS_NAME = TelemetryFieldNames.EXCEPTION_CLASS_NAME
  val EXCEPTION_MESSAGE = TelemetryFieldNames.EXCEPTION_MESSAGE
  val STACKTRACE = TelemetryFieldNames.STACKTRACE
}

// Out-of-band telemetry tags may use different values to TelemetryFieldNames
// because we can't define the tags name freely.
// For example, "connectionString" has to be set as the valid SFURL otherwise
// the message will be ignored.
object TelemetryOOBTags {
  // Spark connector version
  val SPARK_CONNECTOR_VERSION = TelemetryFieldNames.SPARK_CONNECTOR_VERSION
  // The class to send the message
  val SENDER_CLASS_NAME: String = "spark_connector_sender"
  // The operation such as read, write
  val OPERATION: String = "spark_connector_operation"
  // A valid Connection String is necessary.
  val CONNECTION_STRING = "connectionString"
  // Below are optional tags for OOB messages
  val CTX_ACCOUNT = "ctx_account"
  val CTX_HOST = "ctx_host"
  val CTX_PORT = "ctx_port"
  val CTX_PROTOCAL = "ctx_protocol"
  val CTX_USER = "ctx_user"
}

private[snowflake] trait TelemetryMessageSender {
  def send(telemetry: Telemetry, logs: List[(ObjectNode, Long)]): Unit
}

private final class SnowflakeTelemetryMessageSender extends TelemetryMessageSender {
  private val logger = LoggerFactory.getLogger(getClass)

  override def send(telemetry: Telemetry, logs: List[(ObjectNode, Long)]): Unit = {
    logs.foreach {
      case (log, timestamp) =>
        logger.debug(
          s"""
             |Send Telemetry
             |timestamp:$timestamp
             |log:${log.toString}"
           """.stripMargin)
        telemetry.asInstanceOf[TelemetryClient].addLogToBatch(log, timestamp)
    }
    telemetry.sendBatchAsync()
  }
}
