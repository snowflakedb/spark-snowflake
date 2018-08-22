package net.snowflake.spark.snowflake

import java.nio.charset.Charset
import java.sql.Connection

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.SnowflakeLogType.SnowflakeLogType
import net.snowflake.spark.snowflake.io.{CloudStorage, CloudStorageOperations}

import scala.collection.mutable.ArrayBuffer


private[snowflake] object SnowflakeLogManager {

  val LOG_TYPE = "logType"
  val BATCH_ID = "batchId"
  val GROUP_ID = "groupId"
  val FILE_NAMES = "fileNames"
  val FAILED_FILE_NAMES = "failedFileNames"
  val LOADED_FILE_NAMES = "loadedFileNames"
  val PIPE_NAME = "pipeName"
  val STAGE_NAME = "stageName"
  val FILE_FAILED = "fileFailed"
  val LOG_STAGE = "snowflake_spark_log"

  implicit val mapper: ObjectMapper = new ObjectMapper()

  def getLogObject(json: String): SnowflakeLog = getLogObject(mapper.readTree(json).asInstanceOf[ObjectNode])

  def getLogObject(node: ObjectNode): SnowflakeLog =
    if (node.has(LOG_TYPE)) {
      SnowflakeLogType.withName(node.get(LOG_TYPE).asText()) match {
        case SnowflakeLogType.STREAMING_BATCH_LOG =>
          StreamingBatchLog(node)(mapper)
        case _ => throw new UnsupportedOperationException("Not Support")
      }
    }
    else throw new IllegalArgumentException("Invalid log data")

}

private[snowflake] class LogStorageClient(
                                           param: MergedParameters,
                                           conn: Connection,
                                           prefix: String
                                         ) {

  private var storageClient: CloudStorage = _
  private var lastUpdateTime: Long = 0
  private val REFRESH_TIME: Long = 2 * 3600 * 1000 //two hours


  def getLogStorageClient: CloudStorage = {
    val time = System.currentTimeMillis()
    if (time - lastUpdateTime > REFRESH_TIME) {
      val sql =
        s"""
           |create stage if not exists ${SnowflakeLogManager.LOG_STAGE}
           """.stripMargin
      DefaultJDBCWrapper.executeQueryInterruptibly(conn, sql)
      storageClient =
        CloudStorageOperations
          .createStorageClientFromStage(
            param,
            conn,
            SnowflakeLogManager.LOG_STAGE,
            Some(prefix)
          )
      lastUpdateTime = time
    }
    storageClient
  }
}

sealed trait SnowflakeLog {
  implicit val mapper: ObjectMapper
  protected implicit val node: ObjectNode
  protected implicit val logType: SnowflakeLogType

  override def toString: String = node.toString
}

/**
  * The log object for each data batch, contains staging files names and failed file names
  *
  * JSON Format:
  * {
  * "logType": "STREAMING_BATCH_LOG",
  * "batchId": 123,
  * "fileNames": ["fileName1","fileName2"],
  * "LoadedFileNames": ["fileName1", "fileName2"],
  * "fileFailed": false
  * }
  *
  * @param node a JSON object node contains log data
  */
case class StreamingBatchLog(override val node: ObjectNode)
                            (override implicit val mapper: ObjectMapper) extends SnowflakeLog {

  override implicit val logType: SnowflakeLogType = SnowflakeLogType.STREAMING_BATCH_LOG

  private lazy val batchId: Long = node.get(SnowflakeLogManager.BATCH_ID).asLong()

  private def getListFromJson(name: String): List[String] = {
    if (node.has(name)) {
      var result: List[String] = Nil
      val arr = node.get(name).asInstanceOf[ArrayNode]
      (0 until arr.size()).foreach(x => result = arr.get(x).asText :: result)
      result
    } else Nil
  }

  def hasFileFailed: Boolean =
    node.get(SnowflakeLogManager.FILE_FAILED).asBoolean

  def fileFailed: Unit = node.put(SnowflakeLogManager.FILE_FAILED, true)

  def getFileList: List[String] = getListFromJson(SnowflakeLogManager.FILE_NAMES)

  def getLoadedFileList: List[String] = getListFromJson(SnowflakeLogManager.LOADED_FILE_NAMES)

  def setLoadedFileNames(fileNames: List[String]): StreamingBatchLog = {
    val arr = node.putArray(SnowflakeLogManager.LOADED_FILE_NAMES)
    fileNames.foreach(arr.add)
    this
  }

  def save(implicit storage: LogStorageClient): Unit = {
    val outputStream =
      storage.getLogStorageClient.upload(
        StreamingBatchLog.fileName(batchId),
        None,
        false
      )
    val text = this.toString
    println(s"log: $text")
    outputStream.write(text.getBytes("UTF-8"))
    outputStream.close()
  }


}

object StreamingBatchLog {

  //threshold for grouping log
  val GROUP_SIZE: Int = 10

  implicit val mapper: ObjectMapper = SnowflakeLogManager.mapper

  def apply(batchId: Long,
            fileNames: List[String],
            fileFailed: Boolean = false
           ): StreamingBatchLog = {
    val node = mapper.createObjectNode()
    node.put(SnowflakeLogManager.BATCH_ID, batchId)
    node.put(SnowflakeLogManager.LOG_TYPE, SnowflakeLogType.STREAMING_BATCH_LOG.toString)
    node.put(SnowflakeLogManager.FILE_FAILED, fileFailed)
    val arr = node.putArray(SnowflakeLogManager.FILE_NAMES)
    fileNames.foreach(arr.add)
    StreamingBatchLog(node)(mapper)
  }

  def fileName(batchId: Long): String = s"tmp/${batchId / GROUP_SIZE}/${batchId % GROUP_SIZE}.json"

  def logExists(batchId: Long)
               (implicit storage: LogStorageClient): Boolean =
    storage.getLogStorageClient
      .fileExists(fileName(batchId))

  def loadLog(batchId: Long)
             (implicit storage: LogStorageClient): StreamingBatchLog = {
    val inputStream =
      storage.getLogStorageClient
        .download(fileName(batchId), false)
    val buffer = ArrayBuffer.empty[Byte]

    var c: Int = inputStream.read()
    while (c != -1) {
      buffer.append(c.toByte)
      c = inputStream.read()
    }
    try {
      StreamingBatchLog(
        mapper.readTree(
          new String(buffer.toArray, Charset.forName("UTF-8"))
        ).asInstanceOf[ObjectNode]
      )(mapper)
    } catch {
      case _: Exception => throw new IllegalArgumentException(s"log file: ${fileName(batchId)} is broken")
    }
  }

  def deleteBatchLog(batchIds: List[Long])
                    (implicit storage: LogStorageClient): Unit =
    storage.getLogStorageClient.deleteFiles(
      batchIds.map(fileName(_))
    )


  def mergeBatchLog(groupId: Long)
                   (implicit storage: LogStorageClient): Unit = {
    val logs: List[StreamingBatchLog] =
      (0 until GROUP_SIZE)
        .flatMap(x => {
          val batchId = groupId * GROUP_SIZE + x
          if (logExists(batchId))
            Seq(loadLog(batchId))
          else Nil
        }
        ).toList

    StreamingFailedFileReport(
      groupId,
      logs.flatMap(x => {
        val files = x.getFileList
        val loadedFile = x.getLoadedFileList
        files.filterNot(loadedFile.toSet)
      })
    )(mapper).save

    //remove batch log
    deleteBatchLog(
      logs.map(_.batchId)
    )

  }
}

/**
  * Contains all failed file names
  *
  * {
  * "logType":"STREAMING_FAILED_FILE_REPORT",
  * "groupId":1,
  * "failedFileNames":[]
  * }
  *
  */
case class StreamingFailedFileReport(groupId: Long, failedFiles: List[String])
                                    (override implicit val mapper: ObjectMapper) extends SnowflakeLog {
  override implicit val logType: SnowflakeLogType = SnowflakeLogType.STREAMING_FAILED_FILE_REPORT
  override implicit val node: ObjectNode = mapper.createObjectNode()

  node.put(SnowflakeLogManager.LOG_TYPE, logType.toString)
  node.put(SnowflakeLogManager.GROUP_ID, groupId)

  val arr = node.putArray(SnowflakeLogManager.FAILED_FILE_NAMES)
  failedFiles.foreach(arr.add)

  def save(implicit storage: LogStorageClient): Unit = {
    val outputStream =
      storage.getLogStorageClient.upload(
        StreamingFailedFileReport.fileName(groupId),
        None,
        false
      )
    val text = this.toString
    println(s"report: $text")
    outputStream.write(text.getBytes("UTF-8"))
    outputStream.close()
  }

}

object StreamingFailedFileReport {

  def logExists(groupId: Long)
               (implicit storage: LogStorageClient): Boolean =
    storage.getLogStorageClient
      .fileExists(fileName(groupId))

  def fileName(groupId: Long): String =
    s"failed_files/$groupId.json"

}

/**
  *
  * {
  * "pipeName": pipe_name,
  * "stageName": stage_name
  * }
  */
case class StreamingConfiguration(override val node: ObjectNode)
                                 (override implicit val mapper: ObjectMapper) extends SnowflakeLog {
  override implicit val logType: SnowflakeLogType = SnowflakeLogType.STREAMING_CONFIGURATION

  def getPipeName: String =
    node.get(SnowflakeLogManager.PIPE_NAME).asText()

  def getStageName: String =
    node.get(SnowflakeLogManager.STAGE_NAME).asText()

  def save(implicit storage: LogStorageClient): Unit = {
    val outputStream =
      storage.getLogStorageClient
      .upload(
        StreamingConfiguration.fileName,
        None,
        false
      )

    val text = this.toString
    println(s"streaming configuration: $text")
    outputStream.write(text.getBytes("UTF-8"))
    outputStream.close()
  }
}

object StreamingConfiguration{
  implicit val mapper: ObjectMapper = SnowflakeLogManager.mapper
  val fileName = "snowflake_streaming_configuration.json"

  def apply(stageName: String, pipeName: String): StreamingConfiguration = {
    val node = mapper.createObjectNode()
    node.put(SnowflakeLogManager.PIPE_NAME, pipeName)
    node.put(SnowflakeLogManager.STAGE_NAME, stageName)
    StreamingConfiguration(node)(mapper)
  }

  def logExists(implicit storage: LogStorageClient): Boolean =
    storage.getLogStorageClient.fileExists(fileName)

  def loadLog(implicit storage: LogStorageClient): StreamingConfiguration = {
    val inputStream =
      storage.getLogStorageClient
      .download(fileName, false)
    val buffer = ArrayBuffer.empty[Byte]

    var c: Int = inputStream.read()
    while (c != -1) {
      buffer.append(c.toByte)
      c = inputStream.read()
    }

    try {
      StreamingConfiguration(
        mapper.readTree(
          new String(buffer.toArray, Charset.forName("UTF-8"))
        ).asInstanceOf[ObjectNode]
      )(mapper)
    } catch {
      case _: Exception =>
        throw new IllegalArgumentException(s"snowflake streaming configuration log file is broken")
    }
  }

}

private[snowflake] object SnowflakeLogType extends Enumeration {
  type SnowflakeLogType = Value
  val STREAMING_BATCH_LOG,
  STREAMING_FAILED_FILE_REPORT,
  STREAMING_CONFIGURATION = Value
}



