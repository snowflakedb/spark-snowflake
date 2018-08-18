package net.snowflake.spark.snowflake

import java.nio.charset.Charset
import java.util.function.Consumer

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import net.snowflake.spark.snowflake.SnowflakeLogType.SnowflakeLogType
import net.snowflake.spark.snowflake.io.{CloudStorage, CloudStorageOperations}

import scala.collection.mutable.ArrayBuffer


private[snowflake] object SnowflakeLogManager {

  val LOG_TYPE = "logType"
  val BATCH_ID = "batchId"
  val FILE_NAMES = "fileNames"
  val FAILED_FILE_NAMES = "failedFileNames"
  val LOADED_FILE_NAMES = "loadedFileNames"
  val LOG_DIR = "log"
  val PIPE_NAME = "pipeName"

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

  //  case class StreamingFailedFileReport(logs: List[StreamingBatchLog]) extends SnowflakeLog {
  //    override def generateJson: String = ???
  //  }
  /**
    * @param fileName file name
    * @return logDir / file name
    */
  def getFullPath(fileName: String): String = s"${LOG_DIR}/$fileName"

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
  * "pipeName": "snowpipe_name"
  * "fileNames": ["fileName1","fileName2"],
  * "LoadedFileNames": ["fileName1", "fileName2"]
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


  def getFileList: List[String] = getListFromJson(SnowflakeLogManager.FILE_NAMES)

  def getLoadedFileList: List[String] = getListFromJson(SnowflakeLogManager.LOADED_FILE_NAMES)

  def getPipeName: String = node.get(SnowflakeLogManager.PIPE_NAME).asText()

  def setLoadedFileNames(fileNames: List[String]): StreamingBatchLog = {
    val arr = node.putArray(SnowflakeLogManager.LOADED_FILE_NAMES)
    fileNames.foreach(arr.add)
    this
  }

  def save(implicit storage: CloudStorage): Unit = {
    val outputStream =
      storage.upload(
        StreamingBatchLog.fileName(batchId),
        Some(SnowflakeLogManager.LOG_DIR))(false)
    val text = this.toString
    println(s"log: $text")
    outputStream.write(text.getBytes("UTF-8"))
    outputStream.close()
  }


}

object StreamingBatchLog {

  //threshold for grouping log
  private val GROUP_SIZE: Int = 100

  implicit val mapper: ObjectMapper = SnowflakeLogManager.mapper
  implicit val isCompressed: Boolean = false

  def apply(batchId: Long, fileNames: List[String], pipeName: String): StreamingBatchLog = {
    val node = mapper.createObjectNode()
    node.put(SnowflakeLogManager.BATCH_ID, batchId)
    node.put(SnowflakeLogManager.PIPE_NAME, pipeName)
    node.put(SnowflakeLogManager.LOG_TYPE, SnowflakeLogType.STREAMING_BATCH_LOG.toString)
    val arr = node.putArray(SnowflakeLogManager.FILE_NAMES)
    fileNames.foreach(arr.add)
    StreamingBatchLog(node)(mapper)
  }

  def fileName(batchId: Long): String = s"${batchId / GROUP_SIZE}/${batchId % GROUP_SIZE}.json"

  def logExists(batchId: Long)(implicit storage: CloudStorage): Boolean =
    storage.fileExists(SnowflakeLogManager.getFullPath(fileName(batchId)))

  def loadLog(batchId: Long)(implicit storage: CloudStorage): StreamingBatchLog = {
    val inputStream = storage.download(SnowflakeLogManager.getFullPath(fileName(batchId)))
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

  def mergeBatchLog(groupId: Long): Unit = {
    var logs: List[StreamingBatchLog] =
      (0 until GROUP_SIZE)
        .map(x => loadLog(groupId * GROUP_SIZE + x)).toList
  }
}

private[snowflake] object SnowflakeLogType extends Enumeration {
  type SnowflakeLogType = Value
  val STREAMING_BATCH_LOG, STREAMING_FAILED_FILE_REPORT = Value
}



