package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import net.snowflake.spark.snowflake.SnowflakeLogType.SnowflakeLogType
import net.snowflake.spark.snowflake.io.{CloudStorage, CloudStorageOperations}

private[snowflake] object SnowflakeLogManager {

  val LOG_TYPE = "logType"
  val BATCH_ID = "batchId"
  val FILE_NAMES = "fileNames"
  val FAILED_FILE_NAMES = "failedFileNames"
  val LOG_DIR = "log"

  implicit val mapper: ObjectMapper = new ObjectMapper()

  def getLogObject(json: String): SnowflakeLog = getLogObject(mapper.readTree(json).asInstanceOf[ObjectNode])

  def getLogObject(node: ObjectNode): SnowflakeLog =
    if(node.has(LOG_TYPE)) {
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
  * "failedFileNames": ["fileName1", "fileName2"]
  * }
  *
  * @param node a JSON object node contains log data
  */
case class StreamingBatchLog(override val node: ObjectNode)
                            (override implicit val mapper: ObjectMapper) extends SnowflakeLog {

  override implicit val logType: SnowflakeLogType = SnowflakeLogType.STREAMING_BATCH_LOG

  private lazy val batchId: Long = node.get(SnowflakeLogManager.BATCH_ID).asLong()

  def setFailedFiles(fileNames: List[String]): StreamingBatchLog = {
    val arr = node.putArray(SnowflakeLogManager.FAILED_FILE_NAMES)
    fileNames.foreach(arr.add)
    this
  }

  def addFailedFiles(fileNames: List[String]): StreamingBatchLog =
    if (node.has(SnowflakeLogManager.FAILED_FILE_NAMES)) {
      val arr = node.get(SnowflakeLogManager.FAILED_FILE_NAMES).asInstanceOf[ArrayNode]
      fileNames.foreach(arr.add)
      this
    }
    else setFailedFiles(fileNames)

  def save(implicit storage: CloudStorage): Unit = {
    val outputStream =
      storage.upload(StreamingBatchLog.fileName(batchId), Some(SnowflakeLogManager.LOG_DIR))
    outputStream.write(toString.getBytes)
    outputStream.close()
  }


}

object StreamingBatchLog {

  //threshold for grouping log
  private val GROUP_SIZE: Int = 100

  implicit val mapper: ObjectMapper = SnowflakeLogManager.mapper

  def apply(batchId: Long, fileNames: List[String]): StreamingBatchLog = {
    val node = mapper.createObjectNode()
    node.put(SnowflakeLogManager.BATCH_ID, batchId)
    node.put(SnowflakeLogManager.LOG_TYPE, SnowflakeLogType.STREAMING_BATCH_LOG.toString)
    val arr = node.putArray(SnowflakeLogManager.FILE_NAMES)
    fileNames.foreach(arr.add)
    StreamingBatchLog(node)(mapper)
  }

  def fileName(batchId: Long): String = s"${batchId/GROUP_SIZE}/${batchId%GROUP_SIZE}.json"
}

private[snowflake] object SnowflakeLogType extends Enumeration {
  type SnowflakeLogType = Value
  val STREAMING_BATCH_LOG, STREAMING_FAILED_FILE_REPORT = Value
}



