package net.snowflake.spark.snowflake

import java.nio.charset.Charset
import java.sql.Connection

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import net.snowflake.spark.snowflake.SnowflakeLogType.SnowflakeLogType
import net.snowflake.spark.snowflake.io.CloudStorage

import scala.collection.mutable.ArrayBuffer


private[snowflake] class SnowflakeLogManager {
  val LOG_TYPE = "logType"
  val BATCH_ID = "batchId"
  val GROUP_ID = "groupId"
  val FILE_NAMES = "fileNames"
  val FAILED_FILE_NAMES = "failedFileNames"
  val LOADED_FILE_NAMES = "loadedFileNames"
  val TIME_OUT_FILES = "timeOutFiles"
  val TIME_OUT = "timeOut"
  val PIPE_NAME = "pipeName"
  val STAGE_NAME = "stageName"
  val FILE_FAILED = "fileFailed"
  val LOG_DIR = "log"

  implicit val mapper: ObjectMapper = new ObjectMapper()
}

private[snowflake] object SnowflakeLogManager extends SnowflakeLogManager {

  def getLogObject(json: String): SnowflakeLog = getLogObject(mapper.readTree(json).asInstanceOf[ObjectNode])

  def getLogObject(node: ObjectNode): SnowflakeLog =
    if (node.has(LOG_TYPE)) {
      SnowflakeLogType.withName(node.get(LOG_TYPE).asText()) match {
        case SnowflakeLogType.STREAMING_BATCH_LOG =>
          StreamingBatchLog(node)
        case SnowflakeLogType.STREAMING_FAILED_FILE_REPORT =>
          StreamingFailedFileReport(node)
        case _ => throw new UnsupportedOperationException("Not Support")
      }
    }
    else throw new IllegalArgumentException("Invalid log data")

}

sealed trait SnowflakeLog {
  protected implicit lazy val mapper: ObjectMapper = SnowflakeLogManager.mapper
  protected implicit val node: ObjectNode
  protected implicit val logType: SnowflakeLogType

  override def toString: String = node.toString

  def save(implicit storage: CloudStorage, conn: Connection): Unit

  protected def getListFromJson(name: String): List[String] = {
    if (node.has(name)) {
      var result: List[String] = Nil
      val arr = node.get(name).asInstanceOf[ArrayNode]
      (0 until arr.size()).foreach(x => result = arr.get(x).asText :: result)
      result
    } else Nil
  }
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
  * "timeOut": true
  * }
  *
  * @param node a JSON object node contains log data
  */
case class StreamingBatchLog(override val node: ObjectNode) extends SnowflakeLog {

  override implicit val logType: SnowflakeLogType = SnowflakeLogType.STREAMING_BATCH_LOG

  private lazy val batchId: Long = node.get(SnowflakeLogManager.BATCH_ID).asLong()

  def isTimeOut: Boolean = node.get(SnowflakeLogManager.TIME_OUT).asBoolean()

  def timeOut: Unit = node.put(SnowflakeLogManager.TIME_OUT, true)

  def getFileList: List[String] = getListFromJson(SnowflakeLogManager.FILE_NAMES)

  def getLoadedFileList: List[String] = getListFromJson(SnowflakeLogManager.LOADED_FILE_NAMES)

  def setLoadedFileNames(fileNames: List[String]): StreamingBatchLog = {
    val arr = node.putArray(SnowflakeLogManager.LOADED_FILE_NAMES)
    fileNames.foreach(arr.add)
    this
  }

  override def save(implicit storage: CloudStorage, conn: Connection): Unit = {
    val outputStream =
      storage.upload(
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

object StreamingBatchLog extends SnowflakeLogManager {

  //threshold for grouping log
  val GROUP_SIZE: Int = 100

  def apply(batchId: Long,
            fileNames: List[String],
            timeOut: Boolean = false
           ): StreamingBatchLog = {
    val node = mapper.createObjectNode()
    node.put(BATCH_ID, batchId)
    node.put(LOG_TYPE, SnowflakeLogType.STREAMING_BATCH_LOG.toString)
    node.put(TIME_OUT, timeOut)
    val arr = node.putArray(FILE_NAMES)
    fileNames.foreach(arr.add)
    StreamingBatchLog(node)
  }

  def fileName(batchId: Long): String = s"${SnowflakeLogManager.LOG_DIR}/tmp/${batchId / GROUP_SIZE}/${batchId % GROUP_SIZE}.json"

  def logExists(batchId: Long)
               (implicit storage: CloudStorage, conn: Connection): Boolean =
    storage.fileExists(fileName(batchId))

  def loadLog(batchId: Long)
             (implicit storage: CloudStorage, conn: Connection): StreamingBatchLog = {
    val inputStream =
      storage.download(fileName(batchId), false)
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
      )
    } catch {
      case _: Exception => throw new IllegalArgumentException(s"log file: ${fileName(batchId)} is broken")
    }
  }

  def deleteBatchLog(batchIds: List[Long])
                    (implicit storage: CloudStorage, conn: Connection): Unit =
    storage.deleteFiles(
      batchIds.map(fileName)
    )


  def mergeBatchLog(groupId: Long)
                   (implicit storage: CloudStorage, conn: Connection): Unit = {
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
      logs.filterNot(_.isTimeOut).flatMap(x => {
        val files = x.getFileList
        val loadedFile = x.getLoadedFileList
        files.filterNot(loadedFile.toSet)
      }),
      logs.filter(_.isTimeOut).flatMap(_.getFileList)
    ).save

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
  * "failedFileNames":[],
  * "timeOutFiles":[]
  * }
  *
  */
case class StreamingFailedFileReport(override val node: ObjectNode) extends SnowflakeLog {

  override implicit val logType: SnowflakeLogType = SnowflakeLogType.STREAMING_FAILED_FILE_REPORT

  lazy val groupId: Long = node.get(SnowflakeLogManager.GROUP_ID).asLong

  override def save(implicit storage: CloudStorage, conn: Connection): Unit = {
    val outputStream =
      storage.upload(
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

object StreamingFailedFileReport extends SnowflakeLogManager {

  def apply(groupId: Long, failedFiles: List[String], timeOutFiles: List[String]): StreamingFailedFileReport = {
    val node = mapper.createObjectNode()
    node.put(LOG_TYPE, SnowflakeLogType.STREAMING_FAILED_FILE_REPORT.toString)
    node.put(GROUP_ID, groupId)
    val arr = node.putArray(FAILED_FILE_NAMES)
    failedFiles.foreach(arr.add)
    val arr1 = node.putArray(TIME_OUT_FILES)
    timeOutFiles.foreach(arr1.add)
    StreamingFailedFileReport(node)
  }

  def logExists(groupId: Long)
               (implicit storage: CloudStorage, conn: Connection): Boolean =
    storage.fileExists(fileName(groupId))

  def fileName(groupId: Long): String =
    s"${SnowflakeLogManager.LOG_DIR}/failed_files/$groupId.json"

}


private[snowflake] object SnowflakeLogType extends Enumeration {
  type SnowflakeLogType = Value
  val STREAMING_BATCH_LOG,
  STREAMING_FAILED_FILE_REPORT = Value
}



