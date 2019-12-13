package net.snowflake.spark.snowflake.streaming

import java.nio.charset.Charset
import java.sql.Connection

import net.snowflake.client.jdbc.internal.apache.commons.logging.{Log, LogFactory}
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ArrayNode
import net.snowflake.ingest.SimpleIngestManager
import net.snowflake.ingest.connection.IngestStatus
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.CloudStorage

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class SnowflakeIngestService(param: MergedParameters,
                             pipeName: String,
                             storage: CloudStorage,
                             conn: Connection) {

  val SLEEP_TIME: Long = 60 * 1000 // 1m
  val HISTORY_CHECK_TIME: Long = 60 * 60 * 1000 // 1h
  val WAITING_TIME_ON_TERMINATION: Int = 10 // 10m

  lazy implicit val ingestManager: SimpleIngestManager =
    SnowflakeIngestConnector.createIngestManager(param, pipeName)

  private var notClosed: Boolean = true

  private val ingestedFileList: IngestedFileList = init()

  private lazy val checker =
    SnowflakeIngestConnector.createHistoryChecker(ingestManager)

  private var pipeDropped = false

  // run clean function periodically
  private val process = Future {
    while (notClosed) {
      Thread.sleep(SLEEP_TIME)
      val time = System.currentTimeMillis()
      ingestedFileList.checkResponseList(checker())
      if (ingestedFileList.getFirstTimeStamp.isDefined &&
          time - ingestedFileList.getFirstTimeStamp.get > HISTORY_CHECK_TIME) {
        ingestedFileList
          .checkResponseList(
            SnowflakeIngestConnector
              .checkHistoryByRange(
                ingestManager,
                ingestedFileList.getFirstTimeStamp.get,
                time
              )
          )
      }
    }
    cleanAll()
  }

  def ingestFiles(list: List[String]): Unit = {
    SnowflakeIngestConnector.ingestFiles(list)
    ingestedFileList.addFiles(list)
  }

  def cleanAll(): Unit = {
    while (ingestedFileList.nonEmpty) {
      Thread.sleep(SLEEP_TIME)
      val time = System.currentTimeMillis()
      if (time - ingestedFileList.getFirstTimeStamp.get > 10 * 60 * 1000) {
        ingestedFileList
          .checkResponseList(
            SnowflakeIngestConnector
              .checkHistoryByRange(
                ingestManager,
                ingestedFileList.getFirstTimeStamp.get,
                time
              )
          )
      } else ingestedFileList.checkResponseList(checker())
    }
    conn.dropPipe(pipeName)
    ingestedFileList.remove()
    pipeDropped = true

  }

  def close(): Unit = {
    val ct = System.currentTimeMillis()
    IngestContextManager.logger.debug("closing ingest service")
    notClosed = false
    Await.result(process, WAITING_TIME_ON_TERMINATION minutes)
    if (!pipeDropped) {
      IngestContextManager.logger.error(
        s"closing ingest service time out, please drop pipe: $pipeName manually"
      )
    }

    IngestContextManager.logger.debug(
      s"ingest service closed: ${(System.currentTimeMillis() - ct) / 1000.0}"
    )
  }

  /**
    * recover from context files or create new data
    */
  private def init(): IngestedFileList =
    IngestContextManager.readIngestList(storage, conn)

}

object IngestContextManager {
  val CONTEXT_DIR = "context"
  val INGEST_FILE_LIST_NAME = "ingested_file_list.json"
  val FAILED_FILE_INDEX = "failed_file_index"
  val LIST = "list"
  val NAME = "name"
  val TIME = "time"
  val mapper = new ObjectMapper()
  val logger: Log = LogFactory.getLog(getClass)

  def readIngestList(storage: CloudStorage,
                     conn: Connection): IngestedFileList = {
    val fileName = s"$CONTEXT_DIR/$INGEST_FILE_LIST_NAME"
    if (storage.fileExists(fileName)) {
      val inputStream = storage.download(fileName, compress = false)
      val buffer = ArrayBuffer.empty[Byte]
      var c: Int = inputStream.read()
      while (c != -1) {
        buffer.append(c.toByte)
        c = inputStream.read()
      }
      try {
        val node =
          mapper.readTree(new String(buffer.toArray, Charset.forName("UTF-8")))
        val failedIndex: Int = node.get(FAILED_FILE_INDEX).asInt()
        val failedList: FailedFileList =
          readFailedFileList(failedIndex, storage, conn)
        val arrNode = node.get(LIST).asInstanceOf[ArrayNode]
        var list: List[(String, Long)] = Nil
        (0 until arrNode.size()).foreach(i => {
          list = arrNode.get(i).get(NAME).asText() -> arrNode
            .get(i)
            .get(TIME)
            .asLong() :: list
        })
        IngestedFileList(storage, conn, Some(failedList), Some(list))
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"context file: $fileName is broken: $e"
          )
      }
    } else IngestedFileList(storage, conn)
  }

  def readFailedFileList(index: Int,
                         storage: CloudStorage,
                         conn: Connection): FailedFileList = {
    val fileName = s"$CONTEXT_DIR/failed_file_list_$index.json"
    if (storage.fileExists(fileName)) {
      val inputStream = storage.download(fileName, compress = false)
      val buffer = ArrayBuffer.empty[Byte]
      var c: Int = inputStream.read()
      while (c != -1) {
        buffer.append(c.toByte)
        c = inputStream.read()
      }
      try {
        val list = mapper
          .readTree(new String(buffer.toArray, Charset.forName("UTF-8")))
          .asInstanceOf[ArrayNode]
        var set = mutable.HashSet.empty[String]
        (0 until list.size()).foreach(i => {
          set += list.get(i).asText()
        })
        FailedFileList(storage, conn, index, Some(set))
      } catch {
        case e: Exception =>
          throw new IllegalArgumentException(
            s"context file: $fileName is broken: $e"
          )
      }
    } else FailedFileList(storage, conn, index)
  }

}

sealed trait IngestContext {

  val storage: CloudStorage

  val fileName: String

  val conn: Connection

  def save(): Unit = {
    IngestContextManager.logger.debug(s"$fileName:$toString")
    val output =
      storage.upload(fileName, Some(IngestContextManager.CONTEXT_DIR), compress = false)
    output.write(toString.getBytes("UTF-8"))
    output.close()

  }

}

case class FailedFileList(override val storage: CloudStorage,
                          override val conn: Connection,
                          fileIndex: Int = 0,
                          files: Option[mutable.HashSet[String]] = None)
    extends IngestContext {
  val MAX_FILE_SIZE: Int = 1000 // how many file names

  private var fileSet: mutable.HashSet[String] =
    files.getOrElse(mutable.HashSet.empty[String])

  override lazy val fileName: String = s"failed_file_list_$fileIndex.json"

  def addFiles(names: List[String]): FailedFileList = {
    val part1 = names.slice(0, MAX_FILE_SIZE - fileSet.size)
    val part2 = names.slice(MAX_FILE_SIZE - fileSet.size, Int.MaxValue)

    fileSet ++= part1.toSet
    save()
    if (part2.isEmpty) this
    else FailedFileList(storage, conn, fileIndex + 1).addFiles(part2)
  }

  override def toString: String = {
    val node = IngestContextManager.mapper.createArrayNode()
    fileSet.foreach(node.add)
    node.toString
  }

}

case class IngestedFileList(override val storage: CloudStorage,
                            override val conn: Connection,
                            failedFileList: Option[FailedFileList] = None,
                            ingestList: Option[List[(String, Long)]] = None)
    extends IngestContext {
  override val fileName: String = IngestContextManager.INGEST_FILE_LIST_NAME

  private var failedFiles: FailedFileList =
    failedFileList.getOrElse(FailedFileList(storage, conn))

  private var fileList: mutable.PriorityQueue[(String, Long)] =
    mutable.PriorityQueue
      .empty[(String, Long)](Ordering.by[(String, Long), Long](_._2).reverse)

  if (ingestList.isDefined) {
    ingestList.get.foreach(fileList += _)
  }

  def addFiles(names: List[String]): Unit = {
    val time = System.currentTimeMillis()
    names.foreach(fileList += _ -> time)
    save()
  }

  override def toString: String = {
    val node = IngestContextManager.mapper.createObjectNode()
    node.put(IngestContextManager.FAILED_FILE_INDEX, failedFiles.fileIndex)

    val arr = node.putArray(IngestContextManager.LIST)
    fileList.foreach {
      case (name, time) =>
        val n = IngestContextManager.mapper.createObjectNode()
        n.put(IngestContextManager.NAME, name)
        n.put(IngestContextManager.TIME, time)
        arr.add(n)
    }

    node.toString
  }

  def checkResponseList(list: List[(String, IngestStatus)]): Unit = {
    var toClean: List[String] = Nil
    var failed: List[String] = Nil

    list.foreach {
      case (name, status) =>
        if (fileList.exists(_._1 == name)) {
          status match {
            case IngestStatus.LOADED =>
              toClean = name :: toClean
              fileList = fileList.filterNot(_._1 == name)
            case IngestStatus.LOAD_FAILED | IngestStatus.PARTIALLY_LOADED =>
              failed = name :: failed
              fileList = fileList.filterNot(_._1 == name)
            case _ => // do nothing
          }
        }
    }
    if (toClean.nonEmpty) storage.deleteFiles(toClean)
    if (failed.nonEmpty) failedFiles = failedFiles.addFiles(failed)
    save()
  }

  def getFirstTimeStamp: Option[Long] =
    if (fileList.isEmpty) None else Some(fileList.head._2)

  def isEmpty: Boolean = fileList.isEmpty

  def nonEmpty: Boolean = fileList.nonEmpty

  def remove(): Unit =
    storage.deleteFile(IngestContextManager.CONTEXT_DIR + "/" + fileName)

}
