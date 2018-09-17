package net.snowflake.spark.snowflake


import java.sql.Connection

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.ingest.SimpleIngestManager
import net.snowflake.ingest.connection.IngestStatus
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.CloudStorage

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global


class SnowflakeIngestService(
                              param: MergedParameters,
                              pipeName: String,
                              storage: CloudStorage,
                              conn: Connection
                            ) {


  val SLEEP_TIME: Long = 60 * 1000
  val HISTORY_CHECK_TIME: Long = 60 * 60 * 1000

  lazy implicit val ingestManager: SimpleIngestManager =
    SnowflakeIngestConnector.createIngestManager(param, pipeName)

  var notClosed: Boolean = true

  val ingestedFileList: IngestedFileList = null // init

  //run clean function periodically
  val process = Future {
    val checker = SnowflakeIngestConnector.createHistoryChecker(ingestManager)
    var lastCleanTime: Long = 0
    while (notClosed) {
      val time = System.currentTimeMillis()
      val response: List[(String, IngestStatus)] = checker()
      ingestedFileList.checkResponseList(response)(conn)

      if(ingestedFileList.getFirstTimeStamp.isDefined &&
          time - lastCleanTime > HISTORY_CHECK_TIME){
        val start = ingestedFileList.getFirstTimeStamp
        val end = time
        //check
        lastCleanTime = time
      }
      Thread.sleep(SLEEP_TIME)
    }
  }

  def ingestFiles(list: List[String]): Unit = {
    SnowflakeIngestConnector.ingestFiles(list)
    ingestedFileList.addFiles(list)
  }


  def cleanAll(): Unit = {

  }

  def close(): Unit = {
    process.onComplete(_ => cleanAll())
    notClosed = false
  }

  /**
    * recover from logging files or create new data
    */
  private def init(): Unit = ???


}

object IngestLogManager {
  val LOG_DIR = "log"
  val mapper = new ObjectMapper()
}

sealed trait IngestLog {

  val storage: CloudStorage

  val fileName: String

  def save: Unit =
    storage.upload(fileName, Some(IngestLogManager.LOG_DIR), false)
      .write(toString.getBytes("UTF-8"))
}

case class FailedFileList(
                           override val storage: CloudStorage,
                           fileIndex: Int = 0
                         ) extends IngestLog {
  val MAX_FILE_SIZE: Int = 1000 //how many file names

  var fileSet: mutable.HashSet[String] = mutable.HashSet.empty[String]

  override lazy val fileName: String = s"failed_file_list_$fileIndex.json"

  def addFiles(names: List[String]): FailedFileList = {
    val part1 = names.slice(0, MAX_FILE_SIZE - fileSet.size)
    val part2 = names.slice(MAX_FILE_SIZE - fileSet.size, Int.MaxValue)

    fileSet ++= part1.toSet
    save
    if (part2.isEmpty) this
    else FailedFileList(storage, fileIndex + 1).addFiles(part2)
  }

  override def toString: String = {
    val node = IngestLogManager.mapper.createArrayNode()
    fileSet.foreach(node.add)
    node.toString
  }

}

case class IngestedFileList(
                             override val storage: CloudStorage,
                             var failedFileList: Option[FailedFileList] = None
                           ) extends IngestLog {
  override val fileName: String = "ingested_file_list.json"

  var failedFiles: FailedFileList = failedFileList.getOrElse(FailedFileList(storage))

  var fileList: mutable.PriorityQueue[(String, Long)] =
    mutable.PriorityQueue.empty[(String, Long)](Ordering.by[(String, Long), Long](_._2).reverse)

  def addFiles(names: List[String]): Unit = {
    val time = System.currentTimeMillis()
    names.foreach(fileList += _ ->time)
    save
  }

  override def toString: String = {
    val node = IngestLogManager.mapper.createArrayNode()
    fileList.foreach{
      case(name, time) => {
        val n = IngestLogManager.mapper.createObjectNode()
        n.put("name", name)
        n.put("time", time)
        node.add(n)
      }
    }
    node.toString
  }

  def checkResponseList(list: List[(String, IngestStatus)])
                       (implicit conn: Connection): Unit = {
    var toClean: List[String] = Nil
    var failed: List[String] = Nil

    list.foreach{
      case(name, status) => {
        if(fileList.exists(_._1 == name))
        status match {
          case IngestStatus.LOADED =>
            toClean = name :: toClean
            fileList = fileList.filterNot(_._1 == name)
          case IngestStatus.LOAD_FAILED | IngestStatus.PARTIALLY_LOADED =>
            failed = name :: failed
            fileList = fileList.filterNot(_._1 == name)
          case _ => //do nothing
        }
      }
    }
    if(toClean.nonEmpty) storage.deleteFiles(toClean)
    if(failed.nonEmpty) failedFiles = failedFiles.addFiles(failed)
    save
  }

  def getFirstTimeStamp: Option[Long] = if(fileList.isEmpty) None else Some(fileList.head._2)

}


