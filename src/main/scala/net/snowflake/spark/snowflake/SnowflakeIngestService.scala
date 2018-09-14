package net.snowflake.spark.snowflake


import java.sql.Connection

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.ingest.SimpleIngestManager
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


  val sleepTime: Long = 60 * 1000

  val LOG_DIR = "log"

  val CLEAN_LIST_FILE = s"clean_list.json"

  val FAILED_FILE_LIST_FILE = s"failed_file_list.json"

  val mapper = new ObjectMapper()


  lazy implicit val ingestManager: SimpleIngestManager =
    SnowflakeIngestConnector.createIngestManager(param, pipeName)

  var notClosed: Boolean = true

  var cleanList: mutable.HashSet[String] = mutable.HashSet.empty[String]

  var failedFileList: List[String] = Nil


  //run clean function periodically
  val process = Future {
    val checker = SnowflakeIngestConnector.createHistoryChecker(ingestManager)
    while (notClosed) {
      //remove file name and file
      Thread.sleep(sleepTime)
    }
  }

  def ingestFiles(list: List[String]): Unit = {
    SnowflakeIngestConnector.ingestFiles(list)
    cleanList ++= list.toSet
    updateCleanList()
  }

//  def clean(): Unit = {
//
//  }

  def cleanAll(): Unit = ???

  def close(): Unit = {
    process.onComplete(_=>cleanAll())
    notClosed = false
  }

  /**
    * recover from logging files or create new data
    */
  private def init(): Unit = ???

  private def updateCleanList(): Unit = {
    val node = mapper.createArrayNode()
    cleanList.foreach(node.add)
    save(node.toString, CLEAN_LIST_FILE)

  }

  private def updateFailedFileList(): Unit = {
    val node = mapper.createArrayNode()
    failedFileList.foreach(node.add)
    save(node.toString, FAILED_FILE_LIST_FILE)
  }

  private def save(text: String, fileName: String): Unit =
    storage.upload(fileName,Some(LOG_DIR),false)
    .write(text.getBytes("UTF-8"))

}
