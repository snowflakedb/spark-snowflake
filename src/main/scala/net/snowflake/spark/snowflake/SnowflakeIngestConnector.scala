package net.snowflake.spark.snowflake

import java.security.{KeyPair, PrivateKey}
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import net.snowflake.ingest.SimpleIngestManager
import net.snowflake.ingest.connection.IngestStatus
import net.snowflake.ingest.utils.StagedFileWrapper
import net.snowflake.spark.snowflake.Parameters.MergedParameters

import scala.concurrent.{Await, Future, TimeoutException}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

object SnowflakeIngestConnector {

  /**
    * Check ingest load history, return a list of all failed file name
    *
    * @param files     a list of files being ingested
    * @param frequency frequency of load history requests
    * @param manager   ingest manager instance
    * @return a list of failed file name
    */
  def waitForFileHistory(files: List[String], frequency: Long = 5000)
                        (implicit manager: SimpleIngestManager): List[String] = {
    var checkList: Set[String] = files.toSet[String]
    var beginMark: String = null
    var failedFiles: List[String] = Nil

    while (checkList.nonEmpty) {
      Thread.sleep(frequency)
      val response = manager.getHistory(null, null, beginMark)
      beginMark = Option[String](response.getNextBeginMark).getOrElse(beginMark)
      if (response != null && response.files != null) {
        response.files.toList.foreach(entry =>
          if (entry.getPath != null && entry.isComplete && checkList.contains(entry.getPath)) {
            checkList -= entry.getPath
            if (entry.getStatus != IngestStatus.LOADED)
              failedFiles = entry.getPath :: failedFiles
          }
        )
      }
    }
    failedFiles
  }

  /**
    * list of (fileName, loading Succeed or not
    */
  def createHistoryChecker(ingestManager: SimpleIngestManager): () => List[(String, IngestStatus)] = {
    var beginMark: String = null
    () => {
      val response = ingestManager.getHistory(null, null, beginMark)
      beginMark = Option[String](response.getNextBeginMark).getOrElse(beginMark)
      if (response != null && response.files != null) {
        response.files.toList.flatMap(entry => {
          if (entry.getPath != null && entry.isComplete) {
            List((entry.getPath, entry.getStatus))
          } else Nil
        })
      } else Nil
    }
  }

  def checkHistoryByRange(ingestManager: SimpleIngestManager,
                          start: Long,
                          end: Long): List[(String, IngestStatus)] = {
    val response = ingestManager
      .getHistoryRange(null,
        timestampToDate(start),
        timestampToDate(end))
    if(response != null && response.files != null){
      response.files.toList.flatMap(entry => {
        if (entry.getPath != null && entry.isComplete) {
          List((entry.getPath, entry.getStatus))
        } else Nil
      })
    } else Nil
  }
  /**
    * timestamp to ISO-8601 Date
    */
  private def timestampToDate(time: Long): String = {
    val tz = TimeZone.getTimeZone("UTC")
    val df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'")
    df.setTimeZone(tz)
    df.format(new Date(time - 1000)) // 1 sec before
  }

  def createIngestManager(
                           account: String,
                           user: String,
                           pipe: String,
                           host: String,
                           privateKey: PrivateKey,
                           port: Int = 443,
                           scheme: String = "https"
                         ): SimpleIngestManager =
    new SimpleIngestManager(account, user, pipe, privateKey, scheme, host, port)

  /**
    * Ingest files and wait ingest history
    *
    * @param files   a list of file names
    * @param sec     time out in second
    * @param manager Ingest Manager
    * @return a list of failed file name, or None in case of time out
    */
  def ingestFilesAndCheck(files: List[String], sec: Long)
                         (implicit manager: SimpleIngestManager): Option[List[String]] = {
    ingestFiles(files)

    lazy val checker = Future {
      waitForFileHistory(files)
    }

    try {
      Some(Await.result(checker, sec second))
    }
    catch {
      case _: TimeoutException => None
    }


  }

  def ingestFiles(files: List[String])
                 (implicit manager: SimpleIngestManager): Unit =
    manager.ingestFiles(files.map(new StagedFileWrapper(_)).asJava, null)


  def createIngestManager(
                           param: MergedParameters,
                           pipeName: String
                         ): SimpleIngestManager = {
    val urlPattern = "^(https?://)?([^:]+)(:\\d+)?$".r
    val portPattern = ":(\\d+)".r
    val accountPattern = "([^\\.]+).+".r

    param.sfURL.trim match {
      case urlPattern(_, host, portStr) =>
        val scheme: String = if (param.isSslON) "https" else "http"

        val port: Int =
          if (portStr != null) {
            val portPattern(t) = portStr
            t.toInt
          } else if (param.isSslON) 443 else 80

        val accountPattern(account) = host

        require(
          param.privateKey.isDefined,
          "PEM Private key must be specified with 'pem_private_key' parameter"
        )



        val privateKey = param.privateKey.get

        val pipe = s"${param.sfDatabase}.${param.sfSchema}.$pipeName"

        createIngestManager(
          account,
          param.sfUser,
          pipe,
          host,
          privateKey,
          port,
          scheme
        )

      case _ => throw new IllegalArgumentException("incorrect url: " + param.sfURL)
    }
  }

}
