package net.snowflake.spark.snowflake.streaming

import java.security.PrivateKey
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import net.snowflake.ingest.SimpleIngestManager
import net.snowflake.ingest.connection.IngestStatus
import net.snowflake.ingest.utils.StagedFileWrapper
import net.snowflake.spark.snowflake.Parameters.MergedParameters

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}

object SnowflakeIngestConnector {
  /**
    * list of (fileName, loading Succeed or not
    */
  def createHistoryChecker(
    ingestManager: SimpleIngestManager
  ): () => List[(String, IngestStatus)] = {
    var beginMark: String = null
    () =>
      {
        val response = ingestManager.getHistory(null, null, beginMark)
        beginMark =
          Option[String](response.getNextBeginMark).getOrElse(beginMark)
        if (response != null && response.files != null) {
          response.files.asScala.toList.flatMap(entry => {
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
      .getHistoryRange(null, timestampToDate(start), timestampToDate(end))
    if (response != null && response.files != null) {
      response.files.asScala.toList.flatMap(entry => {
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

  def createIngestManager(account: String,
                          user: String,
                          pipe: String,
                          host: String,
                          privateKey: PrivateKey,
                          port: Int = 443,
                          scheme: String = "https"): SimpleIngestManager =
    new SimpleIngestManager(account, user, pipe, privateKey, scheme, host, port)

  def ingestFiles(
    files: List[String]
  )(implicit manager: SimpleIngestManager): Unit =
    manager.ingestFiles(files.map(new StagedFileWrapper(_)).asJava, null)

  def createIngestManager(param: MergedParameters,
                          pipeName: String): SimpleIngestManager = {
    val urlPattern = "^(https?://)?([^:]+)(:\\d+)?$".r
    val portPattern = ":(\\d+)".r
    val accountPattern = "([^.]+).+".r

    param.sfURL.trim match {
      case urlPattern(_, host, portStr) =>
        val scheme: String = if (param.isSslON) "https" else "http"

        val port: Int =
          if (portStr != null) {
            val portPattern(t) = portStr
            t.toInt
          } else if (param.isSslON) 443
          else 80

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

      case _ =>
        throw new IllegalArgumentException("incorrect url: " + param.sfURL)
    }
  }

}
