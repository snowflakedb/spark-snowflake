package net.snowflake.spark.snowflake

import java.nio.file.{Files, Paths}
import java.security.{KeyFactory, KeyPair, PrivateKey, PublicKey}
import java.security.spec.{PKCS8EncodedKeySpec, X509EncodedKeySpec}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import net.snowflake.ingest.SimpleIngestManager
import net.snowflake.ingest.connection.IngestStatus
import net.snowflake.ingest.utils.StagedFileWrapper
import net.snowflake.spark.snowflake.Parameters.MergedParameters

object SnowflakeIngestConnector {

  def privateKeyReader(fileName: String): PrivateKey = {
    val keyBytes: Array[Byte] = Files.readAllBytes(Paths.get(fileName))
    val spec: PKCS8EncodedKeySpec = new PKCS8EncodedKeySpec(keyBytes)
    KeyFactory.getInstance("RSA").generatePrivate(spec)
  }

  def publicKeyReader(fileName: String): PublicKey = {
    val keyBytes: Array[Byte] = Files.readAllBytes(Paths.get(fileName))
    val spec: X509EncodedKeySpec = new X509EncodedKeySpec(keyBytes)
    KeyFactory.getInstance("RSA").generatePublic(spec)
  }

  def createKeyPair(privateKeyName: String, publicKeyName: String): KeyPair =
    new KeyPair(publicKeyReader(publicKeyName), privateKeyReader(privateKeyName))

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

  def createIngestManager(
                           account: String,
                           user: String,
                           pipe: String,
                           host: String,
                           keyPair: KeyPair,
                           port: Int = 443,
                           scheme: String = "https"
                         ): SimpleIngestManager =
    new SimpleIngestManager(account, user, pipe, keyPair, scheme, host, port)

  def ingestFiles(files: List[String])(implicit manager: SimpleIngestManager): List[String] = {
    manager.ingestFiles(files.map(new StagedFileWrapper(_)).asJava, null)
    val failedFiles = waitForFileHistory(files)
    failedFiles
  }

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
          param.getPublicKeyPath.isDefined,
          "Public key location must be specified with 'public_key_path' parameter"
        )

        val publicKey = param.getPublicKeyPath.get

        require(
          param.getPrivateKeyPath.isDefined,
          "Private key location must be specified with 'private_key_path' parameter"
        )

        val privateKey = param.getPrivateKeyPath.get

        val pipe = s"${param.sfDatabase}.${param.sfSchema}.$pipeName"

        createIngestManager(
          account,
          param.sfUser,
          pipe,
          host,
          SnowflakeIngestConnector.createKeyPair(privateKey, publicKey),
          port,
          scheme
        )

      case _ => throw new IllegalArgumentException("incorrect url: " + param.sfURL)
    }
  }

}
