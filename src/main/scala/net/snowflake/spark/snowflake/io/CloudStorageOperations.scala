/*
 * Copyright 2015-2018 Snowflake Computing
 * Copyright 2015 TouchType Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.snowflake.spark.snowflake.io

import java.io.{InputStream, OutputStream}
import java.net.URI
import java.security.SecureRandom
import java.sql.Connection
import java.util
import java.util.zip.GZIPOutputStream

import javax.crypto.{Cipher, CipherOutputStream}
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import net.snowflake.client.jdbc.MatDesc
import net.snowflake.client.jdbc.cloud.storage.StageInfo.StageType
import net.snowflake.client.jdbc.internal.amazonaws.ClientConfiguration
import net.snowflake.client.jdbc.internal.amazonaws.auth.{BasicAWSCredentials, BasicSessionCredentials}
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3Client
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model._
import net.snowflake.client.jdbc.internal.amazonaws.util.Base64
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.{StorageCredentialsAnonymous, StorageCredentialsSharedAccessSignature}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.CloudBlobClient
import net.snowflake.spark.snowflake.{DefaultJDBCWrapper, SnowflakeConnectorUtils}
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.Utils
import net.snowflake.spark.snowflake.s3upload.StreamTransferManager
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.util.Random

object CloudStorageOperations {
  private[io] final val DEFAULT_PARALLELISM = 10
  private[io] final val S3_MAX_RETRIES = 3
  private final val AES = "AES"
  private final val AMZ_KEY: String = "x-amz-key"
  private final val AMZ_IV: String = "x-amz-iv"
  private final val DATA_CIPHER: String = "AES/CBC/PKCS5Padding"
  private final val KEY_CIPHER: String = "AES/ECB/PKCS5Padding"
  private final val AMZ_MATDESC = "x-amz-matdesc"

  private final val AZ_ENCRYPTIONDATA = "encryptiondata"
  private final val AZ_IV = "ContentEncryptionIV"
  private final val AZ_KEY_WRAP = "WrappedContentKey"
  private final val AZ_KEY = "EncryptedKey"
  private final val AZ_MATDESC = "matdesc"

  private val log = LoggerFactory.getLogger(getClass)

  private[io] final def getCipherAndS3Metadata(
                                                masterKey: String,
                                                queryId: String,
                                                smkId: String
                                              ): (Cipher, ObjectMetadata) = {
    val (cipher, matDesc, encKeK, ivData) = getCipherAndMetadata(masterKey, queryId, smkId)
    val meta = new ObjectMetadata()
    meta.addUserMetadata(AMZ_MATDESC, matDesc)
    meta.addUserMetadata(AMZ_KEY, encKeK)
    meta.addUserMetadata(AMZ_IV, ivData)
    (cipher, meta)
  }

  private[io] final def getCipherAndAZMetaData(
                                                masterKey: String,
                                                queryId: String,
                                                smkId: String
                                              ): (Cipher, util.HashMap[String, String]) = {


    def buildEncryptionMetadataJSON(iv64: String, key64: String): String =
      s"""
         | {"EncryptionMode":"FullBlob",
         | "WrappedContentKey":
         | {"KeyId":"symmKey1","EncryptedKey":"$key64","Algorithm":"AES_CBC_256"},
         | "EncryptionAgent":{"Protocol":"1.0","EncryptionAlgorithm":"AES_CBC_256"},
         | "ContentEncryptionIV":"$iv64",
         | "KeyWrappingMetadata":{"EncryptionLibrary":"Java 5.3.0"}}
       """.stripMargin

    val (cipher, matDesc, enKeK, ivData) = getCipherAndMetadata(masterKey, queryId, smkId)

    val meta = new util.HashMap[String, String]()

    meta.put(AZ_MATDESC, matDesc)
    meta.put(AZ_ENCRYPTIONDATA, buildEncryptionMetadataJSON(ivData, enKeK))

    (cipher, meta)
  }

  private[io] final def getCipherAndMetadata(
                                              masterKey: String,
                                              queryId: String,
                                              smkId: String): (Cipher, String, String, String) = {

    val decodedKey = Base64.decode(masterKey)
    val keySize = decodedKey.length
    val fileKeyBytes = new Array[Byte](keySize)
    val fileCipher = Cipher.getInstance(DATA_CIPHER)
    val blockSz = fileCipher.getBlockSize
    val ivData = new Array[Byte](blockSz)

    val secRnd = SecureRandom.getInstance("SHA1PRNG", "SUN")
    secRnd.nextBytes(new Array[Byte](10))

    secRnd.nextBytes(ivData)
    val iv = new IvParameterSpec(ivData)

    secRnd.nextBytes(fileKeyBytes)
    val fileKey = new SecretKeySpec(fileKeyBytes, 0, keySize, AES)

    fileCipher.init(Cipher.ENCRYPT_MODE, fileKey, iv)

    val keyCipher = Cipher.getInstance(KEY_CIPHER)
    val queryStageMasterKey = new SecretKeySpec(decodedKey, 0, keySize, AES)

    // Init cipher
    keyCipher.init(Cipher.ENCRYPT_MODE, queryStageMasterKey)
    val encKeK = keyCipher.doFinal(fileKeyBytes)

    val matDesc =
      new MatDesc(smkId.toLong, queryId, keySize * 8)

    (fileCipher, matDesc.toString, Base64.encodeAsString(encKeK: _*), Base64.encodeAsString(ivData: _*))
  }


  /**
    * @return Storage client and stage name
    */
  def createStorageClient(
                           param: MergedParameters,
                           conn: Connection,
                           tempStage: Boolean = true,
                           stage: Option[String] = None
                         ): (CloudStorage, String) = {
    val propolueSql = Utils.genPrologueSql(param)
    log.debug(propolueSql)
    DefaultJDBCWrapper.executeQueryInterruptibly(conn, propolueSql)


    val azure_url = "wasbs?://([^@]+)@([^\\.]+)\\.([^/]+)/(.*)".r
    val s3_url = "s3[an]://([^/]+)/(.*)".r
    val compress = param.sfCompress
    val stageName = stage
      .getOrElse(s"spark_connector_unload_stage_${Random.alphanumeric take 10 mkString ""}")

    param.rootTempDir match {
      //External Stage
      case azure_url(container, account, endpoint, path) =>
        require(param.azureSAS.isDefined, "missing Azure SAS")

        val azureSAS = param.azureSAS.get

        val sql =
          s"""
             |create or replace ${if (tempStage) "temporary" else ""} stage $stageName
             |url = 'azure://$account.$endpoint/$container/$path'
             |credentials =
             |(azure_sas_token='${azureSAS}')
         """.stripMargin

        DefaultJDBCWrapper.executeQueryInterruptibly(conn, sql)

        (AzureStorage(
          containerName = container,
          azureAccount = account,
          azureEndpoint = endpoint,
          azureSAS = azureSAS,
          compress = compress,
          pref = path
        ), stageName)

      case s3_url(bucket, prefix) =>
        require(param.awsAccessKey.isDefined, "missing aws access key")
        require(param.awsSecretKey.isDefined, "missing aws secret key")

        val sql =
          s"""
             |create or replace ${if (tempStage) "temporary" else ""} stage $stageName
             |url = 's3://$bucket/$prefix'
             |credentials =
             |(aws_key_id='${param.awsAccessKey.get}' aws_secret_key='${param.awsSecretKey.get}')
         """.stripMargin

        DefaultJDBCWrapper.executeQueryInterruptibly(conn, sql)

        (S3Storage(
          bucketName = bucket,
          awsId = param.awsAccessKey.get,
          awsKey = param.awsSecretKey.get,
          pref = prefix,
          compress = compress
        ), stageName)
      case _ => // Internal Stage

        val sql =
          s"""
             |create or replace ${if (tempStage) "temporary" else ""} stage $stageName
           """.stripMargin
        DefaultJDBCWrapper.executeQueryInterruptibly(conn, sql)

        @transient val stageManager =
          new SFInternalStage(true, DefaultJDBCWrapper, param, Some(stageName))
        //todo move stage creation from stage manager to this class

        @transient val keyIds = stageManager.getKeyIds
        val (_, queryId, smkId) = if (keyIds.nonEmpty) keyIds.head else ("", "", "")
        val masterKey = stageManager.masterKey
        val stageLocation = stageManager.stageLocation
        val url = "([^/]+)/?(.*)".r
        val url(bucket, path) = stageLocation

        stageManager.stageType match {
          case StageType.S3 =>
            val awsId = stageManager.awsId
            val awsKey = stageManager.awsKey
            val awsToken = stageManager.awsToken

            (S3Storage(
              bucketName = bucket,
              awsId = awsId.get,
              awsKey = awsKey.get,
              awsToken = awsToken,
              masterKey = Some(masterKey),
              queryId = Some(queryId),
              smkId = Some(smkId),
              compress = compress,
              pref = path
            ), stageName)

          case StageType.AZURE =>

            val azureSAS = stageManager.azureSAS.get
            val azureAccount = stageManager.azureAccountName.get
            val azureEndpoint = stageManager.azureEndpoint.get

            (AzureStorage(
              containerName = bucket,
              azureAccount = azureAccount,
              azureEndpoint = azureEndpoint,
              azureSAS = azureSAS,
              masterKey = Some(masterKey),
              queryId = Some(queryId),
              smkId = Some(smkId),
              compress = compress,
              pref = path
            ), stageName)


          case _ =>
            throw new UnsupportedOperationException(
              s"Only support s3 or Azure stage, stage types: ${stageManager.stageType}"
            )
        }
    }
  }

  /**
    * Save a string rdd to cloud storage
    *
    * @param data    data frame object
    * @param storage storage client
    * @return a list of file name
    */
  def saveToStorage(
                     data: RDD[String],
                     format: SupportedFormat = SupportedFormat.CSV,
                     dir: Option[String] = None
                   )(implicit storage: CloudStorage): List[String] =
    storage.upload(data, format, dir)

  def deleteFiles(files: List[String])(implicit storage: CloudStorage): Unit =
    storage.deleteFiles(files)


  private[io] def createS3Client(
                                  awsId: String,
                                  awsKey: String,
                                  awsToken: Option[String],
                                  parallelism: Int
                                ): AmazonS3Client = {
    val awsCredentials = awsToken match {
      case Some(token) => new BasicSessionCredentials(awsId, awsKey, token)
      case None => new BasicAWSCredentials(awsId, awsKey)
    }

    val clientConfig = new ClientConfiguration()
    clientConfig
      .setMaxConnections(parallelism)
    clientConfig
      .setMaxErrorRetry(CloudStorageOperations.S3_MAX_RETRIES)

    new AmazonS3Client(awsCredentials, clientConfig)
  }

  private[io] final def createAzureClient(
                                           storageAccount: String,
                                           endpoint: String,
                                           sas: Option[String] = None
                                         ): CloudBlobClient = {
    val storageEndpoint: URI =
      new URI("https",
        s"$storageAccount.$endpoint/", null, null)
    val azCreds =
      if (sas.isDefined) new StorageCredentialsSharedAccessSignature(sas.get)
      else StorageCredentialsAnonymous.ANONYMOUS

    new CloudBlobClient(storageEndpoint, azCreds)
  }

}

private class SingleElementIterator(fileName: String) extends Iterator[String] {

  private var name: Option[String] = Some(fileName)

  override def hasNext: Boolean = name.isDefined

  override def next(): String = {
    val t = name.get
    name = None
    t
  }
}

sealed trait CloudStorage {

  val pref: String

  lazy val prefix: String =
    if (pref.isEmpty) pref else if (pref.endsWith("/")) pref else pref + "/"

  def upload(data: RDD[String], format: SupportedFormat = SupportedFormat.CSV,
             dir: Option[String] = None): List[String]

  def download(fileName: String): InputStream

  def deleteFile(fileName: String): Unit

  def deleteFiles(fileNames: List[String]): Unit =
    fileNames.foreach(deleteFile)
}

case class AzureStorage(
                         containerName: String,
                         azureAccount: String,
                         azureEndpoint: String,
                         azureSAS: String,
                         masterKey: Option[String] = None,
                         queryId: Option[String] = None,
                         smkId: Option[String] = None,
                         compress: Boolean = false,
                         override val pref: String = ""
                       ) extends CloudStorage {

  override def upload(data: RDD[String],
                      format: SupportedFormat = SupportedFormat.CSV,
                      dir: Option[String]): List[String] = {
    val directory: String =
      dir match {
        case Some(str: String) => str
        case None => Random.alphanumeric take 10 mkString ""
      }

    val files = data.mapPartitions(rows => {

      val azureClient: CloudBlobClient =
        CloudStorageOperations
          .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))

      val fileName =
        s"$directory/${Random.alphanumeric take 10 mkString ""}.${format.toString}${if (compress) ".gz" else ""}"

      val container = azureClient.getContainerReference(containerName)
      val blob = container.getBlockBlobReference(prefix.concat(fileName))

      val outputStream: OutputStream =
        if (masterKey.isDefined) {
          val (cipher, meta) =
            CloudStorageOperations
              .getCipherAndAZMetaData(masterKey.get, queryId.get, smkId.get)
          blob.setMetadata(meta)
          val encryptedStream = new CipherOutputStream(blob.openOutputStream(), cipher)
          if(compress) new GZIPOutputStream(encryptedStream)
          else encryptedStream
        }
        else blob.openOutputStream()

      while (rows.hasNext) {
        outputStream.write(rows.next.getBytes("UTF-8"))
        outputStream.write('\n')
      }
      outputStream.close()

      new SingleElementIterator(fileName)
    })

    files.collect().toList
  }


  override def download(fileName: String): InputStream =
    throw new NotImplementedError()

  override def deleteFile(fileName: String): Unit =
    CloudStorageOperations
      .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))
      .getContainerReference(containerName)
      .getBlockBlobReference(prefix.concat(fileName)).deleteIfExists()

  override def deleteFiles(fileNames: List[String]): Unit = {
    val container =
      CloudStorageOperations
        .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))
        .getContainerReference(containerName)

    fileNames
      .map(prefix.concat)
      .foreach(container.getBlockBlobReference(_).deleteIfExists())
  }
}

case class S3Storage(
                      bucketName: String,
                      awsId: String,
                      awsKey: String,
                      awsToken: Option[String] = None,
                      masterKey: Option[String] = None,
                      queryId: Option[String] = None,
                      smkId: Option[String] = None,
                      compress: Boolean = false,
                      is256: Boolean = false,
                      override val pref: String = "",
                      parallelism: Int = CloudStorageOperations.DEFAULT_PARALLELISM
                    ) extends CloudStorage {

  //future work, replace io operation in RDD and writer
  override def upload(data: RDD[String],
                      format: SupportedFormat = SupportedFormat.CSV,
                      dir: Option[String] = None): List[String] = {

    val directory: String =
      dir match {
        case Some(str: String) => str
        case None => Random.alphanumeric take 10 mkString ""
      }


    val files = data.mapPartitions(rows => {

      val s3Client: AmazonS3Client = CloudStorageOperations.createS3Client(awsId, awsKey, awsToken, parallelism)

      val fileName =
        s"$directory/${Random.alphanumeric take 10 mkString ""}.${format.toString}${if (compress) ".gz" else ""}"

      val (fileCipher, meta) =
        masterKey match {
          case Some(_) =>
            CloudStorageOperations.getCipherAndS3Metadata(masterKey.get, queryId.get, smkId.get)
          case None =>
            (null, new ObjectMetadata())
        }


      if (compress) meta.setContentEncoding("GZIP")

      val streamTransferManager = new StreamTransferManager(
        bucketName,
        prefix + fileName,
        s3Client,
        meta,
        1,
        parallelism,
        5 * parallelism,
        50
      )

      try {
        val uploadStream = streamTransferManager.getMultiPartOutputStreams.get(0)
        var outputStream: OutputStream = uploadStream

        if (masterKey.isDefined) outputStream =
          new CipherOutputStream(outputStream, fileCipher)

        if (compress)
          outputStream = new GZIPOutputStream(outputStream)

        while (rows.hasNext) {
          outputStream.write(rows.next.getBytes("UTF-8"))
          outputStream.write('\n')
          uploadStream.checkSize()
        }

        outputStream.close()

        streamTransferManager.complete()
      } catch {
        case ex: Exception =>
          streamTransferManager.abort()
          SnowflakeConnectorUtils.handleS3Exception(ex)
      }

      new SingleElementIterator(fileName)
    })

    files.collect().toList
  }


  //todo
  override def download(fileName: String): InputStream =
    throw new NotImplementedError()

  override def deleteFile(fileName: String): Unit =
    CloudStorageOperations
      .createS3Client(awsId, awsKey, awsToken, parallelism)
      .deleteObject(bucketName, prefix.concat(fileName))

  override def deleteFiles(fileNames: List[String]): Unit =
    CloudStorageOperations
      .createS3Client(awsId, awsKey, awsToken, parallelism)
      .deleteObjects(
        new DeleteObjectsRequest(bucketName)
          .withKeys(fileNames.map(prefix.concat): _*)
      )


}

//todo: google cloud, local file for testing?
