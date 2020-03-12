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

import java.io._
import java.net.URI
import java.security.SecureRandom
import java.sql.Connection
import java.util
import java.util.Properties
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import javax.crypto.{Cipher, CipherInputStream, CipherOutputStream, SecretKey}
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import net.snowflake.client.core.{OCSPMode, SFStatement}
import net.snowflake.client.jdbc.{
  ErrorCode,
  MatDesc,
  SnowflakeConnectionV1,
  SnowflakeFileTransferAgent,
  SnowflakeFileTransferConfig,
  SnowflakeFileTransferMetadata,
  SnowflakeSQLException
}
import net.snowflake.client.jdbc.cloud.storage.StageInfo.StageType
import net.snowflake.client.jdbc.internal.amazonaws.ClientConfiguration
import net.snowflake.client.jdbc.internal.amazonaws.auth.{
  BasicAWSCredentials,
  BasicSessionCredentials
}
import net.snowflake.client.jdbc.internal.amazonaws.retry.{
  PredefinedRetryPolicies,
  RetryPolicy
}
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3Client
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model._
import net.snowflake.client.jdbc.internal.amazonaws.util.Base64
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{
  JsonNode,
  ObjectMapper
}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.{
  StorageCredentialsAnonymous,
  StorageCredentialsSharedAccessSignature
}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.CloudBlobClient
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState
import net.snowflake.spark.snowflake._
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import net.snowflake.spark.snowflake.test.{TestHook, TestHookFlag}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object CloudStorageOperations {
  private[io] final val DEFAULT_PARALLELISM = 10
  private[io] final val S3_MAX_RETRIES = 6
  private[io] final val S3_MAX_TIMEOUT_MS = 30 * 1000
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

  val log: Logger = LoggerFactory.getLogger(getClass)

  private[io] final def getDecryptedStream(
    stream: InputStream,
    masterKey: String,
    metaData: util.Map[String, String],
    stageType: StageType
  ): InputStream = {

    val decodedKey = Base64.decode(masterKey)
    val (key, iv) =
      stageType match {
        case StageType.S3 =>
          (metaData.get(AMZ_KEY), metaData.get(AMZ_IV))
        case StageType.AZURE =>
          parseEncryptionData(metaData.get(AZ_ENCRYPTIONDATA))
        case _ =>
          throw new UnsupportedOperationException(
            s"Only support s3 or azure stage. Stage Type: $stageType"
          )
      }

    if (key == null || iv == null) {
      throw new SnowflakeSQLException(
        SqlState.INTERNAL_ERROR,
        ErrorCode.INTERNAL_ERROR.getMessageCode,
        "File " + "metadata incomplete"
      )
    }

    val keyBytes: Array[Byte] = Base64.decode(key)
    val ivBytes: Array[Byte] = Base64.decode(iv)

    val queryStageMasterKey: SecretKey =
      new SecretKeySpec(decodedKey, 0, decodedKey.length, AES)

    val keyCipher: Cipher = Cipher.getInstance(KEY_CIPHER)
    keyCipher.init(Cipher.DECRYPT_MODE, queryStageMasterKey)

    val fileKeyBytes: Array[Byte] = keyCipher.doFinal(keyBytes)
    val fileKey =
      new SecretKeySpec(fileKeyBytes, 0, decodedKey.length, AES)

    val dataCipher = Cipher.getInstance(DATA_CIPHER)
    val ivy: IvParameterSpec = new IvParameterSpec(ivBytes)
    dataCipher.init(Cipher.DECRYPT_MODE, fileKey, ivy)
    new CipherInputStream(stream, dataCipher)
  }

  private[io] final def parseEncryptionData(
    jsonEncryptionData: String
  ): (String, String) = {
    val mapper: ObjectMapper = new ObjectMapper()
    val encryptionDataNode: JsonNode = mapper.readTree(jsonEncryptionData)
    val iv: String = encryptionDataNode.findValue(AZ_IV).asText()
    val key: String = encryptionDataNode
      .findValue(AZ_KEY_WRAP)
      .findValue(AZ_KEY)
      .asText()
    (key, iv)
  }

  private[io] final def getCipherAndS3Metadata(
    masterKey: String,
    queryId: String,
    smkId: String
  ): (Cipher, ObjectMetadata) = {
    val (cipher, matDesc, encKeK, ivData) =
      getCipherAndMetadata(masterKey, queryId, smkId)
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

    val (cipher, matDesc, enKeK, ivData) =
      getCipherAndMetadata(masterKey, queryId, smkId)

    val meta = new util.HashMap[String, String]()

    meta.put(AZ_MATDESC, matDesc)
    meta.put(AZ_ENCRYPTIONDATA, buildEncryptionMetadataJSON(ivData, enKeK))

    (cipher, meta)
  }

  private[io] final def getCipherAndMetadata(
    masterKey: String,
    queryId: String,
    smkId: String
  ): (Cipher, String, String, String) = {

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

    (
      fileCipher,
      matDesc.toString,
      Base64.encodeAsString(encKeK: _*),
      Base64.encodeAsString(ivData: _*)
    )
  }

  def createStorageClientFromStage(param: MergedParameters,
                                   conn: Connection,
                                   stageName: String,
                                   dir: Option[String] = None,
                                   temporary: Boolean = false): CloudStorage = {
    conn.createStage(stageName, temporary = temporary)
    @transient val stageManager =
      new SFInternalStage(
        false,
        param,
        stageName,
        conn.asInstanceOf[SnowflakeConnectionV1]
      )

    stageManager.stageType match {
      case StageType.S3 =>
        InternalS3Storage(param, stageName, conn)

      case StageType.AZURE =>
        InternalAzureStorage(param, stageName, conn)

      case StageType.GCS =>
        InternalGcsStorage(param, stageName, conn, stageManager)

      case _ =>
        throw new UnsupportedOperationException(
          s"""Only support s3, Azure or Gcs stage,
             | stage types: ${stageManager.stageType}
             |""".stripMargin
        )
    }
  }

  /**
    * @return Storage client and stage name
    */
  def createStorageClient(
    param: MergedParameters,
    conn: Connection,
    tempStage: Boolean = true,
    stage: Option[String] = None,
    operation: String = "unload"
  ): (CloudStorage, String) = {
    val azure_url = "wasbs?://([^@]+)@([^.]+)\\.([^/]+)/(.*)".r
    val s3_url = "s3[an]://([^/]+)/(.*)".r
    val gcs_url = "gcs://([^/]+)/(.*)".r

    val stageName = stage
      .getOrElse(
        s"spark_connector_${operation}_stage_${Random.alphanumeric take 10 mkString ""}"
      )

    param.rootTempDir match {
      // External Stage
      case azure_url(container, account, endpoint, path) =>
        require(param.azureSAS.isDefined, "missing Azure SAS")

        val azureSAS = param.azureSAS.get

        val sql =
          s"""
             |create or replace ${if (tempStage) "temporary" else ""} stage $stageName
             |url = 'azure://$account.$endpoint/$container/$path'
             |credentials =
             |(azure_sas_token='$azureSAS')
         """.stripMargin

        DefaultJDBCWrapper.executeQueryInterruptibly(conn, sql)

        (
          ExternalAzureStorage(
            containerName = container,
            azureAccount = account,
            azureEndpoint = endpoint,
            azureSAS = azureSAS,
            param.proxyInfo,
            param.maxRetryCount,
            param.expectedPartitionCount,
            pref = path,
            connection = conn
          ),
          stageName
        )

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

        (
          ExternalS3Storage(
            bucketName = bucket,
            awsId = param.awsAccessKey.get,
            awsKey = param.awsSecretKey.get,
            param.proxyInfo,
            param.maxRetryCount,
            param.expectedPartitionCount,
            pref = prefix,
            connection = conn
          ),
          stageName
        )

      case gcs_url(_, _) =>
        throw new SnowflakeConnectorFeatureNotSupportException(
          s"Doesn't support GCS external stage. url: ${param.rootTempDir}"
        )

      case _ => // Internal Stage
        (
          createStorageClientFromStage(param, conn, stageName, None, tempStage),
          stageName
        )
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
    dir: Option[String] = None,
    compress: Boolean = true
  )(implicit storage: CloudStorage): List[String] =
    storage.upload(data, format, dir, compress)

  def deleteFiles(files: List[String])(implicit storage: CloudStorage,
                                       connection: Connection): Unit =
    storage.deleteFiles(files)

  private[io] def createS3Client(
    awsId: String,
    awsKey: String,
    awsToken: Option[String],
    parallelism: Int,
    proxyInfo: Option[ProxyInfo]
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

    clientConfig.setRetryPolicy(
      new RetryPolicy(
        PredefinedRetryPolicies.DEFAULT_RETRY_CONDITION,
        PredefinedRetryPolicies.DEFAULT_BACKOFF_STRATEGY,
        CloudStorageOperations.S3_MAX_RETRIES,
        true
      )
    )

    clientConfig.setConnectionTimeout(CloudStorageOperations.S3_MAX_TIMEOUT_MS)

    proxyInfo match {
      case Some(proxyInfoValue) =>
        proxyInfoValue.setProxyForS3(clientConfig)
      case None =>
    }

    new AmazonS3Client(awsCredentials, clientConfig)
  }

  private[io] final def createAzureClient(
    storageAccount: String,
    endpoint: String,
    sas: Option[String] = None,
    proxyInfo: Option[ProxyInfo]
  ): CloudBlobClient = {
    val storageEndpoint: URI =
      new URI("https", s"$storageAccount.$endpoint/", null, null)
    val azCreds =
      if (sas.isDefined) new StorageCredentialsSharedAccessSignature(sas.get)
      else StorageCredentialsAnonymous.ANONYMOUS
    proxyInfo match {
      case Some(proxyInfoValue) =>
        proxyInfoValue.setProxyForAzure()
      case None =>
    }

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

private[io] object StorageInfo {
  @inline val BUCKET_NAME = "bucketName"
  @inline val AWS_ID = "awsId"
  @inline val AWS_KEY = "awsKey"
  @inline val AWS_TOKEN = "awsToken"
  @inline val MASTER_KEY = "masterKey"
  @inline val QUERY_ID = "queryId"
  @inline val SMK_ID = "smkId"
  @inline val PREFIX = "prefix"
  @inline val CONTAINER_NAME = "containerName"
  @inline val AZURE_ACCOUNT = "azureAccount"
  @inline val AZURE_END_POINT = "azureEndPoint"
  @inline val AZURE_SAS = "azureSAS"
}

sealed trait CloudStorage {
  protected val RETRY_SLEEP_TIME_UNIT_IN_MS: Long = 200
  private var processedFileCount = 0
  protected val connection: Connection

  protected def getFileName(fileIndex: Int,
                            format: SupportedFormat,
                            compress: Boolean): String = {
    s"$fileIndex.${format.toString}${if (compress) ".gz" else ""}"
  }

  protected def getStageInfo(
    isWrite: Boolean,
    fileName: String = ""
  ): (Map[String, String], List[String]) =
    (new HashMap[String, String], List())

  def upload(fileName: String,
             dir: Option[String],
             compress: Boolean): OutputStream =
    createUploadStream(fileName, dir, compress, getStageInfo(isWrite = true)._1)

  def upload(data: RDD[String],
             format: SupportedFormat = SupportedFormat.CSV,
             dir: Option[String],
             compress: Boolean = true): List[String] =
    uploadRDD(data, format, dir, compress, getStageInfo(isWrite = true)._1)

  protected def uploadRDD(data: RDD[String],
                          format: SupportedFormat = SupportedFormat.CSV,
                          dir: Option[String],
                          compress: Boolean = true,
                          storageInfo: Map[String, String]): List[String] = {

    val directory: String =
      dir match {
        case Some(str: String) => str
        case None => Random.alphanumeric take 10 mkString ""
      }
    val files = data.mapPartitionsWithIndex {
      case (index, rows) =>
        val fileName = getFileName(index, format, compress)

        val outputStream =
          createUploadStream(fileName, Some(directory), compress, storageInfo)
        while (rows.hasNext) {
          outputStream.write(rows.next.getBytes("UTF-8"))
          outputStream.write('\n')
        }
        outputStream.close()

        CloudStorageOperations.log.info(
          SnowflakeResultSetRDD.WORKER_LOG_PREFIX +
          s": upload file $directory/$fileName to stage"
        )

        new SingleElementIterator(s"$directory/$fileName")
    }

    files.collect().toList
  }

  // Implement retry logic when download fails and finish the file download.
  def createDownloadStreamWithRetry(fileName: String,
                                    compress: Boolean,
                                    storageInfo: Map[String, String],
                                    maxRetryCount: Int): InputStream = {
    // download the file with retry and backoff and then consume.
    // val maxRetryCount = 10
    var retryCount = 0
    var error: Option[Throwable] = None

    do {
      try {
        val startTime = System.currentTimeMillis()
        // Create original download stream
        var inputStream = createDownloadStream(fileName, compress, storageInfo)

        // If max retry count is greater than 1, enable the file download.
        // This provides a way to disable the file download if necessary.
        val download = maxRetryCount > 1
        if (download) {
          // Download, decrypt and decompress the data
          val cachedData = IOUtils.toByteArray(inputStream)

          // Generate inputStream for the cached data buffer.
          inputStream = new ByteArrayInputStream(cachedData)
          val endTime = System.currentTimeMillis()
          val downloadTime = (endTime - startTime).toDouble / 1000.0

          CloudStorageOperations.log.info(
            s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: download
               | successful: fileID=$processedFileCount
               | downloadTime=$downloadTime
               | dataSize=${Utils.getSizeString(cachedData.size)}
               |""".stripMargin.filter(_ >= ' ')
          )
        } else {
          CloudStorageOperations.log.info(
            s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: DO NOT download
               | the file completely: fileID=$processedFileCount
               |""".stripMargin.filter(_ >= ' ')
          )
        }

        // succeed download return
        processedFileCount += 1
        return inputStream
      } catch {
        // Find problem to download the file, sleep some time and retry.
        case e: Throwable => {
          error = Some(e)
          val errmsg = e.getMessage
          CloudStorageOperations.log.info(
            s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: hit download error:
               | retryCount=$retryCount: fileName=$fileName,
               | maxRetryCount=$maxRetryCount error details: [ $errmsg ]
               |""".stripMargin.filter(_ >= ' ')
          )
        }
        retryCount = retryCount + 1
        // sleep some time and retry
        Thread.sleep(RETRY_SLEEP_TIME_UNIT_IN_MS * retryCount)
      }
    } while (retryCount < maxRetryCount)

    // Fail to download data after retry
    if (error.isDefined) {
      val errorMessage = error.get.getMessage
      CloudStorageOperations.log.info(
        s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: last error message
           | after retry $retryCount times is [ $errorMessage ]
           |""".stripMargin.filter(_ >= ' ')
      )
      throw error.get
    } else {
      // Suppose this never happens
      val errorMessage = "Unknown error occurs, Contact Snowflake Support"
      CloudStorageOperations.log.info(
        s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: hit un-expected
           | condition after retry $retryCount times is
           | [ $retryCount/$maxRetryCount ]
           |""".stripMargin.filter(_ >= ' ')
      )
      throw new Exception(errorMessage)
    }
  }

  protected def createUploadStream(
    fileName: String,
    dir: Option[String],
    compress: Boolean,
    storageInfo: Map[String, String]
  ): OutputStream

  def download(fileName: String, compress: Boolean): InputStream =
    createDownloadStream(fileName, compress, getStageInfo(isWrite = false, fileName)._1)

  def download(sc: SparkContext,
               format: SupportedFormat = SupportedFormat.CSV,
               compress: Boolean = true,
               subDir: String = ""): RDD[String]

  protected def createDownloadStream(
    fileName: String,
    compress: Boolean,
    storageInfo: Map[String, String]
  ): InputStream

  def deleteFile(fileName: String): Unit

  def deleteFiles(fileNames: List[String]): Unit =
    fileNames.foreach(deleteFile)

  def fileExists(fileName: String): Boolean
}

case class InternalAzureStorage(param: MergedParameters,
                                stageName: String,
                                @transient override val connection: Connection)
    extends CloudStorage {

  val proxyInfo: Option[ProxyInfo] = param.proxyInfo

  override protected def getStageInfo(
    isWrite: Boolean,
    fileName: String = ""
  ): (Map[String, String], List[String]) = {
    @transient val stageManager =
      new SFInternalStage(
        isWrite,
        param,
        stageName,
        connection.asInstanceOf[SnowflakeConnectionV1],
        fileName
      )
    @transient val keyIds = stageManager.getKeyIds

    var storageInfo: Map[String, String] = new HashMap[String, String]()

    val (_, queryId, smkId) = if (keyIds.nonEmpty) keyIds.head else ("", "", "")
    storageInfo += StorageInfo.QUERY_ID -> queryId
    storageInfo += StorageInfo.SMK_ID -> smkId
    storageInfo += StorageInfo.MASTER_KEY -> stageManager.masterKey

    val stageLocation = stageManager.stageLocation
    val url = "([^/]+)/?(.*)".r
    val url(container, path) = stageLocation
    storageInfo += StorageInfo.CONTAINER_NAME -> container
    storageInfo += StorageInfo.AZURE_SAS -> stageManager.azureSAS.get
    storageInfo += StorageInfo.AZURE_ACCOUNT -> stageManager.azureAccountName.get
    storageInfo += StorageInfo.AZURE_END_POINT -> stageManager.azureEndpoint.get

    val prefix: String =
      if (path.isEmpty) path else if (path.endsWith("/")) path else path + "/"
    storageInfo += StorageInfo.PREFIX -> prefix
    val fileList: List[String] =
      if (isWrite) List() else stageManager.getKeyIds.map(_._1).toList
    (storageInfo, fileList)
  }

  def download(sc: SparkContext,
               format: SupportedFormat = SupportedFormat.CSV,
               compress: Boolean = true,
               subDir: String = ""): RDD[String] = {
    val (stageInfo, fileList) = getStageInfo(isWrite = false)
    new SnowflakeRDD(
      sc,
      fileList,
      format,
      createDownloadStreamWithRetry(
        _,
        compress,
        stageInfo,
        param.maxRetryCount
      ),
      param.expectedPartitionCount
    )
  }

  override protected def createUploadStream(
    fileName: String,
    dir: Option[String],
    compress: Boolean,
    storageInfo: Map[String, String]
  ): OutputStream = {

    val file: String =
      if (dir.isDefined) s"${dir.get}/$fileName"
      else fileName

    val blob =
      CloudStorageOperations
        .createAzureClient(
          storageInfo(StorageInfo.AZURE_ACCOUNT),
          storageInfo(StorageInfo.AZURE_END_POINT),
          storageInfo.get(StorageInfo.AZURE_SAS),
          proxyInfo
        )
        .getContainerReference(storageInfo(StorageInfo.CONTAINER_NAME))
        .getBlockBlobReference(storageInfo(StorageInfo.PREFIX).concat(file))

    val encryptedStream = {
      val (cipher, meta) =
        CloudStorageOperations
          .getCipherAndAZMetaData(
            storageInfo(StorageInfo.MASTER_KEY),
            storageInfo(StorageInfo.QUERY_ID),
            storageInfo(StorageInfo.SMK_ID)
          )
      blob.setMetadata(meta)
      new CipherOutputStream(blob.openOutputStream(), cipher)
    }
    if (compress) new GZIPOutputStream(encryptedStream) else encryptedStream
  }

  override def deleteFile(fileName: String): Unit = {

    val (storageInfo, _) = getStageInfo(isWrite = true)

    CloudStorageOperations
      .createAzureClient(
        storageInfo(StorageInfo.AZURE_ACCOUNT),
        storageInfo(StorageInfo.AZURE_END_POINT),
        storageInfo.get(StorageInfo.AZURE_SAS),
        proxyInfo
      )
      .getContainerReference(storageInfo(StorageInfo.CONTAINER_NAME))
      .getBlockBlobReference(storageInfo(StorageInfo.PREFIX).concat(fileName))
      .deleteIfExists()
  }

  override def deleteFiles(fileNames: List[String]): Unit = {
    val (storageInfo, _) = getStageInfo(isWrite = true)

    val container = CloudStorageOperations
      .createAzureClient(
        storageInfo(StorageInfo.AZURE_ACCOUNT),
        storageInfo(StorageInfo.AZURE_END_POINT),
        storageInfo.get(StorageInfo.AZURE_SAS),
        proxyInfo
      )
      .getContainerReference(storageInfo(StorageInfo.CONTAINER_NAME))

    fileNames
      .map(storageInfo(StorageInfo.PREFIX).concat)
      .foreach(container.getBlockBlobReference(_).deleteIfExists())
  }

  override def fileExists(fileName: String): Boolean = {
    val (storageInfo, _) = getStageInfo(isWrite = false)
    CloudStorageOperations
      .createAzureClient(
        storageInfo(StorageInfo.AZURE_ACCOUNT),
        storageInfo(StorageInfo.AZURE_END_POINT),
        storageInfo.get(StorageInfo.AZURE_SAS),
        proxyInfo
      )
      .getContainerReference(storageInfo(StorageInfo.CONTAINER_NAME))
      .getBlockBlobReference(storageInfo(StorageInfo.PREFIX).concat(fileName))
      .exists()
  }

  override protected def createDownloadStream(
    fileName: String,
    compress: Boolean,
    storageInfo: Map[String, String]
  ): InputStream = {
    val blob = CloudStorageOperations
      .createAzureClient(
        storageInfo(StorageInfo.AZURE_ACCOUNT),
        storageInfo(StorageInfo.AZURE_END_POINT),
        storageInfo.get(StorageInfo.AZURE_SAS),
        proxyInfo
      )
      .getContainerReference(storageInfo(StorageInfo.CONTAINER_NAME))
      .getBlockBlobReference(storageInfo(StorageInfo.PREFIX).concat(fileName))

    val azureStorage: ByteArrayOutputStream = new ByteArrayOutputStream()
    blob.download(azureStorage)
    blob.downloadAttributes()

    val inputStream: InputStream =
      CloudStorageOperations.getDecryptedStream(
        new ByteArrayInputStream(azureStorage.toByteArray),
        storageInfo(StorageInfo.MASTER_KEY),
        blob.getMetadata,
        StageType.AZURE
      )

    if (compress) new GZIPInputStream(inputStream) else inputStream
  }
}

case class ExternalAzureStorage(containerName: String,
                                azureAccount: String,
                                azureEndpoint: String,
                                azureSAS: String,
                                proxyInfo: Option[ProxyInfo],
                                maxRetryCount: Int,
                                fileCountPerPartition: Int,
                                pref: String = "",
                                @transient override val connection: Connection)
    extends CloudStorage {

  lazy val prefix: String =
    if (pref.isEmpty) pref else if (pref.endsWith("/")) pref else pref + "/"

  override protected def createUploadStream(
    fileName: String,
    dir: Option[String],
    compress: Boolean,
    storageInfo: Map[String, String]
  ): OutputStream = {
    val file: String =
      if (dir.isDefined) s"${dir.get}/$fileName"
      else fileName

    val blob =
      CloudStorageOperations
        .createAzureClient(
          azureAccount,
          azureEndpoint,
          Some(azureSAS),
          proxyInfo
        )
        .getContainerReference(containerName)
        .getBlockBlobReference(prefix.concat(file))

    if (compress) new GZIPOutputStream(blob.openOutputStream())
    else blob.openOutputStream()
  }

  override def deleteFile(fileName: String): Unit =
    CloudStorageOperations
      .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS), proxyInfo)
      .getContainerReference(containerName)
      .getBlockBlobReference(prefix.concat(fileName))
      .deleteIfExists()

  override def deleteFiles(fileNames: List[String]): Unit = {
    val container =
      CloudStorageOperations
        .createAzureClient(
          azureAccount,
          azureEndpoint,
          Some(azureSAS),
          proxyInfo
        )
        .getContainerReference(containerName)

    fileNames
      .map(prefix.concat)
      .foreach(container.getBlockBlobReference(_).deleteIfExists())
  }

  override def fileExists(fileName: String): Boolean =
    CloudStorageOperations
      .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS), proxyInfo)
      .getContainerReference(containerName)
      .getBlockBlobReference(prefix.concat(fileName))
      .exists()

  override protected def createDownloadStream(
    fileName: String,
    compress: Boolean,
    storageInfo: Map[String, String]
  ): InputStream = {
    val blob = CloudStorageOperations
      .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS), proxyInfo)
      .getContainerReference(containerName)
      .getBlockBlobReference(prefix.concat(fileName))

    val azureStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    blob.download(azureStream)

    val inputStream: InputStream =
      new ByteArrayInputStream(azureStream.toByteArray)

    if (compress) new GZIPInputStream(inputStream) else inputStream

  }

  override def download(sc: SparkContext,
                        format: SupportedFormat,
                        compress: Boolean,
                        subDir: String): RDD[String] = {
    new SnowflakeRDD(
      sc,
      getFileNames(subDir),
      format,
      createDownloadStreamWithRetry(
        _,
        compress,
        Map.empty[String, String],
        maxRetryCount
      ),
      fileCountPerPartition
    )
  }

  private def getFileNames(subDir: String): List[String] = {
    CloudStorageOperations
      .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS), proxyInfo)
      .getContainerReference(containerName)
      .listBlobs(prefix + subDir + "/")
      .toList
      .map(x => {
        val key = x.getUri.toString
        val index = key.lastIndexOf('/')
        subDir + "/" + key.substring(index + 1)
      })
  }
}

case class InternalS3Storage(param: MergedParameters,
                             stageName: String,
                             @transient override val connection: Connection,
                             parallelism: Int =
                               CloudStorageOperations.DEFAULT_PARALLELISM)
    extends CloudStorage {
  val proxyInfo: Option[ProxyInfo] = param.proxyInfo

  override protected def getStageInfo(
    isWrite: Boolean,
    fileName: String = ""
  ): (Map[String, String], List[String]) = {
    @transient val stageManager =
      new SFInternalStage(
        isWrite,
        param,
        stageName,
        connection.asInstanceOf[SnowflakeConnectionV1],
        fileName
      )
    @transient val keyIds = stageManager.getKeyIds

    var storageInfo: Map[String, String] = new HashMap[String, String]

    val (_, queryId, smkId) = if (keyIds.nonEmpty) keyIds.head else ("", "", "")
    storageInfo += StorageInfo.QUERY_ID -> queryId
    storageInfo += StorageInfo.SMK_ID -> smkId
    storageInfo += StorageInfo.MASTER_KEY -> stageManager.masterKey

    val stageLocation = stageManager.stageLocation
    val url = "([^/]+)/?(.*)".r
    val url(bucket, path) = stageLocation
    storageInfo += StorageInfo.BUCKET_NAME -> bucket
    storageInfo += StorageInfo.AWS_ID -> stageManager.awsId.get
    storageInfo += StorageInfo.AWS_KEY -> stageManager.awsKey.get
    stageManager.awsToken.foreach(storageInfo += StorageInfo.AWS_TOKEN -> _)

    val prefix: String =
      if (path.isEmpty) path else if (path.endsWith("/")) path else path + "/"
    storageInfo += StorageInfo.PREFIX -> prefix
    val fileList: List[String] =
      if (isWrite) List() else stageManager.getKeyIds.map(_._1).toList
    (storageInfo, fileList)
  }

  override def download(sc: SparkContext,
                        format: SupportedFormat = SupportedFormat.CSV,
                        compress: Boolean = true,
                        subDir: String = ""): RDD[String] = {
    val (stageInfo, fileList) = getStageInfo(isWrite = false)
    new SnowflakeRDD(
      sc,
      fileList,
      format,
      createDownloadStreamWithRetry(
        _,
        compress,
        stageInfo,
        param.maxRetryCount
      ),
      param.expectedPartitionCount
    )
  }

  override protected def createUploadStream(
    fileName: String,
    dir: Option[String],
    compress: Boolean,
    storageInfo: Map[String, String]
  ): OutputStream = {

    val file: String =
      if (dir.isDefined) s"${dir.get}/$fileName"
      else fileName

    val s3Client: AmazonS3Client =
      CloudStorageOperations.createS3Client(
        storageInfo(StorageInfo.AWS_ID),
        storageInfo(StorageInfo.AWS_KEY),
        storageInfo.get(StorageInfo.AWS_TOKEN),
        parallelism,
        proxyInfo
      )

    val (fileCipher, meta) =
      CloudStorageOperations.getCipherAndS3Metadata(
        storageInfo(StorageInfo.MASTER_KEY),
        storageInfo(StorageInfo.QUERY_ID),
        storageInfo(StorageInfo.SMK_ID)
      )

    if (compress) meta.setContentEncoding("GZIP")

    var outputStream: OutputStream = new OutputStream {
      val buffer: ByteArrayOutputStream = new ByteArrayOutputStream()

      override def write(b: Int): Unit = buffer.write(b)

      override def close(): Unit = {
        buffer.close()
        val resultByteArray = buffer.toByteArray
        // Set up length to avoid S3 client API to raise warning message.
        meta.setContentLength(resultByteArray.length)
        val inputStream: InputStream =
          new ByteArrayInputStream(resultByteArray)
        s3Client.putObject(
          storageInfo(StorageInfo.BUCKET_NAME),
          storageInfo(StorageInfo.PREFIX).concat(file),
          inputStream,
          meta
        )
      }
    }

    outputStream = new CipherOutputStream(outputStream, fileCipher)

    if (compress) new GZIPOutputStream(outputStream)
    else outputStream
  }

  override def deleteFile(fileName: String): Unit = {
    val (storageInfo, _) = getStageInfo(isWrite = true)
    CloudStorageOperations
      .createS3Client(
        storageInfo(StorageInfo.AWS_ID),
        storageInfo(StorageInfo.AWS_KEY),
        storageInfo.get(StorageInfo.AWS_TOKEN),
        parallelism,
        proxyInfo
      )
      .deleteObject(
        storageInfo(StorageInfo.BUCKET_NAME),
        storageInfo(StorageInfo.PREFIX).concat(fileName)
      )
  }

  override def deleteFiles(fileNames: List[String]): Unit = {
    val (storageInfo, _) = getStageInfo(isWrite = true)
    CloudStorageOperations
      .createS3Client(
        storageInfo(StorageInfo.AWS_ID),
        storageInfo(StorageInfo.AWS_KEY),
        storageInfo.get(StorageInfo.AWS_TOKEN),
        parallelism,
        proxyInfo
      )
      .deleteObjects(
        new DeleteObjectsRequest(storageInfo(StorageInfo.BUCKET_NAME))
          .withKeys(fileNames.map(storageInfo(StorageInfo.PREFIX).concat): _*)
      )
  }

  override def fileExists(fileName: String): Boolean = {
    val (storageInfo, _) = getStageInfo(isWrite = false)
    CloudStorageOperations
      .createS3Client(
        storageInfo(StorageInfo.AWS_ID),
        storageInfo(StorageInfo.AWS_KEY),
        storageInfo.get(StorageInfo.AWS_TOKEN),
        parallelism,
        proxyInfo
      )
      .doesObjectExist(
        storageInfo(StorageInfo.BUCKET_NAME),
        storageInfo(StorageInfo.PREFIX).concat(fileName)
      )
  }

  override protected def createDownloadStream(
    fileName: String,
    compress: Boolean,
    storageInfo: Map[String, String]
  ): InputStream = {
    val s3Client: AmazonS3Client =
      CloudStorageOperations
        .createS3Client(
          storageInfo(StorageInfo.AWS_ID),
          storageInfo(StorageInfo.AWS_KEY),
          storageInfo.get(StorageInfo.AWS_TOKEN),
          parallelism,
          proxyInfo
        )
    val dateObject =
      s3Client.getObject(
        storageInfo(StorageInfo.BUCKET_NAME),
        storageInfo(StorageInfo.PREFIX).concat(fileName)
      )

    var inputStream: InputStream = dateObject.getObjectContent
    inputStream = CloudStorageOperations.getDecryptedStream(
      inputStream,
      storageInfo(StorageInfo.MASTER_KEY),
      dateObject.getObjectMetadata.getUserMetadata,
      StageType.S3
    )

    if (compress) new GZIPInputStream(inputStream)
    else inputStream
  }
}

case class ExternalS3Storage(bucketName: String,
                             awsId: String,
                             awsKey: String,
                             proxyInfo: Option[ProxyInfo],
                             maxRetryCount: Int,
                             fileCountPerPartition: Int,
                             awsToken: Option[String] = None,
                             pref: String = "",
                             @transient override val connection: Connection,
                             parallelism: Int =
                               CloudStorageOperations.DEFAULT_PARALLELISM)
    extends CloudStorage {

  lazy val prefix: String =
    if (pref.isEmpty) pref else if (pref.endsWith("/")) pref else pref + "/"

  override protected def createUploadStream(
    fileName: String,
    dir: Option[String],
    compress: Boolean,
    storageInfo: Map[String, String]
  ): OutputStream = {
    val file: String =
      if (dir.isDefined) s"${dir.get}/$fileName"
      else fileName

    val s3Client: AmazonS3Client = CloudStorageOperations.createS3Client(
      awsId,
      awsKey,
      awsToken,
      parallelism,
      proxyInfo
    )

    val meta = new ObjectMetadata()

    if (compress) meta.setContentEncoding("GZIP")

    val outputStream: OutputStream = new OutputStream {
      val buffer: ByteArrayOutputStream = new ByteArrayOutputStream()

      override def write(b: Int): Unit = buffer.write(b)

      override def close(): Unit = {
        buffer.close()
        val resultByteArray = buffer.toByteArray
        // Set up length to avoid S3 client API to raise warning message.
        meta.setContentLength(resultByteArray.length)
        val inputStream: InputStream =
          new ByteArrayInputStream(resultByteArray)
        s3Client.putObject(bucketName, prefix.concat(file), inputStream, meta)
      }
    }

    if (compress) new GZIPOutputStream(outputStream)
    else outputStream
  }

  override def deleteFile(fileName: String): Unit =
    CloudStorageOperations
      .createS3Client(awsId, awsKey, awsToken, parallelism, proxyInfo)
      .deleteObject(bucketName, prefix.concat(fileName))

  override def deleteFiles(fileNames: List[String]): Unit =
    CloudStorageOperations
      .createS3Client(awsId, awsKey, awsToken, parallelism, proxyInfo)
      .deleteObjects(
        new DeleteObjectsRequest(bucketName)
          .withKeys(fileNames.map(prefix.concat): _*)
      )

  override def fileExists(fileName: String): Boolean = {
    val s3Client: AmazonS3Client = CloudStorageOperations.createS3Client(
      awsId,
      awsKey,
      awsToken,
      parallelism,
      proxyInfo
    )
    s3Client.doesObjectExist(bucketName, prefix.concat(fileName))
  }

  override protected def createDownloadStream(
    fileName: String,
    compress: Boolean,
    storageInfo: Map[String, String]
  ): InputStream = {
    val s3Client: AmazonS3Client =
      CloudStorageOperations.createS3Client(
        awsId,
        awsKey,
        awsToken,
        parallelism,
        proxyInfo
      )
    val dataObject = s3Client.getObject(bucketName, prefix.concat(fileName))
    val inputStream: InputStream = dataObject.getObjectContent
    if (compress) new GZIPInputStream(inputStream) else inputStream
  }

  override def download(sc: SparkContext,
                        format: SupportedFormat,
                        compress: Boolean,
                        subDir: String): RDD[String] =
    new SnowflakeRDD(
      sc,
      getFileNames(subDir),
      format,
      createDownloadStreamWithRetry(
        _,
        compress,
        Map.empty[String, String],
        maxRetryCount
      ),
      fileCountPerPartition
    )

  private def getFileNames(subDir: String): List[String] =
    CloudStorageOperations
      .createS3Client(awsId, awsKey, awsToken, parallelism, proxyInfo)
      .listObjects(bucketName, prefix + subDir)
      .getObjectSummaries
      .toList
      .map(x => {
        val key = x.getKey
        val fullName = s"$prefix(.*)".r
        key match {
          case fullName(name) => name
          case _ => throw new Exception("file name is incorrect")
        }
      })

}

// Internal CloudStorage for GCS (Google Cloud Storage).
// NOTE: External storage for GCS is not supported.
case class InternalGcsStorage(param: MergedParameters,
                              stageName: String,
                              @transient override val connection: Connection,
                              @transient stageManager: SFInternalStage)
  extends CloudStorage {
  // proxy related properties
  val proxyProperties: Properties = {
    val proxyProperties = new Properties()
    // Set up proxy info if it is configured.
    param.proxyInfo match {
      case Some(proxyInfoValue) =>
        proxyInfoValue.setProxyForJDBC(proxyProperties)
      case None =>
    }
    proxyProperties
  }

  // Max retry count to upload a file
  val maxRetryCount: Int = param.maxRetryCount

  // Generate file transfer metadata objects for file upload. On GCS,
  // the file transfer metadata is pre-signed URL and related metadata.
  // This function is called on Master node.
  private def generateFileTransferMetadatas(data: RDD[String],
                                            format: SupportedFormat,
                                            compress: Boolean,
                                            dir: String,
                                            fileCount: Int)
  : List[SnowflakeFileTransferMetadata] = {
    CloudStorageOperations.log.info(
      s"""${SnowflakeResultSetRDD.MASTER_LOG_PREFIX}:
         | Begin to retrieve pre-signed URL for
         | ${data.getNumPartitions} files by calling
         | PUT command for each file.
         |""".stripMargin.filter(_ >= ' '))

    val connectionV1 = connection.asInstanceOf[SnowflakeConnectionV1]
    var result = new ListBuffer[SnowflakeFileTransferMetadata]()

    val startTime = System.currentTimeMillis()
    val printStep = 1000
    // Loop to execute one PUT command for one pre-signed URL.
    // This is because GCS doesn't support to generate pre-signed for
    // prefix(path). If GCS supports it, this part can be enhanced.
    for (index <- 0 until fileCount) {
      val fileName = getFileName(index, format, compress)
      val dummyDir = s"/dummy_put_${index}_of_$fileCount"
      val putCommand = s"put file://$dummyDir/$fileName @$stageName/$dir"

      // Retrieve pre-signed URLs and put them in result
      new SnowflakeFileTransferAgent(
        putCommand,
        connectionV1.getSfSession,
        new SFStatement(connectionV1.getSfSession)
      ).getFileTransferMetadatas
        .map(oneMetadata => result += oneMetadata)

      // Output time for retrieving every 1000 pre-signed URLs
      // to indicate the progress for big data.
      if ((index % printStep) == (printStep - 1)) {
        val endTime = System.currentTimeMillis()
        CloudStorageOperations.log.info(
          s"""${SnowflakeResultSetRDD.MASTER_LOG_PREFIX}:
             | Time to retrieve pre-signed URL for
             | ${index + 1} files is
             | ${(endTime - startTime) / 1000.0} seconds.
             |""".stripMargin.filter(_ >= ' '))
      }
    }

    // Output the total time for retrieving pre-signed URLs
    val endTime = System.currentTimeMillis()
    CloudStorageOperations.log.info(
      s"""${SnowflakeResultSetRDD.MASTER_LOG_PREFIX}:
         | Time to retrieve pre-signed URL for
         | ${data.getNumPartitions} files is
         | ${(endTime - startTime) / 1000.0} seconds.
         |""".stripMargin.filter(_ >= ' '))

    result.toList
  }

  // Retrieve data for one partition and upload the result data to stage.
  // The upload is done by JDBC internal API without JDBC connection.
  // This function is called on worker node.
  private def uploadPartition(rows: Iterator[String],
                              format: SupportedFormat,
                              compress: Boolean,
                              directory: String,
                              partitionID: Int,
                              metadata: SnowflakeFileTransferMetadata)
  : SingleElementIterator = {
    val fileName = getFileName(partitionID, format, compress)

    CloudStorageOperations.log.info(
      s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}:
         | Start writing partition ID:$partitionID as $fileName
         |""".stripMargin.filter(_ >= ' '))

    // Retrieve the data for uploading
    val startTime = System.currentTimeMillis()
    val outputStream = new ByteArrayOutputStream(1024 * 1024)
    var rowCount: Long = 0
    while (rows.hasNext) {
      outputStream.write(rows.next.getBytes("UTF-8"))
      outputStream.write('\n')
      rowCount += 1
    }
    val data = outputStream.toByteArray

    if (data.nonEmpty) {
      // Upload the file with retry and backoff.
      // default maxRetryCount is 10 which is configurable.
      var retryCount = 0
      var error: Option[Throwable] = None
      var uploadDone = false
      do {
        try {
          val startUploadTime = System.currentTimeMillis()
          val inStream = new ByteArrayInputStream(data)
          SnowflakeFileTransferAgent.uploadWithoutConnection(
            SnowflakeFileTransferConfig.Builder.newInstance()
              .setSnowflakeFileTransferMetadata(metadata)
              .setUploadStream(inStream)
              .setRequireCompress(compress)
              .setOcspMode(OCSPMode.FAIL_OPEN)
              .setProxyProperties(proxyProperties)
              .build())
          outputStream.close()

          TestHook.raiseExceptionIfTestFlagEnabled(
            TestHookFlag.TH_GCS_UPLOAD_RAISE_EXCEPTION,
            "Negative test to raise error when uploading data to GCS"
          )

          val endTime = System.currentTimeMillis()
          CloudStorageOperations.log.info(
            s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}:
               | Finish writing partition ID:$partitionID:
               | write row count is $rowCount.
               | Uncompressed data size is ${Utils.getSizeString(data.size)}.
               | Total process time is
               | ${(endTime - startTime) / 1000.0}s including
               | conversion_time=${(startUploadTime - startTime) / 1000.0}s
               | and upload_time=${(endTime - startUploadTime) / 1000.0}s.
               |""".stripMargin.filter(_ >= ' '))

          // succeed upload, set done to break the loop
          uploadDone = true
          error = None
        } catch {
          // Hit exception when uploading the file, sleep some time and retry.
          case e: Throwable => {
            error = Some(e)
            val errmsg = e.getMessage
            CloudStorageOperations.log.info(
              s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: hit upload error:
                 | retryCount=$retryCount: fileName=$fileName,
                 | maxRetryCount=$maxRetryCount error details: [ $errmsg ]
                 |""".stripMargin.filter(_ >= ' ')
            )
          }
            retryCount = retryCount + 1
            // sleep some time and retry
            Thread.sleep(RETRY_SLEEP_TIME_UNIT_IN_MS * retryCount)
        }
      } while (retryCount < maxRetryCount && !uploadDone)

      // Fail to upload data after retry
      if (!uploadDone) {
        val errorMessage = error.get.getMessage
        CloudStorageOperations.log.info(
          s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: last error message
             | after retry $retryCount times is [ $errorMessage ]
             |""".stripMargin.filter(_ >= ' ')
        )
        throw error.get
      }
    } else {
      val endTime = System.currentTimeMillis()
      CloudStorageOperations.log.info(
        s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}:
           | Finish writing partition ID:$partitionID:
           | upload is skipped because partition is empty.
           | Total process time is
           | ${(endTime - startTime) / 1000.0}s.
           |""".stripMargin.filter(_ >= ' '))
    }

    new SingleElementIterator(s"$directory/$fileName")
  }

  // The RDD upload on GCS is different to AWS and Azure
  // so override it separately.
  override def upload(data: RDD[String],
                      format: SupportedFormat = SupportedFormat.CSV,
                      dir: Option[String],
                      compress: Boolean = true): List[String] = {

    val directory: String =
      dir match {
        case Some(str: String) => str
        case None => Random.alphanumeric take 10 mkString ""
      }

    // Generate one pre-signed for each partition.
    val metadatas = generateFileTransferMetadatas(
      data, format, compress, directory, data.getNumPartitions)

    val oneMetadataPerFile = metadatas.head.isForOneFile

    // Some explain for newbies on spark connector:
    // Bellow code is executed in distributed by spark FRAMEWORK
    // 1. The master node executes "data.mapPartitionsWithIndex()"
    // 2. Code snippet for CASE clause is executed by distributed worker nodes
    val files = data.mapPartitionsWithIndex {
      case (index, rows) =>
        ///////////////////////////////////////////////////////////////////////
        // Begin code snippet to executed on worker
        ///////////////////////////////////////////////////////////////////////

        // Get file transfer metadata object
        val metadata = if (oneMetadataPerFile) {
          metadatas(index)
        } else {
          metadatas.head
        }
        // Convert and upload the partition with the file transfer metadata
        uploadPartition(rows, format, compress, directory, index, metadata)

      ///////////////////////////////////////////////////////////////////////
      // End code snippet to executed on worker
      ///////////////////////////////////////////////////////////////////////
    }

    files.collect().toList
  }

  // GCS doesn't support streaming yet
  def download(sc: SparkContext,
               format: SupportedFormat = SupportedFormat.CSV,
               compress: Boolean = true,
               subDir: String = ""): RDD[String] = {
    throw new SnowflakeConnectorFeatureNotSupportException(
      "Internal error: download() should not be called for GCS")
  }

  // On GCS, the JDBC driver uploads the data directly,
  // So this function is not needed anymore.
  override protected def createUploadStream(
                                             fileName: String,
                                             dir: Option[String],
                                             compress: Boolean,
                                             storageInfo: Map[String, String]
                                           ): OutputStream = {
    throw new SnowflakeConnectorFeatureNotSupportException(
      "Internal error: createUploadStream() should not be called for GCS")
  }

  // GCS doesn't support streaming yet
  override def deleteFile(fileName: String): Unit = {
    throw new SnowflakeConnectorFeatureNotSupportException(
      "Internal error: deleteFile() should not be called for GCS")
  }

  // GCS doesn't support streaming yet
  override def deleteFiles(fileNames: List[String]): Unit = {
    throw new SnowflakeConnectorFeatureNotSupportException(
      "Internal error: deleteFiles() should not be called for GCS")
  }

  // GCS doesn't support streaming yet
  override def fileExists(fileName: String): Boolean = {
    throw new SnowflakeConnectorFeatureNotSupportException(
      "Internal error: fileExists() should not be called for GCS")
  }

  // GCS doesn't support reading from snowflake with COPY UNLOAD
  override protected def createDownloadStream(
                                               fileName: String,
                                               compress: Boolean,
                                               storageInfo: Map[String, String]
                                             ): InputStream = {
    throw new SnowflakeConnectorFeatureNotSupportException(
      "Internal error: createDownloadStream() should not be called for GCS")
  }
}

