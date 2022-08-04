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
  AWSStaticCredentialsProvider,
  BasicAWSCredentials,
  BasicSessionCredentials
}
import net.snowflake.client.jdbc.internal.amazonaws.client.builder.AwsClientBuilder
import net.snowflake.client.jdbc.internal.amazonaws.retry.{
  PredefinedRetryPolicies,
  RetryPolicy
}
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.{
  AmazonS3,
  AmazonS3Client,
  AmazonS3ClientBuilder
}
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model._
import net.snowflake.client.jdbc.internal.amazonaws.util.Base64
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{
  JsonNode,
  ObjectMapper
}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.{
  AccessCondition,
  OperationContext,
  StorageCredentialsAnonymous,
  StorageCredentialsSharedAccessSignature
}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.{
  BlobRequestOptions,
  CloudBlobClient,
  CloudBlockBlob
}
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState
import net.snowflake.spark.snowflake._
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import net.snowflake.spark.snowflake.test.{TestHook, TestHookFlag}
import org.apache.commons.io.IOUtils
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Random
import scala.collection.immutable.HashMap
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object CloudStorageOperations {
  private[io] val DEFAULT_PARALLELISM = 10
  private[io] val S3_MAX_RETRIES = 6
  private[io] val S3_MAX_TIMEOUT_MS = 30 * 1000
  private[io] val AES = "AES"
  private[io] val AMZ_KEY = "x-amz-key"
  private[io] val AMZ_IV = "x-amz-iv"
  private[io] val DATA_CIPHER = "AES/CBC/PKCS5Padding"
  private[io] val KEY_CIPHER = "AES/ECB/PKCS5Padding"
  private[io] val AMZ_MATDESC = "x-amz-matdesc"
  private[io] val AZ_ENCRYPTIONDATA = "encryptiondata"
  private[io] val AZ_IV = "ContentEncryptionIV"
  private[io] val AZ_KEY_WRAP = "WrappedContentKey"
  private[io] val AZ_KEY = "EncryptedKey"
  private[io] val AZ_MATDESC = "matdesc"

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
            param.sfURL,
            param.useExponentialBackoff,
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
            param.sfURL,
            param.useExponentialBackoff,
            param.expectedPartitionCount,
            pref = prefix,
            connection = conn,
            // For S3 external stage, it doesn't use region name in URL
            useRegionUrl = None,
            regionName = None
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
  )(implicit storage: CloudStorage): List[String] = {
    storage.upload(data, format, dir, compress).map(_.fileName)
  }

  def deleteFiles(files: List[String])(implicit storage: CloudStorage,
                                       connection: Connection): Unit =
    storage.deleteFiles(files)

  private[io] def createS3Client(
    awsId: String,
    awsKey: String,
    awsToken: Option[String],
    parallelism: Int,
    proxyInfo: Option[ProxyInfo],
    useRegionUrl: Option[String],
    regionName: Option[String]
  ): AmazonS3 = {
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

    if (useRegionUrl.isEmpty) {
      new AmazonS3Client(awsCredentials, clientConfig)
    } else {
      val s3ClientBuilder = AmazonS3ClientBuilder.standard
        .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
        .withClientConfiguration(clientConfig)
      // Use URL with region name with the same logic as JDBC,
      // refer to SnowflakeS3Client.setupSnowflakeS3Client() for details.
      if (useRegionUrl.get.toBoolean) {
        s3ClientBuilder.withEndpointConfiguration(
          new AwsClientBuilder.EndpointConfiguration(
            "s3." + regionName.get + ".amazonaws.com", regionName.get))
      } else {
        s3ClientBuilder.withRegion(regionName.get)
      }

      s3ClientBuilder.build()
    }
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

class FileUploadResult(val fileName: String,
                       val fileSize: Long,
                       val rowCount: Long) extends Serializable {
}

private[io] class SingleElementIterator(fileUploadResult: FileUploadResult)
  extends Iterator[FileUploadResult] {
  private var data: Option[FileUploadResult] = Some(fileUploadResult)

  override def hasNext: Boolean = data.isDefined

  override def next(): FileUploadResult = {
    val result = data.get
    data = None
    result
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
  @inline val AWS_REGION = "awsRegion"
  @inline val AWS_USE_REGION_URL = "awsUseRegionUrl"
}

sealed trait CloudStorage {
  protected val RETRY_SLEEP_TIME_UNIT_IN_MS: Int = 1500
  protected val MAX_SLEEP_TIME_IN_MS: Int = 3 * 60 * 1000
  private var processedFileCount = 0
  protected val connection: Connection
  protected val maxRetryCount: Int
  protected val proxyInfo: Option[ProxyInfo]
  protected val sfURL: String
  protected val useExponentialBackoff: Boolean

  // The first 10 sleep time in second will be like
  // 3, 6, 12, 24, 48, 96, 192, 300, 300, 300, etc
  protected def retrySleepTimeInMS(retry: Int): Int = {
    var expectedTime =
      RETRY_SLEEP_TIME_UNIT_IN_MS * Math.pow(2, retry).toInt
    // One sleep should be less than 3 minutes
    expectedTime = Math.min(expectedTime, MAX_SLEEP_TIME_IN_MS)
    // jitter factor is 0.5
    expectedTime = expectedTime / 2 + Random.nextInt(expectedTime / 2)
    expectedTime
  }

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
             compress: Boolean = true): List[FileUploadResult] =
    uploadRDD(data, format, dir, compress, getStageInfo(isWrite = true)._1)

  private[io] def checkUploadMetadata(storageInfo: Option[Map[String, String]],
                                      fileTransferMetadata: Option[SnowflakeFileTransferMetadata]
                                     ): Unit = {
    if ((storageInfo.isEmpty && fileTransferMetadata.isEmpty)
      || (storageInfo.isDefined && fileTransferMetadata.isDefined)) {
      val errorMessage =
        s"""Hit internal error: Either storageInfo or fileTransferMetadata
           | must be set. storageInfo=${storageInfo.isDefined}
           | fileTransferMetadata=${fileTransferMetadata.isDefined}
           |""".stripMargin
      CloudStorageOperations.log.info(
        s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: $errorMessage
           |""".stripMargin.filter(_ >= ' '))
      throw new SnowflakeConnectorException(errorMessage)
    }
  }

  // Retrieve data for one partition and upload the result data to stage.
  // This function is called on worker node.
  // When spark task fails, this task will be rescheduled by spark until
  // the max attempt number is arrived. So we can leverage the spark retry
  // instead of implementing retry in spark connector. Spark sleeps 3 seconds
  // by default before reschedule. To handle cloud storage service throttling
  // issue, it is necessary to use exponential sleep time
  // (but spark doesn't support it). So, we introduce extra exponential
  // sleep time based on the task's attempt number.
  protected def uploadPartition(rows: Iterator[String],
                                format: SupportedFormat,
                                compress: Boolean,
                                directory: String,
                                partitionID: Int,
                                storageInfo: Option[Map[String, String]],
                                fileTransferMetadata: Option[SnowflakeFileTransferMetadata]
                               )
  : SingleElementIterator = {
    val fileName = getFileName(partitionID, format, compress)

    // Either StorageInfo or fileTransferMetadata must be set.
    checkUploadMetadata(storageInfo, fileTransferMetadata)

    try {
      // Read data and upload to cloud storage
      doUploadPartition(rows, format, compress, directory, partitionID,
                        storageInfo, fileTransferMetadata)
    } catch {
      // Hit exception when uploading the file
      case th: Throwable => {
        val stringWriter = new StringWriter
        th.printStackTrace(new PrintWriter(stringWriter))
        val errmsg =
          s"""${th.getClass.toString}, ${th.getMessage},
             | stacktrace: ${stringWriter.toString}""".stripMargin

        // Send OOB telemetry message if uploading failure happens
        val attemptNumber = TaskContext.get().attemptNumber()
        SnowflakeTelemetry.sendTelemetryOOB(
          sfURL,
          this.getClass.getSimpleName,
          TelemetryConstValues.OPERATION_WRITE,
          attemptNumber,
          maxRetryCount,
          false,
          proxyInfo.isDefined,
          None,
          Some(th))

        // Sleep exponential time based on the attempt number.
        if (useExponentialBackoff) {
          val sleepTime = retrySleepTimeInMS(attemptNumber + 1)
          CloudStorageOperations.log.info(
            s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: hit upload error:
               | partition ID:$partitionID $fileName
               | attemptNumber=$attemptNumber
               | backoffTime=${Utils.getTimeString(sleepTime)}
               | error details: [ $errmsg ]
               |""".stripMargin.filter(_ >= ' ')
          )
          Thread.sleep(sleepTime)
        } else {
          CloudStorageOperations.log.warn(
            s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: hit upload error:
               | partition ID:$partitionID $fileName
               | attemptNumber=$attemptNumber
               | Skip exponential backoff sleep because
               | ${Parameters.PARAM_USE_EXPONENTIAL_BACKOFF} is 'off'.
               | Please enable it if necessary, for example, cloud service
               | throttling issues happen.
               |""".stripMargin.filter(_ >= ' ')
          )
        }
        // re-throw the exception
        throw th
      }
    }
  }

  // Read data and upload to cloud storage
  private def doUploadPartition(rows: Iterator[String],
                                format: SupportedFormat,
                                compress: Boolean,
                                directory: String,
                                partitionID: Int,
                                storageInfo: Option[Map[String, String]],
                                fileTransferMetadata: Option[SnowflakeFileTransferMetadata]
                               )
  : SingleElementIterator = {
    val fileName = getFileName(partitionID, format, compress)

    CloudStorageOperations.log.info(
      s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}:
         | Start writing partition ID:$partitionID as $fileName
         | TaskInfo: ${SnowflakeTelemetry.getTaskInfo().toPrettyString}
         |""".stripMargin.filter(_ >= ' '))

    // Read data and upload to cloud storage
    var rowCount: Long = 0
    var dataSize: Long = 0
    var processTimeInfo = ""
    val startTime = System.currentTimeMillis()
    if (storageInfo.isDefined) {
      // For AWS and Azure, the rows are written to OutputStream as they are read.
      var uploadStream: Option[OutputStream] = None
      while (rows.hasNext) {
        // Defer to create the upload stream to avoid empty files.
        if (uploadStream.isEmpty) {
          uploadStream = Some(createUploadStream(
            fileName, Some(directory), compress, storageInfo.get))
        }
        val oneRow = rows.next.getBytes("UTF-8")
        uploadStream.get.write(oneRow)
        uploadStream.get.write('\n')
        rowCount += 1
        dataSize += (oneRow.size + 1)
      }
      if (uploadStream.isDefined) {
        uploadStream.get.close()
      }

      val endTime = System.currentTimeMillis()
      processTimeInfo =
        s"""read_and_upload_time:
           | ${Utils.getTimeString(endTime - startTime)}
           |""".stripMargin.filter(_ >= ' ')
    }
    // For GCP, the rows are cached and then uploaded.
    else if (fileTransferMetadata.isDefined) {
      // cache the data in buffer
      val outputStream = new ByteArrayOutputStream(4 * 1024 * 1024)
      while (rows.hasNext) {
        outputStream.write(rows.next.getBytes("UTF-8"))
        outputStream.write('\n')
        rowCount += 1
      }
      val data = outputStream.toByteArray
      dataSize = data.size
      outputStream.close()

      // Set up proxy info if it is configured.
      val proxyProperties = new Properties()
      proxyInfo match {
        case Some(proxyInfoValue) =>
          proxyInfoValue.setProxyForJDBC(proxyProperties)
        case None =>
      }

      // Upload data with FileTransferMetadata
      val startUploadTime = System.currentTimeMillis()
      val inStream = new ByteArrayInputStream(data)
      SnowflakeFileTransferAgent.uploadWithoutConnection(
        SnowflakeFileTransferConfig.Builder.newInstance()
          .setSnowflakeFileTransferMetadata(fileTransferMetadata.get)
          .setUploadStream(inStream)
          .setRequireCompress(compress)
          .setOcspMode(OCSPMode.FAIL_OPEN)
          .setProxyProperties(proxyProperties)
          .build())

      val endTime = System.currentTimeMillis()
      processTimeInfo =
        s"""read_and_upload_time:
           | ${Utils.getTimeString(endTime - startTime)}
           | read_time: ${Utils.getTimeString(startUploadTime - startTime)}
           | upload_time: ${Utils.getTimeString(endTime - startUploadTime)}
           |""".stripMargin.filter(_ >= ' ')
    }

    // When attempt number is smaller than 2, throw exception
    if (TaskContext.get().attemptNumber() < 2) {
      TestHook.raiseExceptionIfTestFlagEnabled(
        TestHookFlag.TH_GCS_UPLOAD_RAISE_EXCEPTION,
        "Negative test to raise error when uploading data for the first two attempts"
      )
    }

    CloudStorageOperations.log.info(
      s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}:
         | Finish writing partition ID:$partitionID $fileName
         | write row count is $rowCount.
         | Uncompressed data size is ${Utils.getSizeString(dataSize)}.
         | $processTimeInfo
         |""".stripMargin.filter(_ >= ' '))

    new SingleElementIterator(new FileUploadResult(s"$directory/$fileName", dataSize, rowCount))
  }

  protected def uploadRDD(data: RDD[String],
                          format: SupportedFormat = SupportedFormat.CSV,
                          dir: Option[String],
                          compress: Boolean = true,
                          storageInfo: Map[String, String]): List[FileUploadResult] = {

    val directory: String =
      dir match {
        case Some(str: String) => str
        case None => Random.alphanumeric take 10 mkString ""
      }

    CloudStorageOperations.log.info(
      s"""${SnowflakeResultSetRDD.MASTER_LOG_PREFIX}:
         | Begin to process and upload data for ${data.getNumPartitions}
         | partitions: directory=$directory ${format.toString} $compress
         |""".stripMargin.filter(_ >= ' '))

    // Some explain for newbies on spark connector:
    // Bellow code is executed in distributed by spark FRAMEWORK
    // 1. The master node executes "data.mapPartitionsWithIndex()"
    // 2. Code snippet for CASE clause is executed by distributed worker nodes
    val fileUploadResults = data.mapPartitionsWithIndex {
      case (index, rows) =>
        ///////////////////////////////////////////////////////////////////////
        // Begin code snippet to be executed on worker
        ///////////////////////////////////////////////////////////////////////

        // Log system configuration if not yet.
        SparkConnectorContext.recordConfig()

        // Convert and upload the partition with the StorageInfo
        uploadPartition(rows, format, compress, directory, index, Some(storageInfo), None)

        ///////////////////////////////////////////////////////////////////////
        // End code snippet to be executed on worker
        ///////////////////////////////////////////////////////////////////////
    }

    fileUploadResults.collect().toList
  }

  // Implement retry logic when download fails and finish the file download.
  def createDownloadStreamWithRetry(fileName: String,
                                    compress: Boolean,
                                    storageInfo: Map[String, String],
                                    maxRetryCount: Int): InputStream = {
    // download the file with retry and backoff and then consume.
    var retryCount = 0
    var throwable: Option[Throwable] = None
    var downloadDone = false
    var inputStream: InputStream = null

    do {
      try {
        val startTime = System.currentTimeMillis()
        // Create original download stream
        inputStream = createDownloadStream(fileName, compress, storageInfo)

        // If max retry count is greater than 1, enable the file download.
        // This provides a way to disable the file download if necessary.
        val download = maxRetryCount > 1
        if (download) {
          // Download, decrypt and decompress the data
          val cachedData = IOUtils.toByteArray(inputStream)

          // Generate inputStream for the cached data buffer.
          inputStream = new ByteArrayInputStream(cachedData)
          val endTime = System.currentTimeMillis()
          val downloadTime = Utils.getTimeString(endTime - startTime)

          CloudStorageOperations.log.info(
            s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: download
               | successful: fileID=$processedFileCount
               | fileName=$fileName downloadTime=$downloadTime
               | dataSize=${Utils.getSizeString(cachedData.size)}
               |""".stripMargin.filter(_ >= ' ')
          )
        } else {
          CloudStorageOperations.log.info(
            s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: DO NOT download
               | the file completely: fileID=$processedFileCount
               | fileName=$fileName
               |""".stripMargin.filter(_ >= ' ')
          )
        }

        // succeed download
        downloadDone = true
        processedFileCount += 1
      } catch {
        // Find problem to download the file, sleep some time and retry.
        case th: Throwable => {
          throwable = Some(th)
          retryCount = retryCount + 1
          val sleepTime = retrySleepTimeInMS(retryCount)
          val stringWriter = new StringWriter
          th.printStackTrace(new PrintWriter(stringWriter))
          val errmsg = s"${th.getMessage}, stacktrace: ${stringWriter.toString}"

          CloudStorageOperations.log.info(
            s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: hit download error:
               | retryCount=$retryCount fileName=$fileName
               | backoffTime=${Utils.getTimeString(sleepTime)}
               | maxRetryCount=$maxRetryCount error details: [ $errmsg ]
               |""".stripMargin.filter(_ >= ' ')
          )
          // sleep some time and retry
          Thread.sleep(sleepTime)
        }
      }
    } while (retryCount < maxRetryCount && !downloadDone)

    // Send OOB telemetry message if downloading failure happens
    if (retryCount > 0) {
      SnowflakeTelemetry.sendTelemetryOOB(
        sfURL,
        this.getClass.getSimpleName,
        TelemetryConstValues.OPERATION_READ,
        retryCount,
        maxRetryCount,
        downloadDone,
        proxyInfo.isDefined,
        None,
        throwable)
    }

    if (downloadDone) {
      inputStream
    } else {
      // Fail to download data after retry
      val errorMessage = throwable.get.getMessage
      CloudStorageOperations.log.info(
        s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: last error message
           | after retry $retryCount times is [ $errorMessage ]
           |""".stripMargin.filter(_ >= ' ')
      )
      throw throwable.get
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

  def deleteFiles(fileNames: List[String]): Unit

  def fileExists(fileName: String): Boolean
}

case class InternalAzureStorage(param: MergedParameters,
                                stageName: String,
                                @transient override val connection: Connection)
    extends CloudStorage {

  override val maxRetryCount = param.maxRetryCount
  override val proxyInfo: Option[ProxyInfo] = param.proxyInfo
  override val sfURL = param.sfURL
  override val useExponentialBackoff = param.useExponentialBackoff

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

      val opContext = new OperationContext
      try {
        val azureOutput = blob.openOutputStream(
          null.asInstanceOf[AccessCondition],
          null.asInstanceOf[BlobRequestOptions],
          opContext)

        val requestID = opContext.getClientRequestID
        StorageUtils.logAzureInfo(true, "internal", file,
          requestID, blob.getContainer.getName)

        // Inject Exception for test
        TestHook.raiseExceptionIfTestFlagEnabled(
          TestHookFlag.TH_FAIL_CREATE_UPLOAD_STREAM,
          "Negative test to raise error when creating upload stream"
        )
        new CipherOutputStream(azureOutput, cipher)
      } catch {
        case th: Throwable =>
          throw StorageUtils.logAzureThrowable(true, "internal", file, blob, opContext, th)
      }
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

    // Set up download stream and retrieve requestID
    var opContext = new OperationContext
    try {
      blob.download(azureStorage,
        null.asInstanceOf[AccessCondition],
        null.asInstanceOf[BlobRequestOptions],
        opContext)

      // Set up download attribute and retrieve requestID
      opContext = new OperationContext
      blob.downloadAttributes(
        null.asInstanceOf[AccessCondition],
        null.asInstanceOf[BlobRequestOptions],
        opContext)

      val requestID = opContext.getClientRequestID
      StorageUtils.logAzureInfo(false, "internal", fileName,
        requestID, blob.getContainer.getName)

      // Inject test
      TestHook.raiseExceptionIfTestFlagEnabled(
        TestHookFlag.TH_FAIL_CREATE_DOWNLOAD_STREAM,
        "Negative test to raise error when creating a download stream"
      )
    } catch {
      case th: Throwable =>
        throw StorageUtils.logAzureThrowable(false, "internal", fileName, blob, opContext, th)
    }

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
                                override val proxyInfo: Option[ProxyInfo],
                                override val maxRetryCount: Int,
                                override val sfURL: String,
                                override val useExponentialBackoff: Boolean,
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

    val opContext = new OperationContext
    try {
      val azureOutput = blob.openOutputStream(
        null.asInstanceOf[AccessCondition],
        null.asInstanceOf[BlobRequestOptions],
        opContext)
      StorageUtils.logAzureInfo(true, "external", file, opContext.getClientRequestID,
        blob.getContainer.getName)

      if (compress) {
        new GZIPOutputStream(azureOutput)
      } else {
        azureOutput
      }
    } catch {
      case th: Throwable =>
        throw StorageUtils.logAzureThrowable(true, "external", fileName, blob, opContext, th)
    }
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
    // Set up download stream and retrieve requestID
    val opContext = new OperationContext
    try {
      blob.download(azureStream,
        null.asInstanceOf[AccessCondition],
        null.asInstanceOf[BlobRequestOptions],
        opContext)
      StorageUtils.logAzureInfo(false, "external", fileName,
        opContext.getClientRequestID, blob.getContainer.getName)
    } catch {
      case th: Throwable =>
        throw StorageUtils.logAzureThrowable(false, "external", fileName, blob, opContext, th)
    }

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
      .asScala
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
  override val maxRetryCount = param.maxRetryCount
  override val proxyInfo: Option[ProxyInfo] = param.proxyInfo
  override val sfURL = param.sfURL
  override val useExponentialBackoff = param.useExponentialBackoff

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
    if (param.useAWSRegionURL) {
      storageInfo += StorageInfo.AWS_REGION -> stageManager.getRegion
      storageInfo += StorageInfo.AWS_USE_REGION_URL -> stageManager.useS3RegionalUrl.toString
    }

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

    val s3Client: AmazonS3 =
      CloudStorageOperations.createS3Client(
        storageInfo(StorageInfo.AWS_ID),
        storageInfo(StorageInfo.AWS_KEY),
        storageInfo.get(StorageInfo.AWS_TOKEN),
        parallelism,
        proxyInfo,
        storageInfo.get(StorageInfo.AWS_USE_REGION_URL),
        storageInfo.get(StorageInfo.AWS_REGION)
      )

    val (fileCipher, meta) =
      CloudStorageOperations.getCipherAndS3Metadata(
        storageInfo(StorageInfo.MASTER_KEY),
        storageInfo(StorageInfo.QUERY_ID),
        storageInfo(StorageInfo.SMK_ID)
      )

    if (compress) meta.setContentEncoding("GZIP")

    // The AWS multiple parts upload API will be used is
    // data size is bigger than the upload chunk size.
    var outputStream: OutputStream =
      if (param.useAwsMultiplePartsUpload) {
        new S3UploadOutputStream(
          s3Client,
          meta,
          storageInfo,
          param.uploadChunkSize,
          file)
      } else {
        new OutputStream {
          val buffer: ByteArrayOutputStream = new ByteArrayOutputStream()

          override def write(b: Int): Unit = buffer.write(b)

          override def close(): Unit = {
            val start = System.currentTimeMillis()
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
            val usedTime = System.currentTimeMillis() - start
            CloudStorageOperations.log.info(
              s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}:
                 | Finish uploading file $file without AWS multiple parts API
                 | uploadTime=${Utils.getTimeString(usedTime)}
                 | compressedSize=${Utils.getSizeString(resultByteArray.length)}
                 |""".stripMargin.filter(_ >= ' '))
          }
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
        proxyInfo,
        storageInfo.get(StorageInfo.AWS_USE_REGION_URL),
        storageInfo.get(StorageInfo.AWS_REGION)
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
        proxyInfo,
        storageInfo.get(StorageInfo.AWS_USE_REGION_URL),
        storageInfo.get(StorageInfo.AWS_REGION)
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
        proxyInfo,
        storageInfo.get(StorageInfo.AWS_USE_REGION_URL),
        storageInfo.get(StorageInfo.AWS_REGION)
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
    val s3Client: AmazonS3 =
      CloudStorageOperations
        .createS3Client(
          storageInfo(StorageInfo.AWS_ID),
          storageInfo(StorageInfo.AWS_KEY),
          storageInfo.get(StorageInfo.AWS_TOKEN),
          parallelism,
          proxyInfo,
          storageInfo.get(StorageInfo.AWS_USE_REGION_URL),
          storageInfo.get(StorageInfo.AWS_REGION)
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
                             override val proxyInfo: Option[ProxyInfo],
                             override val maxRetryCount: Int,
                             override val sfURL: String,
                             override val useExponentialBackoff: Boolean,
                             fileCountPerPartition: Int,
                             awsToken: Option[String] = None,
                             pref: String = "",
                             @transient override val connection: Connection,
                             parallelism: Int =
                               CloudStorageOperations.DEFAULT_PARALLELISM,
                             useRegionUrl: Option[String],
                             regionName: Option[String])
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

    val s3Client: AmazonS3 = CloudStorageOperations.createS3Client(
      awsId,
      awsKey,
      awsToken,
      parallelism,
      proxyInfo,
      useRegionUrl,
      regionName
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
      .createS3Client(awsId, awsKey, awsToken, parallelism, proxyInfo, useRegionUrl, regionName)
      .deleteObject(bucketName, prefix.concat(fileName))

  override def deleteFiles(fileNames: List[String]): Unit =
    CloudStorageOperations
      .createS3Client(awsId, awsKey, awsToken, parallelism, proxyInfo, useRegionUrl, regionName)
      .deleteObjects(
        new DeleteObjectsRequest(bucketName)
          .withKeys(fileNames.map(prefix.concat): _*)
      )

  override def fileExists(fileName: String): Boolean = {
    val s3Client: AmazonS3 = CloudStorageOperations.createS3Client(
      awsId,
      awsKey,
      awsToken,
      parallelism,
      proxyInfo,
      useRegionUrl,
      regionName
    )
    s3Client.doesObjectExist(bucketName, prefix.concat(fileName))
  }

  override protected def createDownloadStream(
    fileName: String,
    compress: Boolean,
    storageInfo: Map[String, String]
  ): InputStream = {
    val s3Client: AmazonS3 =
      CloudStorageOperations.createS3Client(
        awsId,
        awsKey,
        awsToken,
        parallelism,
        proxyInfo,
        useRegionUrl,
        regionName
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
      .createS3Client(awsId, awsKey, awsToken, parallelism, proxyInfo, useRegionUrl, regionName)
      .listObjects(bucketName, prefix + subDir)
      .getObjectSummaries
      .asScala
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

  override val proxyInfo: Option[ProxyInfo] = param.proxyInfo
  // Max retry count to upload a file
  override val maxRetryCount: Int = param.maxRetryCount
  override val sfURL = param.sfURL
  override val useExponentialBackoff = param.useExponentialBackoff

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
        .asScala
        .map(oneMetadata => result += oneMetadata)

      // Output time for retrieving every 1000 pre-signed URLs
      // to indicate the progress for big data.
      if ((index % printStep) == (printStep - 1)) {
        StorageUtils.logPresignedUrlGenerateProgress(data.getNumPartitions, index + 1, startTime)
      }
    }

    // Output the total time for retrieving pre-signed URLs
    StorageUtils.logPresignedUrlGenerateProgress(data.getNumPartitions,
      data.getNumPartitions, startTime)

    result.toList
  }

  // The RDD upload on GCS is different to AWS and Azure
  // so override it separately.
  override def upload(data: RDD[String],
                      format: SupportedFormat = SupportedFormat.CSV,
                      dir: Option[String],
                      compress: Boolean = true): List[FileUploadResult] = {

    val directory: String =
      dir match {
        case Some(str: String) => str
        case None => Random.alphanumeric take 10 mkString ""
      }

    val startTime = System.currentTimeMillis()
    CloudStorageOperations.log.info(
      s"""${SnowflakeResultSetRDD.MASTER_LOG_PREFIX}:
         | Begin to process and upload data for ${data.getNumPartitions}
         | partitions: directory=$directory ${format.toString} $compress
         |""".stripMargin.filter(_ >= ' '))

    // Generate one pre-signed for each partition.
    val metadatas = generateFileTransferMetadatas(
      data, format, compress, directory, data.getNumPartitions)

    // If the partition count is 0, no metadata is created.
    val oneMetadataPerFile = metadatas.nonEmpty && metadatas.head.isForOneFile

    // Some explain for newbies on spark connector:
    // Bellow code is executed in distributed by spark FRAMEWORK
    // 1. The master node executes "data.mapPartitionsWithIndex()"
    // 2. Code snippet for CASE clause is executed by distributed worker nodes
    val fileUploadResults = data.mapPartitionsWithIndex {
      case (index, rows) =>
        ///////////////////////////////////////////////////////////////////////
        // Begin code snippet to executed on worker
        ///////////////////////////////////////////////////////////////////////

        // Log system configuration if not yet.
        SparkConnectorContext.recordConfig()

        // Get file transfer metadata object
        val metadata = if (oneMetadataPerFile) {
          metadatas(index)
        } else {
          metadatas.head
        }
        // Convert and upload the partition with the file transfer metadata
        uploadPartition(rows, format, compress, directory, index, None, Some(metadata))

        ///////////////////////////////////////////////////////////////////////
        // End code snippet to executed on worker
        ///////////////////////////////////////////////////////////////////////
    }

    val endTime = System.currentTimeMillis()
    CloudStorageOperations.log.info(
      s"""${SnowflakeResultSetRDD.MASTER_LOG_PREFIX}:
         | Finish uploading data for ${data.getNumPartitions} partitions in
         | ${Utils.getTimeString(endTime - startTime)}.
         |""".stripMargin.filter(_ >= ' '))

    fileUploadResults.collect().toList
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

object StorageUtils {
  private val log = CloudStorageOperations.log

  private[io] def logAzureInfo(isUpload: Boolean,
                               stageType: String,
                               file: String,
                               requestID: String,
                               containerName: String): Unit = {

    val streamInfo = if (isUpload) {
      s"outputStream for uploading to"
    } else {
      s"inputStream for downloading from"
    }
    log.info(
      s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}:
         | Retrieve $streamInfo $stageType stage:
         | file: $file client request id: $requestID
         | container=$containerName
         |""".stripMargin.filter(_ >=
        ' '))
  }

  private[io] def logAzureThrowable(isUpload: Boolean,
                                    stageType: String,
                                    file: String,
                                    blob: CloudBlockBlob,
                                    opContext: OperationContext,
                                    th: Throwable): Throwable = {
    val streamInfo = if (isUpload) {
      s"outputStream for uploading to"
    } else {
      s"inputStream for downloading from"
    }

    log.error(
      s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}: Hit error when
         | Retrieve $streamInfo $stageType stage:
         | file: $file client request id: ${opContext.getClientRequestID}
         | container=${blob.getContainer.getName}
         | URI=${blob.getUri.getAuthority}
         | error message: ${th.getMessage}
         |""".stripMargin.filter(_ >=
        ' '))

    // Return the exception because the caller needs to re-throw it
    th
  }

  private[io] def logPresignedUrlGenerateProgress(total: Int,
                                                  index: Int,
                                                  startTime: Long): Unit = {
    val endTime = System.currentTimeMillis()
    CloudStorageOperations.log.info(
      s"""${SnowflakeResultSetRDD.MASTER_LOG_PREFIX}:
         | Time to retrieve pre-signed URL for
         | $index/$total files is
         | ${Utils.getTimeString(endTime - startTime)}.
         |""".stripMargin.filter(_ >= ' '))
  }
}

private[io] class S3UploadOutputStream(s3Client: AmazonS3,
                                       meta: ObjectMetadata,
                                       storageInfo: Map[String, String],
                                       bufferSize: Int,
                                       file: String) extends OutputStream {
  // If the total data size is <= bufferSize, just use normal API to upload data
  private var useMultipleBlockUpload = false
  // Data size in buffer
  private var dataSizeInBuffer: Int = 0
  private val byteArrayOutputStream = new ByteArrayOutputStream()
  private var totalDataSize: Long = 0
  private var totalUploadTime: Long = 0
  // Create a list of ETag objects. You retrieves ETags for each object part uploaded,
  // then, after each individual part has been uploaded, pass the list of ETags to
  // the request to complete the upload.
  private val partETags = new java.util.ArrayList[PartETag]
  // Valid part ID is [1-10000]
  private var partId = 1
  // AWS bucket and file information
  private val bucketName = storageInfo(StorageInfo.BUCKET_NAME)
  private val keyName = storageInfo(StorageInfo.PREFIX).concat(file)

  // If data size is less than one block, normal API is used to upload data,
  // so initiate the multipart upload lazily.
  private lazy val initRequest =
    new InitiateMultipartUploadRequest(bucketName, keyName, meta)
  private lazy val initResponse: InitiateMultipartUploadResult =
    s3Client.initiateMultipartUpload(initRequest)

  // get data in buffer for testing purpose only
  private[io] def getDataInBufferForTest(): Array[Byte] =
    byteArrayOutputStream.toByteArray

  private def uploadDataChunk(isLastChunk: Boolean): Unit = {
    // If there total data size is less than 1 chunk, use normal API
    if (isLastChunk && !useMultipleBlockUpload) {
      val start = System.currentTimeMillis()
      // Set up length to avoid S3 client API to raise warning message.
      meta.setContentLength(dataSizeInBuffer)
      byteArrayOutputStream.close()
      val buffer = byteArrayOutputStream.toByteArray
      val inputStream: InputStream =
        new ByteArrayInputStream(buffer, 0, dataSizeInBuffer)
      s3Client.putObject(bucketName, keyName, inputStream, meta)
      val usedTime = System.currentTimeMillis() - start
      totalUploadTime += usedTime

      CloudStorageOperations.log.info(
        s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}:
           | Finish uploading file $file without AWS multiple parts API
           | because the data size is less than the buffer size:
           | bufferSize=${Utils.getSizeString(bufferSize)}
           | compressedSize=${Utils.getSizeString(dataSizeInBuffer)}
           |""".stripMargin.filter(_ >= ' '))
    } else {
      // Upload the data chunk with Multiple block upload API
      doUploadOnePart()

      // For last block, commit the multipart upload.
      if (isLastChunk) {
        byteArrayOutputStream.close()
        val start = System.currentTimeMillis()
        val compRequest = new CompleteMultipartUploadRequest(
          bucketName, keyName, initResponse.getUploadId, partETags)
        s3Client.completeMultipartUpload(compRequest)
        val usedTime = System.currentTimeMillis() - start
        totalUploadTime += usedTime
        CloudStorageOperations.log.info(
          s"""${SnowflakeResultSetRDD.WORKER_LOG_PREFIX}:
             | Finish uploading file $file with AWS multiple parts API:
             | bufferSize=${Utils.getSizeString(bufferSize)} chunkCount=$partId
             | compressedSize=${Utils.getSizeString(totalDataSize)}
             |""".stripMargin.filter(_ >= ' '))
      }
    }
  }

  // upload one part with multiple part upload API
  private def doUploadOnePart(): Unit = {
    val start = System.currentTimeMillis()
    // Indicate the upload to using multiple parts API.
    useMultipleBlockUpload = true
    // byteArrayOutputStream.close()
    val buffer = byteArrayOutputStream.toByteArray
    val inputStream: InputStream =
      new ByteArrayInputStream(buffer, 0, dataSizeInBuffer)

    // Create the request to upload a part.
    val uploadRequest = new UploadPartRequest()
      .withBucketName(bucketName)
      .withKey(keyName)
      .withUploadId(initResponse.getUploadId)
      .withPartNumber(partId)
      .withInputStream(inputStream)
      .withPartSize(dataSizeInBuffer)

    // Upload the part and add the response's ETag to our list.
    val uploadResult = s3Client.uploadPart(uploadRequest)
    partETags.add(uploadResult.getPartETag)

    // Update status
    totalDataSize += dataSizeInBuffer
    dataSizeInBuffer = 0
    partId += 1
    byteArrayOutputStream.reset()
    // byteArrayOutputStream = new ByteArrayOutputStream(bufferSize + 8)

    val usedTime = System.currentTimeMillis() - start
    totalUploadTime += usedTime

    if (partId == 3) {
      // Inject exception for test purpose
      TestHook.raiseExceptionIfTestFlagEnabled(
        TestHookFlag.TH_FAIL_UPLOAD_AWS_2ND_BLOCK,
        "Negative test to raise error after upload 2nd parts"
      )
    }
  }

  override def write(b: Int): Unit = {
    // If there is no room in the buffer, then write the buffer,
    // and then put current BYTE in the buffer
    if (dataSizeInBuffer >= bufferSize) {
      uploadDataChunk(isLastChunk = false)
    }

    byteArrayOutputStream.write(b)
    dataSizeInBuffer += 1
  }

  // This function need to be override for better performance
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    val inputSize = len - off
    if ((inputSize + dataSizeInBuffer) <= bufferSize) {
      // There is enough room on the buffer, just put it
      byteArrayOutputStream.write(b, off, inputSize)
      dataSizeInBuffer += inputSize
    } else {
      // There is not enough room on the buffer,
      // The input needs to be moved to buffer with 2 parts
      // (If the input data size is bigger than buffer size,
      // it may be put in more than 2 parts)

      // First, write first part into buffer
      val firstPartSize = bufferSize - dataSizeInBuffer
      if (firstPartSize > 0) {
        // if the buffer is already full, skip this step
        byteArrayOutputStream.write(b, off, firstPartSize)
        dataSizeInBuffer += firstPartSize
      }

      var leftLen = len - firstPartSize
      var newOff = off + firstPartSize
      // Second, write the second (or following) part.
      // NOTE: If input size is much bigger than buffer size,
      // it can be multiple parts
      do {
        // write the full buffer
        uploadDataChunk(isLastChunk = false)

        // Put the next part.
        val partSize = Math.min(bufferSize, leftLen)
        byteArrayOutputStream.write(b, newOff, partSize)
        dataSizeInBuffer += partSize

        // move indicators
        leftLen -= partSize
        newOff += partSize
      } while (leftLen > 0)
    }
  }

  override def close(): Unit = {
    // "dataSizeInBuffer is zero" means write() is never called.
    // For this case, the file upload is skipped.
    if (dataSizeInBuffer > 0) {
      uploadDataChunk(isLastChunk = true)
    }
  }
}

