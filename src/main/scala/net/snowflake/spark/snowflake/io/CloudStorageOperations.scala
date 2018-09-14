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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.net.URI
import java.security.SecureRandom
import java.sql.Connection
import java.util
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import javax.crypto.{Cipher, CipherInputStream, CipherOutputStream, SecretKey}
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import net.snowflake.client.jdbc.{ErrorCode, MatDesc, SnowflakeSQLException}
import net.snowflake.client.jdbc.cloud.storage.StageInfo.StageType
import net.snowflake.client.jdbc.internal.amazonaws.ClientConfiguration
import net.snowflake.client.jdbc.internal.amazonaws.auth.{BasicAWSCredentials, BasicSessionCredentials}
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3Client
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model._
import net.snowflake.client.jdbc.internal.amazonaws.util.Base64
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.{StorageCredentialsAnonymous, StorageCredentialsSharedAccessSignature}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.CloudBlobClient
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState
import net.snowflake.spark.snowflake.DefaultJDBCWrapper
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.Utils
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.util.{Random, Try}


//todo: replace all storage operations by the methods in this class

//todo: split get and put

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
          throw new
              UnsupportedOperationException(
                s"Only support s3 or azure stage. Stage Type: $stageType")
      }


    if (key == null || iv == null)
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
        ErrorCode.INTERNAL_ERROR.getMessageCode,
        "File " + "metadata incomplete")

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

  private[io] final def parseEncryptionData(jsonEncryptionData: String):
  (String, String) = {
    val mapper: ObjectMapper = new ObjectMapper()
    val encryptionDataNode: JsonNode = mapper.readTree(jsonEncryptionData)
    val iv: String = encryptionDataNode.findValue(AZ_IV).asText()
    val key: String = encryptionDataNode
      .findValue(AZ_KEY_WRAP).findValue(AZ_KEY).asText()
    (key, iv)
  }

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

  def createStorageClientFromStage(
                                    param: MergedParameters,
                                    conn: Connection,
                                    stageName: String,
                                    dir: Option[String] = None,
                                    temporary: Boolean = false
                                  ): CloudStorage = {
    def stageNotExists(stage: String): Boolean = {
      val sql = s"desc stage $stage"
      Try {
        DefaultJDBCWrapper.executeQueryInterruptibly(conn, sql)
      }.isFailure
    }

    if (stageNotExists(stageName)) {
      val sql =
        s"""
           |create ${if (temporary) "temporary" else ""} stage $stageName
           """.stripMargin
      DefaultJDBCWrapper.executeQueryInterruptibly(conn, sql)
    }

    @transient val stageManager =
      new SFInternalStage(false, DefaultJDBCWrapper, param, Some(stageName))

    stageManager.stageType match {
      case StageType.S3 =>
        InternalS3Storage(param, stageName)

      case StageType.AZURE =>
        InternalAzureStorage(param, stageName)

      case _ =>
        throw new UnsupportedOperationException(
          s"Only support s3 or Azure stage, stage types: ${stageManager.stageType}"
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
                           stage: Option[String] = None
                         ): (CloudStorage, String) = {
    val propolueSql = Utils.genPrologueSql(param)
    log.debug(propolueSql.statementString)
    propolueSql.execute(conn)


    val azure_url = "wasbs?://([^@]+)@([^\\.]+)\\.([^/]+)/(.*)".r
    val s3_url = "s3[an]://([^/]+)/(.*)".r
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

        (ExternalAzureStorage(
          containerName = container,
          azureAccount = account,
          azureEndpoint = endpoint,
          azureSAS = azureSAS,
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

        (ExternalS3Storage(
          bucketName = bucket,
          awsId = param.awsAccessKey.get,
          awsKey = param.awsSecretKey.get,
          pref = prefix
        ), stageName)
      case _ => // Internal Stage
        (createStorageClientFromStage(param, conn, stageName, None, tempStage), stageName)
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

  def deleteFiles(files: List[String])(implicit storage: CloudStorage, connection: Connection): Unit =
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

  def upload(fileName: String, dir: Option[String], compress: Boolean)
            (implicit connection: Connection): OutputStream

  def upload(
              data: RDD[String],
              format: SupportedFormat = SupportedFormat.CSV,
              dir: Option[String],
              compress: Boolean = true
            ): List[String] = {
    val directory: String =
      dir match {
        case Some(str: String) => str
        case None => Random.alphanumeric take 10 mkString ""
      }
    val files = data.mapPartitionsWithIndex {
      case (index, rows) => {
        val fileName =
          s"$index.${format.toString}${if (compress) ".gz" else ""}"

        val outputStream = upload(fileName, Some(directory), compress)(null)
        while (rows.hasNext) {
          outputStream.write(rows.next.getBytes("UTF-8"))
          outputStream.write('\n')
        }
        outputStream.close()
        new SingleElementIterator(s"$directory/$fileName")
      }
    }

    files.collect().toList
  }

  def download(fileName: String, compress: Boolean)
              (implicit connection: Connection): InputStream

  def deleteFile(fileName: String)
                (implicit connection: Connection): Unit

  def deleteFiles(fileNames: List[String])
                 (implicit connection: Connection): Unit =
    fileNames.foreach(deleteFile)

  def fileExists(fileName: String)
                (implicit connection: Connection): Boolean
}

case class InternalAzureStorage(
                                 param: MergedParameters,
                                 stageName: String
                               ) extends CloudStorage {


  private def getStageInfo(isWrite: Boolean, fileName:String = "", conn: Option[Connection] = None):
  (String, String, String, String, String, String, String, String) = {
    @transient val stageManager = new SFInternalStage(isWrite, DefaultJDBCWrapper, param, Some(stageName), fileName, conn)
    @transient val keyIds = stageManager.getKeyIds
    val (_, queryId, smkId) = if (keyIds.nonEmpty) keyIds.head else ("", "", "")
    val masterKey = stageManager.masterKey
    val stageLocation = stageManager.stageLocation
    val url = "([^/]+)/?(.*)".r
    val url(container, path) = stageLocation
    val azureSAS = stageManager.azureSAS.get
    val azureAccount = stageManager.azureAccountName.get
    val azureEndPoint = stageManager.azureEndpoint.get
    val prefix: String =
      if (path.isEmpty) path else if (path.endsWith("/")) path else path + "/"

    (container, azureAccount, azureEndPoint, azureSAS, masterKey, queryId, smkId, prefix)
  }


  override def upload(fileName: String, dir: Option[String], compress: Boolean)
                     (implicit connection: Connection): OutputStream = {

    val (containerName, azureAccount, azureEndpoint, azureSAS, masterKey, queryId, smkId, prefix)
    = getStageInfo(true, conn = Option(connection))

    val file: String =
      if (dir.isDefined) s"${dir.get}/$fileName"
      else fileName

    val blob =
      CloudStorageOperations
        .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))
        .getContainerReference(containerName)
        .getBlockBlobReference(prefix.concat(file))

    val encryptedStream = {
      val (cipher, meta) =
        CloudStorageOperations
          .getCipherAndAZMetaData(masterKey, queryId, smkId)
      blob.setMetadata(meta)
      new CipherOutputStream(blob.openOutputStream(), cipher)
    }


    if (compress) new GZIPOutputStream(encryptedStream) else encryptedStream

  }

  override def download(fileName: String, compress: Boolean)
                       (implicit connection: Connection): InputStream = {

    val (containerName, azureAccount, azureEndpoint, azureSAS, masterKey, _, _, prefix)
    = getStageInfo(false, fileName, Option(connection))

    println(s"download fileName:${prefix.concat(fileName)}")
    val blob =
      CloudStorageOperations
        .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))
        .getContainerReference(containerName)
        .getBlockBlobReference(prefix.concat(fileName))

    val azureStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    blob.download(azureStream)
    blob.downloadAttributes()

    val inputStream: InputStream =
      CloudStorageOperations.getDecryptedStream(
        new ByteArrayInputStream(azureStream.toByteArray),
        masterKey,
        blob.getMetadata,
        StageType.AZURE
      )

    if (compress) new GZIPInputStream(inputStream)
    else inputStream
  }

  override def deleteFile(fileName: String)
                         (implicit connection: Connection): Unit = {

    val (containerName, azureAccount, azureEndpoint, azureSAS, _, _, _, prefix)
    = getStageInfo(true, conn = Option(connection))

    CloudStorageOperations
      .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))
      .getContainerReference(containerName)
      .getBlockBlobReference(prefix.concat(fileName)).deleteIfExists()
  }

  override def deleteFiles(fileNames: List[String])
                          (implicit connection: Connection): Unit = {
    val (containerName, azureAccount, azureEndpoint, azureSAS, _, _, _, prefix)
    = getStageInfo(true, conn = Option(connection))

    val container =
      CloudStorageOperations
        .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))
        .getContainerReference(containerName)

    fileNames
      .map(prefix.concat)
      .foreach(container.getBlockBlobReference(_).deleteIfExists())
  }


  override def fileExists(fileName: String)
                         (implicit connection: Connection): Boolean = {
    val (containerName, azureAccount, azureEndpoint, azureSAS, _, _, _, prefix)
    = getStageInfo(false, conn = Option(connection))
    CloudStorageOperations
      .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))
      .getContainerReference(containerName)
      .getBlockBlobReference(prefix.concat(fileName)).exists()
  }
}

case class ExternalAzureStorage(
                                 containerName: String,
                                 azureAccount: String,
                                 azureEndpoint: String,
                                 azureSAS: String,
                                 pref: String = ""
                               ) extends CloudStorage {

  lazy val prefix: String =
    if (pref.isEmpty) pref else if (pref.endsWith("/")) pref else pref + "/"

  override def upload(fileName: String, dir: Option[String], compress: Boolean)
                     (implicit connection: Connection): OutputStream = {
    val file: String =
      if (dir.isDefined) s"${dir.get}/$fileName"
      else fileName

    val blob =
      CloudStorageOperations
        .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))
        .getContainerReference(containerName)
        .getBlockBlobReference(prefix.concat(file))

    if (compress) new GZIPOutputStream(blob.openOutputStream()) else blob.openOutputStream()

  }

  override def download(fileName: String, compress: Boolean)
                       (implicit connection: Connection): InputStream = {

    println(s"download fileName:${prefix.concat(fileName)}")
    val blob =
      CloudStorageOperations
        .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))
        .getContainerReference(containerName)
        .getBlockBlobReference(prefix.concat(fileName))

    val azureStream: ByteArrayOutputStream = new ByteArrayOutputStream()
    blob.download(azureStream)
    blob.downloadAttributes()

    val inputStream: InputStream = new ByteArrayInputStream(azureStream.toByteArray)
    if (compress) new GZIPInputStream(inputStream)
    else inputStream
  }

  override def deleteFile(fileName: String)
                         (implicit connection: Connection): Unit =
    CloudStorageOperations
      .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))
      .getContainerReference(containerName)
      .getBlockBlobReference(prefix.concat(fileName)).deleteIfExists()

  override def deleteFiles(fileNames: List[String])
                          (implicit connection: Connection): Unit = {
    val container =
      CloudStorageOperations
        .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))
        .getContainerReference(containerName)

    fileNames
      .map(prefix.concat)
      .foreach(container.getBlockBlobReference(_).deleteIfExists())
  }

  override def fileExists(fileName: String)
                         (implicit connection: Connection): Boolean =
    CloudStorageOperations
      .createAzureClient(azureAccount, azureEndpoint, Some(azureSAS))
      .getContainerReference(containerName)
      .getBlockBlobReference(prefix.concat(fileName)).exists()
}

case class InternalS3Storage(
                              param: MergedParameters,
                              stageName: String,
                              parallelism: Int = CloudStorageOperations.DEFAULT_PARALLELISM
                            ) extends CloudStorage {

  private def getStageInfo(isWrite: Boolean, fileName: String = "", conn: Option[Connection] = None):
  (String, String, String, Option[String], String, String, String, String) = {
    @transient val stageManager =
      new SFInternalStage(isWrite, DefaultJDBCWrapper, param, Some(stageName), fileName, conn)
    //todo move stage creation from stage manager to this class

    @transient val keyIds = stageManager.getKeyIds
    val (_, queryId, smkId) = if (keyIds.nonEmpty) keyIds.head else ("", "", "")
    val masterKey = stageManager.masterKey
    val stageLocation = stageManager.stageLocation
    val url = "([^/]+)/?(.*)".r
    val url(bucket, path) = stageLocation
    val awsId = stageManager.awsId.get
    val awsKey = stageManager.awsKey.get
    val awsToken = stageManager.awsToken

    val prefix: String =
      if (path.isEmpty) path else if (path.endsWith("/")) path else path + "/"

    (bucket, awsId, awsKey, awsToken, masterKey, queryId, smkId, prefix)
  }


  override def upload(fileName: String, dir: Option[String], compress: Boolean)
                     (implicit connection: Connection): OutputStream = {

    val (bucketName, awsId, awsKey, awsToken, masterKey, queryId, smkId, prefix) = getStageInfo(true,  conn = Option(connection))


    val file: String =
      if (dir.isDefined) s"${dir.get}/$fileName"
      else fileName

    val s3Client: AmazonS3Client = CloudStorageOperations.createS3Client(awsId, awsKey, awsToken, parallelism)

    val (fileCipher, meta) =
      CloudStorageOperations.getCipherAndS3Metadata(masterKey, queryId, smkId)

    if (compress) meta.setContentEncoding("GZIP")

    var outputStream: OutputStream = new OutputStream {
      val buffer: ByteArrayOutputStream = new ByteArrayOutputStream()

      override def write(b: Int): Unit = buffer.write(b)

      override def close(): Unit = {
        buffer.close()
        val inputStream: InputStream =
          new ByteArrayInputStream(buffer.toByteArray)
        s3Client.putObject(bucketName, prefix.concat(file), inputStream, meta)
      }
    }

    outputStream =
      new CipherOutputStream(outputStream, fileCipher)

    if (compress) new GZIPOutputStream(outputStream)
    else outputStream
  }

  override def download(fileName: String, compress: Boolean)
                       (implicit connection: Connection): InputStream = {
    val (bucketName, awsId, awsKey, awsToken, masterKey, _, _, prefix) = getStageInfo(false, fileName, Option(connection))

    val s3Client: AmazonS3Client = CloudStorageOperations.createS3Client(awsId, awsKey, awsToken, parallelism)
    val dataObject = s3Client.getObject(bucketName, prefix.concat(fileName))
    var inputStream: InputStream = dataObject.getObjectContent
    inputStream =
      CloudStorageOperations.getDecryptedStream(
        inputStream,
        masterKey,
        dataObject.getObjectMetadata.getUserMetadata,
        StageType.S3
      )
    if (compress) new GZIPInputStream(inputStream)
    else inputStream

  }

  override def deleteFile(fileName: String)
                         (implicit connection: Connection): Unit = {
    val (bucketName, awsId, awsKey, awsToken, _, _, _, prefix) = getStageInfo(true, conn = Option(connection))
    CloudStorageOperations
      .createS3Client(awsId, awsKey, awsToken, parallelism)
      .deleteObject(bucketName, prefix.concat(fileName))
  }

  override def deleteFiles(fileNames: List[String])
                          (implicit connection: Connection): Unit = {
    val (bucketName, awsId, awsKey, awsToken, _, _, _, prefix) = getStageInfo(true, conn = Option(connection))
    CloudStorageOperations
      .createS3Client(awsId, awsKey, awsToken, parallelism)
      .deleteObjects(
        new DeleteObjectsRequest(bucketName)
          .withKeys(fileNames.map(prefix.concat): _*)
      )
  }

  override def fileExists(fileName: String)
                         (implicit connection: Connection): Boolean = {
    val (bucketName, awsId, awsKey, awsToken, _, _, _, prefix) = getStageInfo(false, conn = Option(connection))
    val s3Client: AmazonS3Client = CloudStorageOperations.createS3Client(awsId, awsKey, awsToken, parallelism)
    s3Client.doesObjectExist(bucketName, prefix.concat(fileName))
  }
}

case class ExternalS3Storage(
                              bucketName: String,
                              awsId: String,
                              awsKey: String,
                              awsToken: Option[String] = None,
                              pref: String = "",
                              parallelism: Int = CloudStorageOperations.DEFAULT_PARALLELISM
                            ) extends CloudStorage {

  lazy val prefix: String =
    if (pref.isEmpty) pref else if (pref.endsWith("/")) pref else pref + "/"

  override def upload(fileName: String, dir: Option[String], compress: Boolean)
                     (implicit connection: Connection): OutputStream = {
    val file: String =
      if (dir.isDefined) s"${dir.get}/$fileName"
      else fileName

    val s3Client: AmazonS3Client = CloudStorageOperations.createS3Client(awsId, awsKey, awsToken, parallelism)

    val meta = new ObjectMetadata()

    if (compress) meta.setContentEncoding("GZIP")

    val outputStream: OutputStream = new OutputStream {
      val buffer: ByteArrayOutputStream = new ByteArrayOutputStream()

      override def write(b: Int): Unit = buffer.write(b)

      override def close(): Unit = {
        buffer.close()
        val inputStream: InputStream =
          new ByteArrayInputStream(buffer.toByteArray)
        s3Client.putObject(bucketName, prefix.concat(file), inputStream, meta)
      }
    }

    if (compress) new GZIPOutputStream(outputStream)
    else outputStream
  }

  override def download(fileName: String, compress: Boolean)
                       (implicit connection: Connection): InputStream = {
    val s3Client: AmazonS3Client = CloudStorageOperations.createS3Client(awsId, awsKey, awsToken, parallelism)
    val dataObject = s3Client.getObject(bucketName, prefix.concat(fileName))
    val inputStream: InputStream = dataObject.getObjectContent
    if (compress) new GZIPInputStream(inputStream)
    else inputStream

  }

  override def deleteFile(fileName: String)
                         (implicit connection: Connection): Unit =
    CloudStorageOperations
      .createS3Client(awsId, awsKey, awsToken, parallelism)
      .deleteObject(bucketName, prefix.concat(fileName))

  override def deleteFiles(fileNames: List[String])
                          (implicit connection: Connection): Unit =
    CloudStorageOperations
      .createS3Client(awsId, awsKey, awsToken, parallelism)
      .deleteObjects(
        new DeleteObjectsRequest(bucketName)
          .withKeys(fileNames.map(prefix.concat): _*)
      )

  override def fileExists(fileName: String)
                         (implicit connection: Connection): Boolean = {
    val s3Client: AmazonS3Client = CloudStorageOperations.createS3Client(awsId, awsKey, awsToken, parallelism)
    s3Client.doesObjectExist(bucketName, prefix.concat(fileName))
  }
}

//todo: google cloud, local file for testing?
