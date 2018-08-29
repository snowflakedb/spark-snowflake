/*
 * Copyright 2017 - 2018 Snowflake Computing
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


import java.io.InputStream
import java.net.URI
import java.security.SecureRandom
import java.sql.Connection
import java.util

import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import javax.crypto.{Cipher, CipherInputStream, SecretKey}
import net.snowflake.client.jdbc.internal.amazonaws.ClientConfiguration
import net.snowflake.client.jdbc.internal.amazonaws.auth.{BasicAWSCredentials, BasicSessionCredentials}
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.{AmazonS3Client, AmazonS3EncryptionClient}
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model._
import net.snowflake.client.jdbc.internal.amazonaws.util.Base64
import net.snowflake.client.core.SFStatement
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState
import net.snowflake.client.jdbc._
import net.snowflake.client.jdbc.cloud.storage.StageInfo
import net.snowflake.client.jdbc.cloud.storage.StageInfo.StageType
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.{StorageCredentialsAnonymous, StorageCredentialsSharedAccessSignature}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.CloudBlobClient
import net.snowflake.spark.snowflake.{JDBCWrapper, SnowflakeConnectorException, SnowflakeTelemetry, Utils}
import net.snowflake.spark.snowflake.Parameters.MergedParameters

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Created by ema on 4/14/17.
  */
private[io] object SFInternalStage {
  private[io] final val DUMMY_LOCATION =
    "file:///tmp/dummy_location_spark_connector_tmp/"
  private[io] final val AES = "AES"
  @Deprecated
  private[io] final val DEFAULT_PARALLELISM = 10
  @Deprecated
  private[io] final val S3_MAX_RETRIES = 3
  private[io] final val CREATE_TEMP_STAGE_STMT =
    s"""CREATE OR REPLACE TEMP STAGE """
  private[io] final val AMZ_KEY: String = "x-amz-key"
  private[io] final val AMZ_IV: String = "x-amz-iv"
  private[io] final val DATA_CIPHER: String = "AES/CBC/PKCS5Padding"
  private[io] final val KEY_CIPHER: String = "AES/ECB/PKCS5Padding"
  private[io] final val AMZ_MATDESC = "x-amz-matdesc"


  private[io] final val AZ_ENCRYPTIONDATA = "encryptiondata"
  private[io] final val AZ_IV = "ContentEncryptionIV"
  private[io] final val AZ_KEY_WRAP = "WrappedContentKey"
  private[io] final val AZ_KEY = "EncryptedKey"
  private[io] final val AZ_MATDESC = "matdesc"

  /**
    * A small helper for extracting bucket name and path from stage location.
    *
    * @param stageLocation stage location
    * @return s3 location
    */
  private[io] final def extractBucketNameAndPath(
                                                  stageLocation: String): (String, String) =
    if (stageLocation.contains("/"))
      (stageLocation.substring(0, stageLocation.indexOf("/")),
        stageLocation.substring(stageLocation.indexOf("/") + 1))
    else (stageLocation, "")

  private[io] final def TEMP_STAGE_LOCATION: String =
    "spark_connector_unload_stage_" + (Random.alphanumeric take 10 mkString "")

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


  private[io] final def createS3Client(
                                        is256: Boolean,
                                        masterKey: String,
                                        queryId: String,
                                        smkId: String,
                                        awsId: String,
                                        awsKey: String,
                                        awsToken: String,
                                        parallel: Option[Int] = None): AmazonS3Client = {

    val parallelism = parallel.getOrElse(DEFAULT_PARALLELISM)

    val decodedKey = Base64.decode(masterKey)
    val queryStageMasterKey: SecretKey =
      new SecretKeySpec(decodedKey, 0, decodedKey.length, AES)

    val awsCredentials =
      if (awsToken != null)
        new BasicSessionCredentials(awsId, awsKey, awsToken)
      else new BasicAWSCredentials(awsId, awsKey)

    val clientConfig = new ClientConfiguration
    clientConfig.setMaxConnections(parallelism)
    clientConfig.setMaxErrorRetry(S3_MAX_RETRIES)

    if (is256) {
      val cryptoConfig =
        new CryptoConfiguration(CryptoMode.EncryptionOnly)

      val encryptionMaterials =
        new EncryptionMaterials(queryStageMasterKey)
      encryptionMaterials.addDescription("queryId", queryId)
      encryptionMaterials.addDescription("smkId", smkId)

      new AmazonS3EncryptionClient(
        awsCredentials,
        new StaticEncryptionMaterialsProvider(encryptionMaterials),
        clientConfig,
        cryptoConfig)
    } else {
      new AmazonS3Client(awsCredentials, clientConfig)
    }
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

  private[io] final def getDecryptedStream(
                                            stream: InputStream,
                                            masterKey: String,
                                            metaData: util.Map[String, String],
                                            stageType: StageType
                                            //meta: ObjectMetadata
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

    val fileKeyBytes: Array[Byte] = keyCipher.doFinal(keyBytes) // NB: we assume qsmk
    // .length == fileKey.length
    //     (fileKeyBytes.length may be bigger due to padding)
    val fileKey =
    new SecretKeySpec(fileKeyBytes, 0, decodedKey.length, AES)

    val dataCipher = Cipher.getInstance(DATA_CIPHER)
    val ivy: IvParameterSpec = new IvParameterSpec(ivBytes)
    dataCipher.init(Cipher.DECRYPT_MODE, fileKey, ivy)
    new CipherInputStream(stream, dataCipher)
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

  private final def getCipherAndMetadata(
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


}

private[io] class SFInternalStage(isWrite: Boolean,
                                  jdbcWrapper: JDBCWrapper,
                                  params: MergedParameters,
                                  stage: Option[String] = None,
                                  fileName: String = "",
                                  conn: Option[Connection] = None
                                 ) {

  import SFInternalStage._

  private lazy val connection: SnowflakeConnectionV1 =
    conn.getOrElse(jdbcWrapper.getConnector(params)).asInstanceOf[SnowflakeConnectionV1]

  private lazy val sfAgent = {
    Utils.setLastPutCommand(command)
    Utils.setLastGetCommand(command)

    new SnowflakeFileTransferAgent(command,
      connection.getSfSession,
      new SFStatement(connection.getSfSession))
  }

  private lazy val encryptionMaterials = sfAgent.getEncryptionMaterial
  private lazy val srcMaterialsMap =
    if (!isWrite) sfAgent.getSrcToMaterialsMap else null
  private lazy val stageCredentials = sfAgent.getStageCredentials

  private lazy val stageInfo: StageInfo = sfAgent.getStageInfo

  private lazy val encMat =
    if (encryptionMaterials.size() > 0) {
      encryptionMaterials.get(0)
    } else null

  private[io] lazy val stageType: StageInfo.StageType =
    stageInfo.getStageType

  //try get aws credentials
  private[io] lazy val awsId: Option[String] =
    if (stageType == StageInfo.StageType.S3)
      Option(stageCredentials.get("AWS_ID").toString)
    else None

  private[io] lazy val awsKey: Option[String] =
    if (stageType == StageInfo.StageType.S3)
      Option(stageCredentials.get("AWS_KEY").toString)
    else None

  private[io] lazy val awsToken: Option[String] =
    if (stageType == StageInfo.StageType.S3)
      Option(stageCredentials.get("AWS_TOKEN").toString)
    else None

  //try get azure credentials

  private[io] lazy val azureSAS: Option[String] =
    if (stageType == StageInfo.StageType.AZURE)
      Option(stageCredentials.get("AZURE_SAS_TOKEN").toString)
    else None

  private[io] lazy val azureEndpoint: Option[String] =
    if (stageType == StageInfo.StageType.AZURE)
      Option(stageInfo.getEndPoint)
    else None

  private[io] lazy val azureAccountName: Option[String] =
    if (stageType == StageInfo.StageType.AZURE)
      Option(stageInfo.getStorageAccount)
    else None

  private[io] lazy val stageLocation: String =
    sfAgent.getStageLocation

  private[io] lazy val getKeyIds: Seq[(String, String, String)] = {
    if (srcMaterialsMap != null) {
      srcMaterialsMap.asScala.toList.map {
        case (k, v) =>
          (k,
            if (v != null) v.getQueryId else null,
            if (v != null) v.getSmkId.toString else null)
      }
    } else {
      encryptionMaterials.asScala map { encMat =>
        ("", encMat.getQueryId, encMat.getSmkId.toString)
      }
    }
  }

  private[io] lazy val masterKey =
    if (encMat != null) encMat.getQueryStageMasterKey else null
  private[io] lazy val decodedKey =
    if (masterKey != null) Base64.decode(masterKey) else null

  private[io] lazy val is256Encryption: Boolean = {
    val length = if (decodedKey != null) decodedKey.length * 8 else 128
    if (length == 256)
      true
    else if (length == 128)
      false
    else
      throw new SnowflakeConnectorException(s"Unsupported Key Size: $length")
  }


  private lazy val tempStage: String =
    stage match {
      case Some(str) => str
      case None =>
        val name = TEMP_STAGE_LOCATION
        jdbcWrapper.executeQueryInterruptibly(
          connection, CREATE_TEMP_STAGE_STMT + name)
        name
    }

  private val dummyLocation = DUMMY_LOCATION

  private val command = {
    val comm =
      if (isWrite)
        s"PUT $dummyLocation @$tempStage"
      else
        s"GET @$tempStage/$fileName $dummyLocation"

    if (params.parallelism.isDefined) {
      comm + s" PARALLEL=${params.parallelism.get}"
    }
    else
      comm
  }


  private[io] def closeConnection(): Unit = {
    SnowflakeTelemetry.send(jdbcWrapper.getTelemetry(connection))
    connection.close()
  }

  private[io] def getEncryptionMaterials = encryptionMaterials

  private[io] def setupStageArea(): String = {
    tempStage
  }

  private[io] def executeWithConnection(
                                         connectionFunc: (Connection => Any)) = {
    connectionFunc(connection)
  }
}
