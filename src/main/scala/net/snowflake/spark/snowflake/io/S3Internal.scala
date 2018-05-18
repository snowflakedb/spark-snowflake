package net.snowflake.spark.snowflake.io

import java.io.InputStream
import java.security.SecureRandom
import java.sql.Connection

import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import javax.crypto.{Cipher, CipherInputStream, SecretKey}
import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{BasicAWSCredentials, BasicSessionCredentials}
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3EncryptionClient}
import com.amazonaws.services.s3.model._
import com.amazonaws.util.Base64
import net.snowflake.client.core.SFStatement
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState
import net.snowflake.client.jdbc._
import net.snowflake.spark.snowflake.{JDBCWrapper, SnowflakeConnectorException, Utils}
import net.snowflake.spark.snowflake.Parameters.MergedParameters

import scala.collection.JavaConverters._
import scala.util.Random

/**
  * Created by ema on 4/14/17.
  */
private[snowflake] object S3Internal {
  private[snowflake] final val DUMMY_LOCATION =
    "file:///tmp/dummy_location_spark_connector_tmp/"
  private[snowflake] final val AES                 = "AES"
  private[snowflake] final val DEFAULT_PARALLELISM = 10
  private[snowflake] final val S3_MAX_RETRIES      = 3
  private[snowflake] final val CREATE_TEMP_STAGE_STMT =
    s"""CREATE OR REPLACE TEMP STAGE """
  private[snowflake] final val AMZ_KEY: String     = "x-amz-key"
  private[snowflake] final val AMZ_IV: String      = "x-amz-iv"
  private[snowflake] final val DATA_CIPHER: String = "AES/CBC/PKCS5Padding"
  private[snowflake] final val KEY_CIPHER: String  = "AES/ECB/PKCS5Padding"
  private[snowflake] final val AMZ_MATDESC         = "x-amz-matdesc"

  /**
    * A small helper for extracting bucket name and path from stage location.
    *
    * @param stageLocation stage location
    * @return s3 location
    */
  private[snowflake] final def extractBucketNameAndPath(
      stageLocation: String): (String, String) =
    if(stageLocation.contains("/"))
      (stageLocation.substring(0,stageLocation.indexOf("/")),
      stageLocation.substring(stageLocation.indexOf("/") + 1))
    else (stageLocation, "")

  private[snowflake] final def TEMP_STAGE_LOCATION: String =
    "spark_connector_unload_stage_" + (Random.alphanumeric take 10 mkString "")

  private[snowflake] final def createS3Client(
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

  private[snowflake] final def getDecryptedStream(
      stream: InputStream,
      masterKey: String,
      meta: ObjectMetadata): InputStream = {

    val metaData   = meta.getUserMetadata
    val decodedKey = Base64.decode(masterKey)
    val (key, iv)  = (metaData.get(AMZ_KEY), metaData.get(AMZ_IV))

    if (key == null || iv == null)
      throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR,
                                      ErrorCode.INTERNAL_ERROR.getMessageCode,
                                      "File " + "metadata incomplete")

    val keyBytes: Array[Byte] = Base64.decode(key)
    val ivBytes: Array[Byte]  = Base64.decode(iv)

    val queryStageMasterKey: SecretKey =
      new SecretKeySpec(decodedKey, 0, decodedKey.length, AES)

    val keyCipher: Cipher = Cipher.getInstance(KEY_CIPHER)
    keyCipher.init(Cipher.DECRYPT_MODE, queryStageMasterKey)

    val fileKeyBytes: Array[Byte] = keyCipher.doFinal(keyBytes) // NB: we assume qsmk
    // .length == fileKey.length
    //     (fileKeyBytes.length may be bigger due to padding)
    val fileKey =
      new SecretKeySpec(fileKeyBytes, 0, decodedKey.length, AES)

    val dataCipher           = Cipher.getInstance(DATA_CIPHER)
    val ivy: IvParameterSpec = new IvParameterSpec(ivBytes)
    dataCipher.init(Cipher.DECRYPT_MODE, fileKey, ivy)
    new CipherInputStream(stream, dataCipher)
  }

  private[snowflake] final def getCipherAndMetadata(
      masterKey: String,
      queryId: String,
      smkId: String): (Cipher, ObjectMetadata) = {

    val decodedKey   = Base64.decode(masterKey)
    val keySize      = decodedKey.length
    val fileKeyBytes = new Array[Byte](keySize)
    val fileCipher   = Cipher.getInstance(DATA_CIPHER)
    val blockSz      = fileCipher.getBlockSize
    val ivData       = new Array[Byte](blockSz)

    val secRnd = SecureRandom.getInstance("SHA1PRNG", "SUN")
    secRnd.nextBytes(new Array[Byte](10))

    secRnd.nextBytes(ivData)
    val iv = new IvParameterSpec(ivData)

    secRnd.nextBytes(fileKeyBytes)
    val fileKey = new SecretKeySpec(fileKeyBytes, 0, keySize, AES)

    fileCipher.init(Cipher.ENCRYPT_MODE, fileKey, iv)

    val keyCipher           = Cipher.getInstance(KEY_CIPHER)
    val queryStageMasterKey = new SecretKeySpec(decodedKey, 0, keySize, AES)

    // Init cipher
    keyCipher.init(Cipher.ENCRYPT_MODE, queryStageMasterKey)
    val encKeK = keyCipher.doFinal(fileKeyBytes)

    val matDesc =
      new MatDesc(smkId.toLong, queryId, keySize * 8)

    val meta = new ObjectMetadata()

    meta.addUserMetadata(AMZ_MATDESC, matDesc.toString)
    meta.addUserMetadata(AMZ_KEY, Base64.encodeAsString(encKeK: _*))
    meta.addUserMetadata(AMZ_IV, Base64.encodeAsString(ivData: _*))

    (fileCipher, meta)
  }
}

private[snowflake] class S3Internal(isWrite: Boolean,
                                                 jdbcWrapper: JDBCWrapper,
                                                 params: MergedParameters) {

  import S3Internal._

  private lazy val connection: SnowflakeConnectionV1 =
    jdbcWrapper.getConnector(params).asInstanceOf[SnowflakeConnectionV1]

  private lazy val sfAgent = {
    Utils.setLastPutCommand(command)
    Utils.setLastGetCommand(command)

    if (!stageSet) setupStageArea()
    new SnowflakeFileTransferAgent(command,
                                   connection.getSfSession,
                                   new SFStatement(connection.getSfSession))
  }

  private lazy val encryptionMaterials = sfAgent.getEncryptionMaterial
  private lazy val srcMaterialsMap =
    if (!isWrite) sfAgent.getSrcToMaterialsMap else null
  private lazy val stageCredentials = sfAgent.getStageCredentials

  private lazy val encMat =
    if (encryptionMaterials.size() > 0) {
      encryptionMaterials.get(0)
    } else null

  private[snowflake] lazy val awsId: String =
    stageCredentials.get("AWS_ID").toString
  private[snowflake] lazy val awsKey: String =
    stageCredentials.get("AWS_KEY").toString
  private[snowflake] lazy val awsToken: String =
    stageCredentials.get("AWS_TOKEN").toString
  private[snowflake] lazy val stageLocation: String =
    sfAgent.getStageLocation

  private[snowflake] lazy val getKeyIds: Seq[(String, String, String)] = {
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

  private[snowflake] lazy val masterKey =
    if (encMat != null) encMat.getQueryStageMasterKey else null
  private[snowflake] lazy val decodedKey =
    if (masterKey != null) Base64.decode(masterKey) else null

  private[snowflake] lazy val is256Encryption: Boolean = {
    val length = if (decodedKey != null) decodedKey.length * 8 else 128
    if (length == 256)
      true
    else if (length == 128)
      false
    else
      throw new SnowflakeConnectorException(s"Unsupported Key Size: $length")
  }

  private val tempStage     = TEMP_STAGE_LOCATION
  private val dummyLocation = DUMMY_LOCATION

  private val command = {
    val comm =
      if (isWrite)
        s"PUT $dummyLocation @$tempStage"
      else
        s"GET @$tempStage $dummyLocation"

    if (params.parallelism.isDefined) {
      comm + s" PARALLEL=${params.parallelism.get}"
    }
    else
      comm
  }

  private var stageSet: Boolean = false

  private[snowflake] def closeConnection(): Unit = {
    connection.close()
  }

  private[snowflake] def getEncryptionMaterials = encryptionMaterials

  private[snowflake] def setupStageArea(): String = {

    jdbcWrapper.executeInterruptibly(connection,
                                          CREATE_TEMP_STAGE_STMT + tempStage)

    stageSet = true
    tempStage
  }

  private[snowflake] def executeWithConnection(
      connectionFunc: (Connection => Any)) = {
    connectionFunc(connection)
  }
}
