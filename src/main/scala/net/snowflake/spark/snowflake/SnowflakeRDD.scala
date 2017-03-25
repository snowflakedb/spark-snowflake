package net.snowflake.spark.snowflake

import java.io.IOException
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import javax.crypto.{Cipher, SecretKey}

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{BasicAWSCredentials, BasicSessionCredentials}
import com.amazonaws.services.s3.model.{
  CryptoConfiguration,
  CryptoMode,
  EncryptionMaterials,
  StaticEncryptionMaterialsProvider
}
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3EncryptionClient}
import com.amazonaws.util.Base64
import net.snowflake.client.core.SFStatement
import net.snowflake.client.jdbc._
import net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag
import scala.util.Random

private[snowflake] class SnowflakeRDDPartition(
    val srcFiles: List[(java.lang.String, java.lang.String, java.lang.Long)],
    val rddId: Int,
    val index: Int)
    extends Partition {

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

private[snowflake] object SnowflakeRDD {
  private final val DUMMY_LOCATION =
    "file:///tmp/dummy_location_spark_connector_tmp/"
  private final val AES                 = "AES"
  private final val DEFAULT_PARALLELISM = 10
  private final val S3_MAX_RETRIES      = 3
  private final val CREATE_TEMP_STAGE_STMT =
    s"""CREATE OR REPLACE TEMP STAGE """
  private final val AMZ_KEY: String     = "x-amz-key"
  private final val AMZ_IV: String      = "x-amz-iv"
  private final val DATA_CIPHER: String = "AES/CBC/PKCS5Padding"
  private final val KEY_CIPHER: String  = "AES/ECB/PKCS5Padding"

  /**
    * A small helper for extracting bucket name and path from stage location.
    *
    * @param stageLocation stage location
    * @return s3 location
    */
  private final def extractBucketNameAndPath(
      stageLocation: String): (String, String) = {
    var bucketName = stageLocation
    var s3path     = ""

    // split stage location as bucket name and path
    if (stageLocation.contains("/")) {
      bucketName = stageLocation.substring(0, stageLocation.indexOf("/"))
      s3path = stageLocation.substring(stageLocation.indexOf("/") + 1)
    }

    (bucketName, s3path)
  }

  private final def TEMP_STAGE_LOCATION: String =
    "spark_connector_unload_stage_" + (Random.alphanumeric take 10 mkString "")
}

private[snowflake] class SnowflakeRDD[T: ClassTag](
    @transient val sqlContext: SQLContext,
    @transient val jdbcWrapper: JDBCWrapper,
    @transient val params: MergedParameters,
    @transient val sql: String,
    resultSchema: StructType
) extends RDD[T](sqlContext.sparkContext, Nil) {
  import SnowflakeRDD._

  @transient private val tempStage = TEMP_STAGE_LOCATION
  @transient private val GET_COMMAND =
    s"GET @$tempStage $DUMMY_LOCATION"
  @transient private val connection: SnowflakeConnectionV1 =
    jdbcWrapper.getConnector(params) match {
      case conn: SnowflakeConnectionV1 => conn
      case _                           => throw new SnowflakeConnectorException("JDBC Connection Error.")
    }

  @transient private val unloadSql = buildUnloadStmt(sql)

  setup()

  @transient private val sfAgent = new SnowflakeFileTransferAgent(
    GET_COMMAND,
    connection.getSfSession,
    new SFStatement(connection.getSfSession))

  @transient private val encryptionMaterials = sfAgent.getEncryptionMaterials
  @transient private val stageCredentials    = sfAgent.getStageCredentials

  private val stageLocation = sfAgent.getStageLocation

  private val awsID    = stageCredentials.get("AWS_ID").toString
  private val awsKey   = stageCredentials.get("AWS_KEY").toString
  private val awsToken = stageCredentials.get("AWS_TOKEN").toString
  private val compress = if (params.sfCompress) "GZIP" else null

  private val masterKey: String = if (encryptionMaterials.size() > 0) {
    encryptionMaterials
      .entrySet()
      .iterator()
      .next()
      .getValue
      .getQueryStageMasterKey
  } else ""

  override def getPartitions: Array[Partition] = {

    val partitions = new Array[Partition](encryptionMaterials.size())
    val it         = encryptionMaterials.entrySet().iterator()

    var i = 0

    // TODO: Split file list for partitions evenly instead of one each.
    while (it.hasNext) {
      val next  = it.next
      val key   = next.getKey
      val value = next.getValue
      partitions(i) = new SnowflakeRDDPartition(
        List(
          (key,
           if (value != null) value.getQueryId else null,
           if (value != null) value.getSmkId else null)),
        id,
        i)
      i = i + 1
    }
    partitions
  }

  override def compute(thePartition: Partition,
                       context: TaskContext): Iterator[T] = {

    val converter = Conversions.createRowConverter[T](resultSchema)

    val mats       = thePartition.asInstanceOf[SnowflakeRDDPartition].srcFiles
    val decodedKey = Base64.decode(masterKey)
    val reader     = new SnowflakeRecordReader

    reader.setCompressionType(compress)

    val awsCredentials =
      if (awsToken != null)
        new BasicSessionCredentials(awsID, awsKey, awsToken)
      else new BasicAWSCredentials(awsID, awsKey)

    val encryptionKeySize = decodedKey.length * 8

    val clientConfig = new ClientConfiguration

    clientConfig.setMaxConnections(DEFAULT_PARALLELISM)
    clientConfig.setMaxErrorRetry(S3_MAX_RETRIES)
    val queryStageMasterKey: SecretKey =
      new SecretKeySpec(decodedKey, 0, decodedKey.length, AES)

    val cryptoConfig =
      new CryptoConfiguration(CryptoMode.EncryptionOnly)

    val keyCipher: Cipher = Cipher.getInstance(KEY_CIPHER)

    keyCipher.init(Cipher.DECRYPT_MODE, queryStageMasterKey)

    mats.foreach {
      case (file, queryId, smkId) =>
        if (queryId != null) {
          var amazonClient: AmazonS3Client = null
          if (encryptionKeySize == 256) {
            val encryptionMaterials =
              new EncryptionMaterials(queryStageMasterKey)
            encryptionMaterials.addDescription("queryId", queryId)
            encryptionMaterials.addDescription("smkId", smkId.toString)

            amazonClient = new AmazonS3EncryptionClient(
              awsCredentials,
              new StaticEncryptionMaterialsProvider(encryptionMaterials),
              clientConfig,
              cryptoConfig)

          } else if (encryptionKeySize == 128) {
            amazonClient = new AmazonS3Client(awsCredentials, clientConfig)
          } else {
            throw new SnowflakeConnectorException(
              "Unsupported encryption-key size.")
          }

          val (bucketName, stagePath) = extractBucketNameAndPath(stageLocation)

          var stageFilePath = file

          if (!stagePath.isEmpty) {
            stageFilePath =
              SnowflakeUtil.concatFilePathNames(stagePath, file, "/")
          }

          val dataObject = amazonClient.getObject(bucketName, stageFilePath)
          val metaData   = dataObject.getObjectMetadata.getUserMetadata
          val (key, iv)  = (metaData.get(AMZ_KEY), metaData.get(AMZ_IV))

          if (key == null || iv == null)
            throw new SnowflakeSQLException(
              SqlState.INTERNAL_ERROR,
              ErrorCode.INTERNAL_ERROR.getMessageCode,
              "File " +
                "metadata incomplete")

          val keyBytes: Array[Byte] = Base64.decode(key)
          val ivBytes: Array[Byte]  = Base64.decode(iv)

          val fileKeyBytes: Array[Byte] = keyCipher.doFinal(keyBytes) // NB: we assume qsmk
          // .length == fileKey.length
          //     (fileKeyBytes.length may be bigger due to padding)
          val fileKey =
            new SecretKeySpec(fileKeyBytes, 0, decodedKey.length, AES)

          val dataCipher: Cipher   = Cipher.getInstance(DATA_CIPHER)
          val ivy: IvParameterSpec = new IvParameterSpec(ivBytes)
          dataCipher.init(Cipher.DECRYPT_MODE, fileKey, ivy)

          // TODO: Ciphers need to be on a per file basis, not a per reader basis!!!
          reader.setCipher(dataCipher)
          reader.addStream(dataObject.getObjectContent)
        }
    }

    new InterruptibleIterator(context, new Iterator[T] {

      private var finished = false
      private var havePair = false

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          try {
            finished = !reader.nextKeyValue
          } catch {
            case e: IOException =>
              finished = true
          }

          havePair = !finished
        }
        !finished
      }

      override def next(): T = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false

        converter(reader.getCurrentValue)
      }
    })
  }

  override def finalize(): Unit = {
    connection.close()
    super.finalize()
  }

  private def setup(): Unit = {
    // Prologue
    val prologueSql = Utils.genPrologueSql(params)
    log.debug(Utils.sanitizeQueryText(prologueSql))
    jdbcWrapper.executeInterruptibly(connection, prologueSql)

    Utils.executePreActions(jdbcWrapper, connection, params)

    // Run the unload query
    log.debug(Utils.sanitizeQueryText(unloadSql))
    jdbcWrapper.executeQueryInterruptibly(connection,
                                          CREATE_TEMP_STAGE_STMT + tempStage)
    val res = jdbcWrapper.executeQueryInterruptibly(connection, unloadSql)

    // Verify it's the expected format
    val sch = res.getMetaData
    assert(sch.getColumnCount == 3)
    assert(sch.getColumnName(1) == "rows_unloaded")
    assert(sch.getColumnTypeName(1) == "NUMBER")
    // First record must be in
    val first = res.next()
    assert(first)
    val numRows = res.getInt(1)
    // There can be no more records
    val second = res.next()
    assert(!second)

    Utils.executePostActions(jdbcWrapper, connection, params)
  }

  private def buildUnloadStmt(query: String): String = {

    val credsString =
      AWSCredentialsUtils.getSnowflakeCredentialsString(sqlContext, params)

    // Save the last SELECT so it can be inspected
    Utils.setLastSelect(query)

    // Determine the compression type
    val compressionString = if (params.sfCompress) "gzip" else "none"

    s"""
       |COPY INTO @$tempStage
       |FROM ($query)
       |$credsString
       |FILE_FORMAT = (
       |    TYPE=CSV
       |    COMPRESSION='$compressionString'
       |    FIELD_DELIMITER='|'
       |    /*ESCAPE='\\\\'*/
       |    /*TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM'*/
       |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
       |    NULL_IF= ()
       |  )
       |MAX_FILE_SIZE = ${params.s3maxfilesize}
       |""".stripMargin.trim
  }
}
