package net.snowflake.spark.snowflake

import java.io.IOException
import javax.crypto.spec.SecretKeySpec

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
import net.snowflake.client.jdbc.internal.snowflake.common.core.{
  S3FileEncryptionMaterial,
  SqlState
}
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag

private[snowflake] class SnowflakeRDDPartition(
    val srcFiles: List[(String, S3FileEncryptionMaterial)],
    val rddId: Int,
    val index: Int)
    extends Partition {

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

private[snowflake] class SnowflakeRDD[T: ClassTag](
    sc: SparkContext,
    resultSchema: StructType,
    sfConnection: SnowflakeConnectionV1,
    tempStage: String)
    extends RDD[T](sc, Nil) {
  import SnowflakeRDD._

  private final val GET_COMMAND =
    s"GET $tempStage $DUMMY_LOCATION"

  private val (encryptionMaterials, stageCredentials, stageLocation) = try {
    val sfAgent =
      new SnowflakeFileTransferAgent(
        GET_COMMAND,
        sfConnection.getSfSession,
        new SFStatement(sfConnection.getSfSession))
    (sfAgent.getEncryptionMaterials,
     sfAgent.getStageCredentials,
     sfAgent.getStageLocation)
  } finally {
    sfConnection.close()
  }

  override def getPartitions: Array[Partition] = {

    val partitions = new Array[Partition](encryptionMaterials.size())
    val it         = encryptionMaterials.entrySet().iterator()

    var i = 0

    // TODO: Split file list for partitions evenly instead of one each.
    while (it.hasNext) {
      val next = it.next
      partitions(i) =
        new SnowflakeRDDPartition(List((next.getKey, next.getValue)), id, i)
      i = i + 1
    }
    partitions
  }

  override def compute(thePartition: Partition,
                       context: TaskContext): Iterator[T] = {

    val converter = Conversions.createRowConverter[T](resultSchema)

    val mats   = thePartition.asInstanceOf[SnowflakeRDDPartition].srcFiles
    val reader = new SnowflakeRecordReader

    mats.foreach {
      case (file, material) =>
        val decodedKey = Base64.decode(mats.head._2.getQueryStageMasterKey)

        val encryptionKeySize = decodedKey.length * 8

        val awsID    = stageCredentials.get("AWS_ID").toString
        val awsKey   = stageCredentials.get("AWS_KEY").toString
        val awsToken = stageCredentials.get("AWS_TOKEN").toString

        val awsCredentials =
          if (awsToken != null)
            new BasicSessionCredentials(awsID, awsKey, awsToken)
          else new BasicAWSCredentials(awsID, awsKey)

        val clientConfig = new ClientConfiguration
        clientConfig.setMaxConnections(DEFAULT_PARALLELISM)
        clientConfig.setMaxErrorRetry(S3_MAX_RETRIES)

        if (material != null) {
          var amazonClient: AmazonS3Client = null
          if (encryptionKeySize == 256) {
            val queryStageMasterKey =
              new SecretKeySpec(decodedKey, 0, decodedKey.length, AES)
            val encryptionMaterials =
              new EncryptionMaterials(queryStageMasterKey)
            encryptionMaterials.addDescription("queryId", material.getQueryId)
            encryptionMaterials.addDescription("smkId",
                                               material.getSmkId.toString)
            val cryptoConfig =
              new CryptoConfiguration(CryptoMode.EncryptionOnly)
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
          reader.addStream(dataObject.getObjectContent)
        }
    }

    val iter = new Iterator[T] {

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

    }
    new InterruptibleIterator(context, iter)

  }
}

private[snowflake] object SnowflakeRDD {
  final val DUMMY_LOCATION      = "/tmp/dummy_location_spark_connector_tmp/"
  final val AES                 = "AES"
  final val DEFAULT_PARALLELISM = 10
  final val S3_MAX_RETRIES      = 3

  /**
    * A small helper for extracting bucket name and path from stage location.
    *
    * @param stageLocation stage location
    * @return s3 location
    */
  def extractBucketNameAndPath(stageLocation: String): (String, String) = {
    var bucketName = stageLocation
    var s3path     = ""

    // split stage location as bucket name and path
    if (stageLocation.contains("/")) {
      bucketName = stageLocation.substring(0, stageLocation.indexOf("/"))
      s3path = stageLocation.substring(stageLocation.indexOf("/") + 1)
    }

    (bucketName, s3path)
  }
}
