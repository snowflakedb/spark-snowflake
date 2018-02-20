package net.snowflake.spark.snowflake

import java.io.{IOException, InputStream}
import java.util.zip.GZIPInputStream

import net.snowflake.client.jdbc._
import net.snowflake.spark.snowflake.ConnectorSFStageManager._
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

private[snowflake] class SnowflakeRDDPartition(
    val srcFiles: Seq[(java.lang.String, java.lang.String, java.lang.String)],
    val rddId: Int,
    val index: Int)
    extends Partition {

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

private[snowflake] class SnowflakeRDD[T: ClassTag](
    @transient val sqlContext: SQLContext,
    @transient val jdbcWrapper: JDBCWrapper,
    @transient val params: MergedParameters,
    @transient val sql: String,
    resultSchema: StructType
) extends RDD[T](sqlContext.sparkContext, Nil)
    with DataUnloader {

  @transient override val log = LoggerFactory.getLogger(getClass)
  @transient private val stageManager =
    new ConnectorSFStageManager(false, jdbcWrapper, params)

  @transient val tempStage = stageManager.setupStageArea()

  @transient private val FILES_PER_PARTITION = 2

  private val compress = if (params.sfCompress) "gzip" else "none"
  private val fileType = params.sfFileType
  private val parallel = params.parallelism

  private[snowflake] val rowCount = stageManager
    .executeWithConnection({ c =>
      setup(preStatements = Seq.empty,
            sql = buildUnloadStmt(sql, s"@$tempStage", compress, None, params.sfFileType),
            conn = c,
            keepOpen = true)
    })
    .asInstanceOf[Long]

  private val stageLocation = stageManager.stageLocation
  private val awsID         = stageManager.awsId
  private val awsKey        = stageManager.awsKey
  private val awsToken      = stageManager.awsToken
  private val masterKey     = stageManager.masterKey
  private val is256         = stageManager.is256Encryption

  override def getPartitions: Array[Partition] = {

    val encryptionMaterialsGrouped =
      stageManager.getKeyIds.grouped(FILES_PER_PARTITION).toList

    val partitions = new Array[Partition](encryptionMaterialsGrouped.length)

    var i = 0

    while (i < encryptionMaterialsGrouped.length) {
      partitions(i) =
        new SnowflakeRDDPartition(encryptionMaterialsGrouped(i), id, i)
      i = i + 1
    }
    partitions
  }

  override def compute(thePartition: Partition,
                       context: TaskContext): Iterator[T] = {



    val mats   = thePartition.asInstanceOf[SnowflakeRDDPartition].srcFiles


    fileType match {
      case "csv" => {
        val reader = new SnowflakeRecordReader
        val converter = Conversions.createRowConverter[T](resultSchema)

        try {
          mats.foreach {
            case (file, queryId, smkId) =>
              if (queryId != null) {
                var stream: InputStream = null

                val amazonClient =
                  createS3Client(is256,
                    masterKey,
                    queryId,
                    smkId,
                    awsID,
                    awsKey,
                    awsToken,
                    parallel)

                val (bucketName, stagePath) =
                  extractBucketNameAndPath(stageLocation)

                var stageFilePath = file

                if (!stagePath.isEmpty) {
                  stageFilePath =
                    SnowflakeUtil.concatFilePathNames(stagePath, file, "/")
                }

                val dataObject = amazonClient.getObject(bucketName, stageFilePath)
                stream = dataObject.getObjectContent

                if (!is256) {
                  stream = getDecryptedStream(stream,
                    masterKey,
                    dataObject.getObjectMetadata)
                }

                stream = compress match {
                  case "gzip" => new GZIPInputStream(stream)
                  case _      => stream
                }

                reader.addStream(stream)
              }
          }
        } catch {
          case ex: Exception =>
            reader.closeAll()
            SnowflakeConnectorUtils.handleS3Exception(ex)
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
      case "parquet" => {
        val reader = new SnowflakeParquetRecordReader
        val converter = ParquetConversions.createRowConverter[T](resultSchema)

        try {
          mats.foreach {
            case (file, queryId, smkId) =>
              if (queryId != null) {
                var stream: InputStream = null

                val amazonClient =
                  createS3Client(is256,
                    masterKey,
                    queryId,
                    smkId,
                    awsID,
                    awsKey,
                    awsToken,
                    parallel)

                val (bucketName, stagePath) =
                  extractBucketNameAndPath(stageLocation)

                var stageFilePath = file

                if (!stagePath.isEmpty) {
                  stageFilePath =
                    SnowflakeUtil.concatFilePathNames(stagePath, file, "/")
                }

                val dataObject = amazonClient.getObject(bucketName, stageFilePath)
                stream = dataObject.getObjectContent

                val inputStreamLength = dataObject.getObjectMetadata.getContentLength // size before decryption


                if (!is256) {
                  stream = getDecryptedStream(stream,
                    masterKey,
                    dataObject.getObjectMetadata)
                }

                stream = compress match {
                  // case "snappy" => new SnappyInputStream(stream)
                  case _      => stream
                }
                reader.addStream(stream, inputStreamLength)
              }
          }
        } catch {
          case ex: Exception =>
            reader.closeAll()
            SnowflakeConnectorUtils.handleS3Exception(ex)
        }

        new InterruptibleIterator(context, new Iterator[T] {

          var times = 0
          var totalTime: Long = 0l

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

            val result = converter(reader.getCurrentValue)

            result
          }
        })
      }
    }


  }

  override def finalize(): Unit = {
    stageManager.closeConnection()
    super.finalize()
  }
}
