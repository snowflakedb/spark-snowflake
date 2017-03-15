package net.snowflake.spark.snowflake

import javax.crypto.spec.SecretKeySpec

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.{BasicAWSCredentials, BasicSessionCredentials}
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3EncryptionClient}
import com.amazonaws.services.s3.model.{CryptoConfiguration, CryptoMode, EncryptionMaterials, StaticEncryptionMaterialsProvider}
import com.amazonaws.util.Base64
import net.snowflake.client.core.SFStatement
import net.snowflake.client.jdbc.internal.snowflake.common.core.{S3FileEncryptionMaterial, SqlState}
import net.snowflake.client.jdbc._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.{CombineFileSplit, FileSplit}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ShutdownHookManager

import scala.reflect.ClassTag

private[snowflake] class SnowflakeRDDPartition(
    val srcFiles: List[String],
    val encMats: List[S3FileEncryptionMaterial],
    val rddId: Int,
    val index: Int)
    extends Partition {

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

/**
  * :: DeveloperApi ::
  * An RDD that provides core functionality for reading data stored in Hadoop (e.g., files in HDFS,
  * sources in HBase, or S3), using the new MapReduce API (`org.apache.hadoop.mapreduce`).
  *
  * @param sc The SparkContext to associate the RDD with.
  *
  * @note Instantiating this class directly is not recommended, please use
  * `org.apache.spark.SparkContext.newAPIHadoopRDD()`
  */
@DeveloperApi
private[snowflake] class SnowflakeRDD[T](sc: SparkContext,
                                         sfConnection: SnowflakeConnectionV1,
                                         tempStage: String)
    extends RDD[T](sc, Nil) {
  import SnowflakeRDD.{AES, DUMMY_LOCATION, DEFAULT_PARALLELISM, S3_MAX_RETRIES, extractBucketNameAndPath}

  private final val GET_COMMAND =
    s"GET $tempStage $DUMMY_LOCATION"

  private val (encryptionMaterials, stageCredentials, stageLocation) = try {
    val sfAgent =
    new SnowflakeFileTransferAgent(
      GET_COMMAND,
      sfConnection.getSfSession,
      new SFStatement(sfConnection.getSfSession))
    (sfAgent.getEncryptionMaterials, sfAgent.getStageCredentials, sfAgent.getStageLocation)
  } finally {
    sfConnection.close()
  }

  override def getPartitions: Array[Partition] = {

    val partitions = new Array[Partition](encryptionMaterials.size())

    val it = encryptionMaterials.entrySet().iterator()

    var i = 0

    while (it.hasNext) {
      val next = it.next
      partitions(i) = new SnowflakeRDDPartition(List(next.getKey),
                                                List(next.getValue),
                                                id,
                                                i)
      i = i + 1
    }
    partitions
  }

  override def compute(thePartition: Partition,
     context: TaskContext):Iterator[T] = {

    val mats = thePartition.asInstanceOf[SnowflakeRDDPartition].encMats
    val files = thePartition.asInstanceOf[SnowflakeRDDPartition].srcFiles
    val decodedKey = Base64.decode(mats.head.getQueryStageMasterKey)

    val encryptionKeySize = decodedKey.length*8

    //this.encMat = encMat;
   // clientConfig.withSignerOverride("AWSS3V4SignerType");

    val awsID = stageCredentials.get("AWS_ID").toString
    val awsKey = stageCredentials.get("AWS_KEY").toString
    val awsToken = stageCredentials.get("AWS_TOKEN").toString

    val awsCredentials = if (awsToken != null)
      new BasicSessionCredentials(awsID, awsKey, awsToken)
                         else new BasicAWSCredentials(awsID, awsKey)

    val clientConfig = new ClientConfiguration;
    clientConfig.setMaxConnections(DEFAULT_PARALLELISM);
    clientConfig.setMaxErrorRetry(S3_MAX_RETRIES);

      val items = files.zip(mats)

    items.foreach { case (file, material) => {
      if (material != null) {
        var amazonClient: AmazonS3Client = null
        if (encryptionKeySize == 256) {
          val queryStageMasterKey = new SecretKeySpec(decodedKey, 0, decodedKey.length, AES)
          val encryptionMaterials = new EncryptionMaterials(queryStageMasterKey)
          encryptionMaterials.addDescription("queryId", material.getQueryId)
          encryptionMaterials.addDescription("smkId", material.getSmkId.toString)
          val cryptoConfig = new CryptoConfiguration(CryptoMode.EncryptionOnly)
          amazonClient = new AmazonS3EncryptionClient(awsCredentials, new StaticEncryptionMaterialsProvider(encryptionMaterials), clientConfig, cryptoConfig);

        } else if (encryptionKeySize == 128) {
          amazonClient = new AmazonS3Client(awsCredentials, clientConfig);
        } else {
          throw new SnowflakeSQLException(SqlState.INTERNAL_ERROR, ErrorCode.INTERNAL_ERROR.getMessageCode(), "unsupported key size", encryptionKeySize);
        }

        val (bucketName, stagePath) = extractBucketNameAndPath(stageLocation);

    var stageFilePath = file

    if (!stagePath.isEmpty())
    {
      stageFilePath = SnowflakeUtil.concatFilePathNames(stagePath,
        file, "/");

      }
        amazonClient.getObject(bucketName, stageFilePath)
    }
    }


    val iter = new Iterator[(K, V)] {
      private val split = theSplit.asInstanceOf[SnowflakeRDDPartition]
      logInfo("Input split: " + split.serializableHadoopSplit)
      private val conf = getConf

      private val inputMetrics      = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // Sets the thread local variable for the file's name
      split.serializableHadoopSplit.value match {
        case fs: FileSplit =>
          InputFileNameHolder.setInputFileName(fs.getPath.toString)
        case _ => InputFileNameHolder.unsetInputFileName()
      }

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // creating RecordReader, because RecordReader's constructor might read some bytes
      private val getBytesReadCallback: Option[() => Long] =
        split.serializableHadoopSplit.value match {
          case _: FileSplit | _: CombineFileSplit =>
            SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()
          case _ => None
        }

      // For Hadoop 2.5+, we get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def updateBytesRead(): Unit = {
        getBytesReadCallback.foreach { getBytesRead =>
          inputMetrics.setBytesRead(existingBytesRead + getBytesRead())
        }
      }

      private val format = inputFormatClass.newInstance
      format match {
        case configurable: Configurable =>
          configurable.setConf(conf)
        case _ =>
      }
      private val attemptId =
        new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
      private val hadoopAttemptContext =
        new TaskAttemptContextImpl(conf, attemptId)
      private var finished = false
      private var reader =
        try {
          val _reader = format.createRecordReader(
            split.serializableHadoopSplit.value,
            hadoopAttemptContext)
          _reader.initialize(split.serializableHadoopSplit.value,
                             hadoopAttemptContext)
          _reader
        } catch {
          case e: IOException if ignoreCorruptFiles =>
            logWarning(
              s"Skipped the rest content in the corrupted file: ${split.serializableHadoopSplit}",
              e)
            finished = true
            null
        }

      // Register an on-task-completion callback to close the input stream.
      context.addTaskCompletionListener(context => close())
      private var havePair                  = false
      private var recordsSinceMetricsUpdate = 0

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          try {
            finished = !reader.nextKeyValue
          } catch {
            case e: IOException if ignoreCorruptFiles =>
              logWarning(
                s"Skipped the rest content in the corrupted file: ${split.serializableHadoopSplit}",
                e)
              finished = true
          }
          if (finished) {
            // Close and release the reader here; close() will also be called when the task
            // completes, but for tasks that read from many files, it helps to release the
            // resources early.
            close()
          }
          havePair = !finished
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        if (!finished) {
          inputMetrics.incRecordsRead(1)
        }
        if (inputMetrics.recordsRead % SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
          updateBytesRead()
        }
        (reader.getCurrentKey, reader.getCurrentValue)
      }

      private def close() {
        if (reader != null) {
          InputFileNameHolder.unsetInputFileName()
          // Close the reader and release it. Note: it's very important that we don't close the
          // reader more than once, since that exposes us to MAPREDUCE-5918 when running against
          // Hadoop 1.x and older Hadoop 2.x releases. That bug can lead to non-deterministic
          // corruption issues when reading compressed input.
          try {
            reader.close()
          } catch {
            case e: Exception =>
              if (!ShutdownHookManager.inShutdown()) {
                logWarning("Exception in RecordReader.close()", e)
              }
          } finally {
            reader = null
          }
          if (getBytesReadCallback.isDefined) {
            updateBytesRead()
          } else if (split.serializableHadoopSplit.value
                       .isInstanceOf[FileSplit] ||
                     split.serializableHadoopSplit.value
                       .isInstanceOf[CombineFileSplit]) {
            // If we can't get the bytes read from the FS stats, fall back to the split size,
            // which may be inaccurate.
            try {
              inputMetrics.incBytesRead(
                split.serializableHadoopSplit.value.getLength)
            } catch {
              case e: java.io.IOException =>
                logWarning(
                  "Unable to get input size to set InputMetrics for task",
                  e)
            }
          }
        }
      }
    }
    new InterruptibleIterator(context, iter)
  }

  /** Maps over a partition, providing the InputSplit that was used as the base of the partition. */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
      f: (InputSplit, Iterator[(K, V)]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    new NewHadoopMapPartitionsWithSplitRDD(this, f, preservesPartitioning)
  }

  override def getPreferredLocations(hsplit: Partition): Seq[String] = {
    val split =
      hsplit.asInstanceOf[SnowflakeRDDPartition].serializableHadoopSplit.value
    val locs = HadoopRDD.SPLIT_INFO_REFLECTIONS match {
      case Some(c) =>
        try {
          val infos =
            c.newGetLocationInfo.invoke(split).asInstanceOf[Array[AnyRef]]
          HadoopRDD.convertSplitLocationInfo(infos)
        } catch {
          case e: Exception =>
            logDebug("Failed to use InputSplit#getLocationInfo.", e)
            None
        }
      case None => None
    }
    locs.getOrElse(split.getLocations.filter(_ != "localhost"))
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning(
        "Caching NewHadoopRDDs as deserialized objects usually leads to undesired" +
          " behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
          " Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

}


}


  private[snowflake] object SnowflakeRDD {
  final val DUMMY_LOCATION = "/tmp/dummy_location_spark_connector_tmp/"
    final val AES = "AES"
    final val DEFAULT_PARALLELISM = 10
    final val S3_MAX_RETRIES = 3

    /**
      * A small helper for extracting bucket name and path from stage location.
      *
      * @param stageLocation stage location
      * @return s3 location
      */
    def extractBucketNameAndPath(stageLocation: String) : (String, String) =
    {
      var bucketName = stageLocation;
      var s3path = "";

      // split stage location as bucket name and path
      if (stageLocation.contains("/"))
      {
        bucketName = stageLocation.substring(0, stageLocation.indexOf("/"));
        s3path = stageLocation.substring(stageLocation.indexOf("/")+1);
      }

      return (bucketName, s3path)
    }

    }


  /*

private[spark] object SnowflakeRDD {

  /**
    * Configuration's constructor is not threadsafe (see SPARK-1097 and HADOOP-10456).
    * Therefore, we synchronize on this lock before calling new Configuration().
    */
  val CONFIGURATION_INSTANTIATION_LOCK = new Object()

  /**
    * Analogous to [[org.apache.spark.rdd.MapPartitionsRDD]], but passes in an InputSplit to
    * the given function rather than the index of the partition.
    */
  private[spark] class NewHadoopMapPartitionsWithSplitRDD[U: ClassTag,
                                                          T: ClassTag](
      prev: RDD[T],
      f: (InputSplit, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false)
      extends RDD[U](prev) {

    override val partitioner =
      if (preservesPartitioning) firstParent[T].partitioner else None

    override def getPartitions: Array[Partition] = firstParent[T].partitions

    override def compute(split: Partition, context: TaskContext): Iterator[U] = {
      val partition  = split.asInstanceOf[SnowflakeRDDPartition]
      val inputSplit = partition.serializableHadoopSplit.value
      f(inputSplit, firstParent[T].iterator(split, context))
    }
  }
  */
}
