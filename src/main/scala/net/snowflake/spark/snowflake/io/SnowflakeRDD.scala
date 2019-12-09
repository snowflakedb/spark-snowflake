package net.snowflake.spark.snowflake.io

import java.io.InputStream

import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD


class SnowflakeRDD(
                    sc: SparkContext,
                    fileNames: List[String],
                    format: SupportedFormat,
                    downloadFile: (String) => InputStream,
                    expectedPartitionCount: Int
                  ) extends RDD[String](sc, Nil) {

  @transient private val MIN_FILES_PER_PARTITION = 2

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val stringIterator = new SFRecordReader(format)
    stringIterator.setDownloadFunction(downloadFile)

    split.asInstanceOf[SnowflakePartition].fileNames
      .foreach(name => {
        stringIterator.addFileName(name)
      })
    stringIterator
  }

  override protected def getPartitions: Array[Partition] = {
    val fileCountPerPartition =
      Math.max(MIN_FILES_PER_PARTITION,
        (fileNames.length + expectedPartitionCount / 2) / expectedPartitionCount)
    val fileCount = fileNames.length
    val partitionCount = (fileCount + fileCountPerPartition - 1) / fileCountPerPartition
    logger.info(s"""SnowflakeRDD.getPartitions statistic:
         | fileCount=$fileCount filePerPartition=$fileCountPerPartition
         | actualPartitionCount=$partitionCount
         | expectedPartitionCount=$expectedPartitionCount
         |""".stripMargin.filter(_ >= ' '))

    fileNames.grouped(fileCountPerPartition).zipWithIndex.map {
      case (names, index) => SnowflakePartition(names, id, index)
    }.toArray
  }

}


private case class SnowflakePartition(
                                       fileNames: List[String],
                                       rddId: Int,
                                       index: Int
                                     ) extends Partition {

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}