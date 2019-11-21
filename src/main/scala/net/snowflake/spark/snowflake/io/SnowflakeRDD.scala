package net.snowflake.spark.snowflake.io

import java.io.InputStream

import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD


class SnowflakeRDD(
                    sc: SparkContext,
                    fileNames: List[String],
                    format: SupportedFormat,
                    downloadFile: (String) => (InputStream, Boolean),
                    expectedPartitionCount: Int
                  ) extends RDD[String](sc, Nil) {

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
      Math.max(2, (fileNames.length + expectedPartitionCount / 2) / expectedPartitionCount)
    val fileCount = fileNames.length
    val partitionCount = (fileCount + fileCountPerPartition - 1) / fileCountPerPartition
    StageReader.logger.info(s"NIKEPOC: fileCount=$fileCount, filePerPartition=$fileCountPerPartition, partitionCount=$partitionCount")
    //fileNames.foreach(x => println(s"$x"))
    //println(s"file count: $fileCountPerPartition")
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