package net.snowflake.spark.snowflake_beta.io

import java.io.InputStream

import net.snowflake.spark.snowflake_beta.io.SupportedFormat.SupportedFormat
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD


class SnowflakeRDD(
                    sc: SparkContext,
                    fileNames: List[String],
                    format: SupportedFormat,
                    downloadFile: (String) => InputStream
                  ) extends RDD[String](sc, Nil) {

  @transient private val FILES_PER_PARTITION = 2

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val stringIterator = new SFRecordReader(format)
    split.asInstanceOf[SnowflakePartition].fileNames
      .foreach(name => stringIterator.addStream(downloadFile(name)))
    stringIterator
  }

  override protected def getPartitions: Array[Partition] =
    fileNames.grouped(FILES_PER_PARTITION).zipWithIndex.map {
      case (names, index) => SnowflakePartition(names, id, index)
    }.toArray

}


private case class SnowflakePartition(
                                       fileNames: List[String],
                                       rddId: Int,
                                       index: Int
                                     ) extends Partition {

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}