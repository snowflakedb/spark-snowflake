package net.snowflake.spark.snowflake.io

import net.snowflake.spark.snowflake.JDBCWrapper
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class InternalRDD(
                      @transient val sqlContext: SQLContext,
                      @transient val params: MergedParameters,
                      @transient val sql: String,
                      @transient val jdbcWrapper: JDBCWrapper,
                      val format: SupportedFormat = SupportedFormat.CSV
                      ) extends RDD[String](sqlContext.sparkContext, Nil){
  override def compute(split: Partition, context: TaskContext): Iterator[String] = null

  override protected def getPartitions: Array[Partition] = null
}
