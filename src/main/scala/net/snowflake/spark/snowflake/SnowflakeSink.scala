package net.snowflake.spark.snowflake

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.streaming.OutputMode
import org.slf4j.LoggerFactory

class SnowflakeSink(
                     sqlContext: SQLContext,
                     parameters: Map[String, String],
                     partitionColumns: Seq[String],
                     outputMode: OutputMode
                   ) extends Sink{
  private val log = LoggerFactory.getLogger(getClass)

  private val param = Parameters.mergeParameters(parameters)

  //discussion: Do we want to support overwrite mode?
  //In Spark Streaming, there are only three mode append, complete, update
  require(
    outputMode == OutputMode.Append(),
    "Snowflake streaming only supports append mode"
  )

  require(
    param.table.isDefined,
    "Snowflake table name must be specified with the 'dbtable' parameter"
  )

  private val tableName = param.table.get
  






  override def addBatch(batchId: Long, data: DataFrame): Unit = {}
}
