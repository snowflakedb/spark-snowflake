package org.apache.spark.sql.snowflake

import org.apache.spark.sql.{DataFrame, SQLContext}

object SparkStreamingFunctions {

  def streamingToNonStreaming(sqlContext: SQLContext, dataFrame: DataFrame): DataFrame =
    sqlContext.internalCreateDataFrame(
      dataFrame.queryExecution.toRdd,
      dataFrame.schema,
      isStreaming = false
    )
}
