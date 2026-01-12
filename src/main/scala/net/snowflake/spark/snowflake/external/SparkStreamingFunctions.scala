package org.apache.spark.sql.snowflake

import org.apache.spark.sql.{DataFrame, SQLContext}

object SparkStreamingFunctions {

  def streamingToNonStreaming(sqlContext: SQLContext,
                              dataFrame: DataFrame): DataFrame =
    sqlContext.sparkSession.createDataFrame(
      dataFrame.queryExecution.toRdd,
      dataFrame.schema
    )
}
