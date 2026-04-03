package org.apache.spark.sql.snowflake

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.classic.{SQLContext => ClassicSQLContext}

object SparkStreamingFunctions {

  def streamingToNonStreaming(sqlContext: SQLContext,
                              dataFrame: DataFrame): DataFrame =
    sqlContext.asInstanceOf[ClassicSQLContext].internalCreateDataFrame(
      dataFrame.queryExecution.toRdd,
      dataFrame.schema,
      isStreaming = false
    )
}
