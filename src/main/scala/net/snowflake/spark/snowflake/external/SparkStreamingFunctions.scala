package org.apache.spark.sql.snowflake

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.catalyst.CatalystTypeConverters

object SparkStreamingFunctions {

  def streamingToNonStreaming(sqlContext: SQLContext,
                              dataFrame: DataFrame): DataFrame = {
    val sparkSession = sqlContext.sparkSession
    val schema = dataFrame.schema
    
    // Convert RDD[InternalRow] to RDD[Row] using CatalystTypeConverters
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    val rowRdd = dataFrame.queryExecution.toRdd.map { internalRow =>
      converter(internalRow).asInstanceOf[Row]
    }
    
    sparkSession.createDataFrame(rowRdd, schema)
  }
}
