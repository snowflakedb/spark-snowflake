package org.apache.spark.sql.snowflake

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}

/** For IT code outside `org.apache.spark.sql.*`: `Builder#sparkContext` is `private[sql]`. */
object IntegrationSqlContext {

  def apply(sc: SparkContext): SQLContext =
    SparkSession.builder().sparkContext(sc).getOrCreate().sqlContext
}
