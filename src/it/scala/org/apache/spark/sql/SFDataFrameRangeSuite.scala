package org.apache.spark.sql

import org.apache.spark.sql.snowflake.{SFQueryTest, SFTestSessionBase}

class SFDataFrameRangeSuite extends DataFrameRangeSuite with SFTestSessionBase with SFQueryTest {
  // replace Spark Utils
  override def spark: SparkSession = getSnowflakeSession()

  override protected def blackList: Seq[String] =
    Seq(
      // we don't support sql("range()")
      "SPARK-20430 Initialize Range parameters in a driver side",
      // Expected exception org.apache.spark.SparkException to be thrown,
      // but net.snowflake.client.jdbc.SnowflakeSQLException was thrown
      "Cancelling stage in a query with Range. (whole-stage-codegen off)",
      "Cancelling stage in a query with Range. (whole-stage-codegen on)")
}
