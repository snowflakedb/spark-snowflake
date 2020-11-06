package org.apache.spark.sql

import org.apache.spark.sql.snowflake.{SFQueryTest, SFTestData, SFTestSessionBase}

class SFDataFrameWindowFunctionsSuite
    extends DataFrameWindowFunctionsSuite
    with SFTestSessionBase
    with SFQueryTest
    with SFTestData {

  override def spark: SparkSession = getSnowflakeSession()

  override protected def blackList: Seq[String] =
    Seq(
      // TS - corr, covar_pop, stddev_pop functions in specific window
      "corr, covar_pop, stddev_pop functions in specific window",
      // TS - covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window
      "covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window",
      // TS - NaN and -0.0 in window partition keys
      "NaN and -0.0 in window partition keys"
    )
}
