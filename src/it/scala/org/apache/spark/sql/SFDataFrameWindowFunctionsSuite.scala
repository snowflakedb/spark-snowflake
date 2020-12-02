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
      // Below test case uses test DataFrame "testData2" and a temp view "testData2".
      // We override "testData2", but no temp view is created, so hit below error when run it.
      //     "Table or view not found: testData2" did not contain "window functions inside WHERE and HAVING clauses"
      // So, skip below test case for spark 2.4
      "SPARK-24575: Window functions inside WHERE and HAVING clauses",
      // TS - NaN and -0.0 in window partition keys
      "NaN and -0.0 in window partition keys"
    )
}
