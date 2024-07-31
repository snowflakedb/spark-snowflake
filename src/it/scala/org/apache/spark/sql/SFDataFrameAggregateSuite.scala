package org.apache.spark.sql

import org.apache.spark.sql.snowflake.{SFQueryTest, SFTestData, SFTestSessionBase}

class SFDataFrameAggregateSuite
    extends DataFrameAggregateSuite
    with SFTestSessionBase
    with SFQueryTest
    with SFTestData {

  override def spark: SparkSession = getSnowflakeSession()

  override protected def blackList: Seq[String] =
    Seq(
      // replace by TS - zero moments, Snowflake aggregate suite returns null instead of Double.NaN
      // Snowflake skewness function is called skew
      "zero moments",
      // Replaced by TS - SPARK-21580 ints in aggregation expressions are taken as group-by ordinal.
      // Snowflake does not have table TestData2 stored in test database
      "SPARK-21580 ints in aggregation expressions are taken as group-by ordinal.",
      // We have different SnowflakePlans, so testing the .toString does not make sense
      "SPARK-24788: RelationalGroupedDataset.toString with unresolved exprs should not fail",
      // Snowflake does not support float type, struct()
      "SPARK-26021: NaN and -0.0 in grouping expressions",
      "max_by",
      "min_by",
      // Below 2 test cases are new test cases for spark 3.2.
      // The are not runnable for the negative test assertion:
      // It expects to see an SparkException, but Spark connector test will write data
      // to a table and read it back, so no SparkException is raised.
      //     val df2 = Seq((Period.ofMonths(Int.MaxValue), Duration.ofDays(106751991)),
      //      (Period.ofMonths(10), Duration.ofDays(10)))
      //      .toDF("year-month", "day")
      //    val error = intercept[SparkException] {
      //      checkAnswer(df2.select(sum($"year-month")), Nil)
      //    }
      "SPARK-34716: Support ANSI SQL intervals by the aggregate function `sum`",
      "SPARK-34837: Support ANSI SQL intervals by the aggregate function `avg`",
      // Replace Spark exception by Snowflake exception, replaced by
      // TS - SPARK-21896: Window functions inside aggregate functions
      "bit aggregate",
      "SPARK-16484: hll_*_agg + hll_union negative tests"
    )
}
