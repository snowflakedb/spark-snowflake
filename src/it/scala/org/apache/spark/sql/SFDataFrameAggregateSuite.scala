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
      // Replace Spark exception by Snowflake exception, replaced by
      // TS - SPARK-21896: Window functions inside aggregate functions
      "SPARK-21896: Window functions inside aggregate functions",
       )
}
