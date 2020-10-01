package org.apache.spark.sql

import org.apache.spark.sql.thundersnow._

class SFDataFrameAggregateSuite
    extends DataFrameAggregateSuite
    with SnowflakeTestSessionBase
    with TSTestData {

  override def beforeAll(): Unit = super.beforeAll()

  override def spark: SparkSession = getSnowflakeSession()

  override protected def blackList: Seq[String] =
    Seq(
      // contains RDD, replaced by TS - groupBy
      "groupBy",
      // column name case issue, replace by TS - SPARK-17124 agg should be ordering preserving
      "SPARK-17124 agg should be ordering preserving",
      // we don't support RegexpExact, it is not a SF function
      "SPARK-18952: regexes fail codegen when used as keys due to bad forward-slash escapes",
      // contains RDD, replaced by TS - cube
      "cube",
      // replaced by TS - grouping and grouping_id, replaced AnalysisException by
      // SnowflakeSQLException
      "grouping and grouping_id",
      // replaced by TS - count, SF does not support DataFrame.rdd
      "count",
      // replace by TS - stddev, floating point issue
      "stddev",
      // replace by TS - moments, floating point issue, kurtosis has different output
      "moments",
      // replace by TS - zero moments, Snowflake aggregate suite returns null instead of Double.NaN
      // Snowflake skewness function is called skew
      "zero moments",
      // replace by TS - null moments, Snowflake skewness function is called skew
      "null moments",
      // Snowflake does not support collect_set and collect_list
      "collect functions",
      "collect functions structs",
      "collect_set functions cannot have maps",
      "SPARK-17641: collect functions should not collect null values",
      "collect functions should be able to cast to array type with no null values",
      // Replaced by TS - SPARK-14664: Decimal sum/avg over window should work.
      // Snowflake does not support "select * from values 1.0, 2.0, 3.0 T(a)"
      // Need to replace with "select * from values (1.0), (2.0), (3.0) T(a)"
      "SPARK-14664: Decimal sum/avg over window should work.",
      // Snowflake does not support collect_list
      "SPARK-17616: distinct aggregate combined with a non-partial aggregate",
      // skipped because Snowflake does not support pivot with multiple aggregrate functions
      "SPARK-17237 remove backticks in a pivot result schema",
      // Replaced by TS - aggregate function in GROUP BY
      // Snowflake does not throw AnalysisException, throws SnowflakeSQLException instead
      "aggregate function in GROUP BY",
      // Snowflake does not support monotonically_increasing_id() and spark_partition_id() functions
      "SPARK-19471: AggregationIterator does not initialize the generated result "
        + "projection before using it",
      // Replaced by TS - SPARK-21580 ints in aggregation expressions are taken as group-by ordinal.
      // Snowflake does not have table TestData2 stored in test database
      "SPARK-21580 ints in aggregation expressions are taken as group-by ordinal.",
      // Snowflake does not support collect_list, repartition
      "SPARK-22223: ObjectHashAggregate should not introduce unnecessary shuffle",
      // We have different SnowflakePlans, so testing the .toString does not make sense
      "SPARK-24788: RelationalGroupedDataset.toString with unresolved exprs should not fail",
      // Snowflake does not support float type, struct()
      "SPARK-26021: NaN and -0.0 in grouping expressions",
      // Snowflake does not support range() and count(distinct(*)) to count all distinct rows
      "SPARK-27581: DataFrame countDistinct(\"*\") shouldn't fail with AnalysisException",
      // Snowflake does not support tempView used in following tests and max_by, min_by, count_if
      "max_by",
      "min_by",
      "count_if",
      // Replace Spark exception by Snowflake exception, replaced by
      // TS - SPARK-21896: Window functions inside aggregate functions
      "SPARK-21896: Window functions inside aggregate functions",
      // SF doesn't support empty DataFrame. SF's table has to have at least one column,
      // but Spark DataFrame has no column
      "SPARK-22951: dropDuplicates on empty dataFrames should produce correct aggregate "
        + "(whole-stage-codegen off)",
      "SPARK-22951: dropDuplicates on empty dataFrames should produce correct aggregate "
        + "(whole-stage-codegen on)",
      "SPARK-31620: agg with subquery (whole-stage-codegen = true)",
      "SPARK-31620: agg with subquery (whole-stage-codegen = false)",
      // SF doesn't support collect_set()
      "SPARK-31500: collect_set() of BinaryType returns duplicate elements",
      // disable this test case, re-enable when we have parameters
      "spark.sql.retainGroupColumns config")
}
