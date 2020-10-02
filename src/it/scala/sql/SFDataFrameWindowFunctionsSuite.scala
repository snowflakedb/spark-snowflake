package org.apache.spark.sql

import net.snowflake.client.jdbc.SnowflakeSQLException
import org.apache.spark.sql.catalyst.optimizer.TransposeWindow
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.thundersnow.{SFQueryTest, SFTestSessionBase, SFTestData}
import org.scalatest.Matchers.the

import scala.reflect.ClassTag

class SFDataFrameWindowFunctionsSuite
    extends DataFrameWindowFunctionsSuite
    with SFTestSessionBase
    with SFQueryTest
    with SFTestData {

  import testImplicits._

  override def spark: SparkSession = getSnowflakeSession()

  override protected def blackList: Seq[String] =
    Seq(
      // report SnowflakeSQLException instead of AnalysisException, replaced by
      // TS - window function should fail if order by clause is not specified
      "window function should fail if order by clause is not specified",
      // Double.NaN is not supported, replaced by
      // TS - corr, covar_pop, stddev_pop functions in specific window
      "corr, covar_pop, stddev_pop functions in specific window",
      // Double.NaN is not supported, replaced by
      // TS - covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window
      "covar_samp, var_samp (variance), stddev_samp (stddev) functions in specific window",
      // collect_list and collect_set are not supported, skip
      "collect_list in ascending ordered window",
      "collect_list in descending ordered window",
      "collect_set in window",
      // SF returns different result of Skewness and Kurtosis, replaced by
      // TS - skewness and kurtosis functions in window
      "skewness and kurtosis functions in window",
      // report SnowflakeSQLException instead of AnalysisException, replaced by
      // TS - aggregation function on invalid column
      "aggregation function on invalid column",
      // numerical aggregate function on string column,
      // Spark returns null value, but SF reports error, skip
      "numerical aggregate functions on string column",
      // SF answer is BigDecimal but not Double, replaced by
      // TS - statistical functions
      "statistical functions",
      // invoke temp view, skip
      "SPARK-16195 empty over spec",
      "aggregation and rows between with unbounded + predicate pushdown",
      "rank functions in unspecific window",
      "aggregation and range between with unbounded + predicate pushdown",
      // invoke udaf, skip
      "window function with udaf",
      "window function with aggregator",
      // skip, spark's first and last functions are different than SF's FIRST_VALUE and LAST_VALUE
      "last/first with ignoreNulls",
      "last/first on descending ordered window",
      // invoke struct, skip
      "SPARK-12989 ExtractWindowExpressions treats alias as regular attribute",
      // invoke first/last function and struct, skip
      "SPARK-21258: complex object in combination with spilling",
      // change Exception names, replaced by
      // TS - SPARK-24575: Window functions inside WHERE and HAVING clauses
      "SPARK-24575: Window functions inside WHERE and HAVING clauses",
      // never spill, skip
      "Window spill with more than the inMemoryThreshold and spillThreshold",
      // no number exchanged, replaced by
      // TS - window functions in multiple selects
      "window functions in multiple selects",
      // SF result always be Double, replaced by
      // TS - NaN and -0.0 in window partition keys
      "NaN and -0.0 in window partition keys")
}
