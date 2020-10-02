package org.apache.spark.sql

import net.snowflake.client.jdbc.SnowflakeSQLException
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, count, min}
import org.apache.spark.sql.thundersnow.{SFQueryTest, SFTestSessionBase, SFTestData}

class SFDataFrameWindowFramesSuite
    extends DataFrameWindowFramesSuite
    with SFTestSessionBase
    with SFQueryTest
    with SFTestData {

  import testImplicits._

  override def spark: SparkSession = getSnowflakeSession()

  override protected def blackList: Seq[String] =
    Seq(
      // invoke FIRST/LAST function, skip
      "rows/range between with empty data frame",
      "unbounded preceding/following rows between with aggregation",
      "reverse unbounded preceding/following rows between with aggregation",
      // SF Window size is no more than 1000, replaced by
      // TS - rows between should accept int/long values as boundary
      "rows between should accept int/long values as boundary",
      // change Exception type, replaced by
      // TS - range between should accept at most one ORDER BY expression when unbounded
      "range between should accept at most one ORDER BY expression when unbounded",
      // change Exception type, replaced by
      // TS - range between should accept numeric values only when bounded
      "range between should accept numeric values only when bounded",
      // SF sliding window frame unsupported for function COUNT, skip
      "range between should accept int/long values as boundary",
      // SF cumulative window frame unsupported for function SUM, skip
      "unbounded preceding/following range between with aggregation",
      "reverse preceding/following range between with aggregation",
      // floating point issue, replaced by
      // TS - sliding rows between with aggregation
      "sliding rows between with aggregation",
      // floating point issue, replaced by
      // TS - reverse sliding rows between with aggregation
      "reverse sliding rows between with aggregation",
      // Sliding window frame unsupported for function AVG, skip
      "sliding range between with aggregation",
      "reverse sliding range between with aggregation",
      // Sliding window frame unsupported for function LEAD, skip
      "SPARK-24033: Analysis Failure of OffsetWindowFunction")
}
