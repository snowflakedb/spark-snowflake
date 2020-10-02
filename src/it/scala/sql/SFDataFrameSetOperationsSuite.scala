package org.apache.spark.sql

import java.sql.{Date, Timestamp}

import net.snowflake.client.jdbc.SnowflakeSQLException
import org.apache.spark.sql.functions.{lit, min, rand, sum}
import org.apache.spark.sql.thundersnow.{SFQueryTest, SFTestSessionBase, SFTestData}

class SFDataFrameSetOperationsSuite
    extends DataFrameSetOperationsSuite
    with SFTestSessionBase
    with SFQueryTest
    with SFTestData {
  import testImplicits._

  override def spark: SparkSession = getSnowflakeSession()

  override protected def blackList: Seq[String] =
    Seq(
      // All test names that are commented pass
      // "intersect",
      // "except",
      // "SPARK-23274: except between two projects without references used in filter",
      // "except distinct - SQL compliance",
      // "SPARK-10539: Project should not be pushed down through Intersect or Except",
      //
      // Added test for these below with some modifications
      "union all",
      "union by name",
      "except - nullability",
      "intersect - nullability",
      "SPARK-25368 Incorrect predicate pushdown returns wrong result",
      "SPARK-17123: Performing set operations that combine non-scala native types",
      "union by name - check name duplication",
      "SPARK-10740: handle nondeterministic expressions correctly for set operations",
      //
      // Snowflake does not support except all and intersect all
      "except all",
      "exceptAll - nullability",
      "intersectAll",
      "intersectAll - nullability",
      //
      // This test just asserts that map function does not work with union
      "SPARK-19893: cannot run set operations with map type",
      // TODO: We will need to handle upcasting for columns if their types don't match
      "union by name - type coercion",
      // ThunderSnow does not support SQL confs for case sensitivity
      "union by name - check case sensitivity",
      // Snowflake does not support UDT
      "union should union DataFrames with UDTs (SPARK-13410)")
}
