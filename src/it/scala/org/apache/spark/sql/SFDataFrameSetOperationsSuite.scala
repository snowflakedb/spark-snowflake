package org.apache.spark.sql

import org.apache.spark.sql.snowflake.{SFQueryTest, SFTestData, SFTestSessionBase}
import org.apache.spark.sql.test.SharedSparkSession

class SFDataFrameSetOperationsSuite
    extends DataFrameSetOperationsSuite
    with SFTestSessionBase
    with SFQueryTest
    with SFTestData {

  override def spark: SparkSession = getSnowflakeSession()

  override protected def blackList: Seq[String] =
    Seq(
      "SPARK-25368 Incorrect predicate pushdown returns wrong result",
      "exceptAll - nullability",
      "intersectAll - nullability"
   )
}

// DataFrameSetOperationsSuite is new from spark 3.0
// So this test is not applicable to spark 2.3/2.4
// Create an empty class to work around the compiling issue.
class DataFrameSetOperationsSuite extends QueryTest with SharedSparkSession {
}
