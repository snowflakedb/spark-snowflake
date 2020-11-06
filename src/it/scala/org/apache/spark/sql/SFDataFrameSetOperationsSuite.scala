package org.apache.spark.sql

import org.apache.spark.sql.snowflake.{SFQueryTest, SFTestData, SFTestSessionBase}

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
