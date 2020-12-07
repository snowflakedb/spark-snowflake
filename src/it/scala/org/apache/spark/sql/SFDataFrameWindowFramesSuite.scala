package org.apache.spark.sql

import org.apache.spark.sql.snowflake.{SFQueryTest, SFTestData, SFTestSessionBase}

class SFDataFrameWindowFramesSuite
    extends DataFrameWindowFramesSuite
    with SFTestSessionBase
    with SFQueryTest
    with SFTestData {

  override def spark: SparkSession = getSnowflakeSession()

  override protected def blackList: Seq[String] = Seq.empty
}
