/*
 * Copyright 2015-2019 Snowflake Computing
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.snowflake.spark.snowflake

import java.sql.{Date, Timestamp}
import java.util.TimeZone

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.TestHook
import org.apache.spark.sql._

// scalastyle:off println
class PushdownEnhancement01 extends IntegrationSuiteBase {
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()
  private val test_table_length: String = s"test_length_$randomSuffix"
  private val test_table_ts_trunc: String = s"test_ts_trunc_$randomSuffix"
  private val test_table_date_trunc: String = s"test_date_trunc_$randomSuffix"

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_length")
      jdbcUpdate(s"drop table if exists $test_table_ts_trunc")
      jdbcUpdate(s"drop table if exists $test_table_date_trunc")
    } finally {
      TestHook.disableTestHook()
      super.afterAll()
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // There is bug for Date.equals() to compare Date with different timezone,
    // so set up the timezone to work around it.
    val gmtTimezone = TimeZone.getTimeZone("GMT")
    TimeZone.setDefault(gmtTimezone)

    connectorOptionsNoTable.foreach(tup => {
      thisConnectorOptionsNoTable += tup
    })
  }

  test("test pushdown length() function") {
    jdbcUpdate(s"create or replace table $test_table_length(c1 char(10), c2 varchar(10), c3 string)")
    jdbcUpdate(s"insert into $test_table_length values ('', 'abc', null)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_length)
      .load()

    tmpDF.createOrReplaceTempView("test_table_length")

    val result = sparkSession.sql(
      "select length(c1), length(c2), length(c3) from test_table_length")
    val expectedResult = Seq(Row(0, 3, null))

    testPushdown(
      s""" SELECT ( LENGTH ( "SUBQUERY_0"."C1" ) ) AS "SUBQUERY_1_COL_0" ,
         |( LENGTH ( "SUBQUERY_0"."C2" ) ) AS "SUBQUERY_1_COL_1" ,
         |( LENGTH ( "SUBQUERY_0"."C3" ) ) AS "SUBQUERY_1_COL_2"
         |FROM ( SELECT * FROM ( $test_table_length ) AS
         |"SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" """.stripMargin,
      result,
      expectedResult
    )
  }

  test("test pushdown date_trunc function") {
    jdbcUpdate(s"create or replace table $test_table_ts_trunc(ts timestamp_ntz)")
    jdbcUpdate(s"insert into $test_table_ts_trunc values ('2019-08-21 12:34:56.789')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_ts_trunc)
      .load()

    tmpDF.createOrReplaceTempView("test_table_ts_trunc")

    //  ["YEAR", "YYYY", "YY", "MON", "MONTH", "MM", "DAY", "DD",
    //  "HOUR", "MINUTE", "SECOND", "WEEK", "QUARTER"]
    val result =
      sparkSession.sql(
        s"""select date_trunc('YEAR', ts), date_trunc('YYYY', ts),
           | date_trunc('YY', ts), date_trunc('MON', ts),
           | date_trunc('MONTH', ts), date_trunc('MM', ts),
           | date_trunc('DAY', ts), date_trunc('DD', ts),
           | date_trunc('HOUR', ts), date_trunc('MINUTE', ts),
           | date_trunc('WEEK', ts), date_trunc('QUARTER', ts)
           | from test_table_ts_trunc""".stripMargin)

    val expectedResult = Seq(Row(
      Timestamp.valueOf("2019-01-01 00:00:00.000"),
      Timestamp.valueOf("2019-01-01 00:00:00.000"),
      Timestamp.valueOf("2019-01-01 00:00:00.000"),
      Timestamp.valueOf("2019-08-01 00:00:00.000"),
      Timestamp.valueOf("2019-08-01 00:00:00.000"),
      Timestamp.valueOf("2019-08-01 00:00:00.000"),
      Timestamp.valueOf("2019-08-21 00:00:00.000"),
      Timestamp.valueOf("2019-08-21 00:00:00.000"),
      Timestamp.valueOf("2019-08-21 12:00:00.000"),
      Timestamp.valueOf("2019-08-21 12:34:00.000"),
      Timestamp.valueOf("2019-08-19 00:00:00.000"),
      Timestamp.valueOf("2019-07-01 00:00:00.000")
    ))

    result.printSchema()
    result.show()

    testPushdown(
      s"""select(date_trunc('year',"subquery_0"."ts"))as"subquery_1_col_0",
         |(date_trunc('yyyy',"subquery_0"."ts"))as"subquery_1_col_1",
         |(date_trunc('yy',"subquery_0"."ts"))as"subquery_1_col_2",
         |(date_trunc('mon',"subquery_0"."ts"))as"subquery_1_col_3",
         |(date_trunc('month',"subquery_0"."ts"))as"subquery_1_col_4",
         |(date_trunc('mm',"subquery_0"."ts"))as"subquery_1_col_5",
         |(date_trunc('day',"subquery_0"."ts"))as"subquery_1_col_6",
         |(date_trunc('dd',"subquery_0"."ts"))as"subquery_1_col_7",
         |(date_trunc('hour',"subquery_0"."ts"))as"subquery_1_col_8",
         |(date_trunc('minute',"subquery_0"."ts"))as"subquery_1_col_9",
         |(date_trunc('week',"subquery_0"."ts"))as"subquery_1_col_10",
         |(date_trunc('quarter',"subquery_0"."ts"))as"subquery_1_col_11"
         |from(select*from($test_table_ts_trunc )as
         |"sf_connector_query_alias")as"subquery_0"""".stripMargin,
      result,
      expectedResult
    )
  }

  test("test pushdown trunc() function") {
    jdbcUpdate(s"create or replace table $test_table_date_trunc(dt date)")
    jdbcUpdate(s"insert into $test_table_date_trunc values ('2019-08-21')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_date_trunc)
      .load()

    tmpDF.createOrReplaceTempView("test_table_date_trunc")

    //  ["year", "yyyy", "yy", "mon", "month", "mm"]
    val result =
    sparkSession.sql(
      s"""select trunc(dt, 'YEAR'), trunc(dt, 'YYYY'),
         | trunc(dt, 'YY'), trunc(dt, 'MON'),
         | trunc(dt, 'MONTH'), trunc(dt, 'MM')
         | from test_table_date_trunc""".stripMargin)

    val expectedResult = Seq(Row(
      Date.valueOf("2019-01-01"), Date.valueOf("2019-01-01"),
      Date.valueOf("2019-01-01"), Date.valueOf("2019-08-01"),
      Date.valueOf("2019-08-01"), Date.valueOf("2019-08-01")
    ))

    result.printSchema()
    result.show()

    testPushdown(
      s"""select(trunc("subquery_0"."dt",'year'))as"subquery_1_col_0",
         |(trunc("subquery_0"."dt",'yyyy'))as"subquery_1_col_1",
         |(trunc("subquery_0"."dt",'yy'))as"subquery_1_col_2",
         |(trunc("subquery_0"."dt",'mon'))as"subquery_1_col_3",
         |(trunc("subquery_0"."dt",'month'))as"subquery_1_col_4",
         |(trunc("subquery_0"."dt",'mm'))as"subquery_1_col_5"
         |from(select*from($test_table_date_trunc )as
         |"sf_connector_query_alias")as"subquery_0
         |"""".stripMargin,
      result,
      expectedResult
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }
}
// scalastyle:on println

