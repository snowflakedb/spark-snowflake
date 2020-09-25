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

import java.sql._
import java.util.TimeZone

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.TestHook
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// scalastyle:off println
class PushdownEnhancement03 extends IntegrationSuiteBase {
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()
  private val test_table_unix_timestamp: String = s"test_table_unix_timestamp_$randomSuffix"
  private val test_table_time_add: String = s"test_table_time_add_$randomSuffix"

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_unix_timestamp")
      jdbcUpdate(s"drop table if exists $test_table_time_add")
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

  ignore("test pushdown function: unix_timestamp() for String") {
    jdbcUpdate(
      s"""create or replace table $test_table_unix_timestamp (
      | s1_standard string, s2_ddMMyy string, s3_dd_MM_yyyy string,
      | s4_yyyyMMdd string, s5_yyMMdd string, s6_MMdd string,
      | s7_ddMMyyyy string, s8_SSS string, s9_literal1 string,
      | s10_literal2 string)
      |""".stripMargin)
    // Note: Spark default format is "yyyy-MM-dd HH:mm:ss"
    jdbcUpdate(s"""insert into $test_table_unix_timestamp values
               | ('2017-01-01 12:12:12', '31072020', '31/07/2020',
               | '20200731', '200731', '0731', '31072020',
               | '2017/01/30 12:12:12.123', '2017.01.01T12:12:12Z',
               | 'year: 2012, month: 01, day: 30')
               | """.stripMargin)

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_unix_timestamp)
      .load()

    val resultDF = tmpDF.select(
      unix_timestamp(col("s1_standard")),
      unix_timestamp(col("s2_ddMMyy"), "ddMMyy"),
      unix_timestamp(col("s3_dd_MM_yyyy"), "dd/MM/yyyy"),
      unix_timestamp(col("s4_yyyyMMdd"), "yyyyMMdd"),
      unix_timestamp(col("s5_yyMMdd"), "yyMMdd"),
      unix_timestamp(col("s6_MMdd"), "MMdd"),
      unix_timestamp(col("s7_ddMMyyyy"), "ddMMyyyy"),
      unix_timestamp(col("s8_SSS"), "yyyy/MM/dd HH:mm:ss.SSS"),
      unix_timestamp(col("s9_literal1"), "yyyy.MM.dd'T'HH:mm:ss'Z'"),
      unix_timestamp(col("s10_literal2"), "'year: 'yyyy', month: 'MM', day: 'dd")
    )

    // resultDF.printSchema()
    // resultDF.show(truncate = false)

    // The expected result is generated when pushdown is disabled.
    val expectedResult = Seq(
      Row(1483272732L, 1596153600L, 1596153600L,
        1596153600L, 1596153600L, 18230400L, 1596153600L,
        1485778332L, 1483272732L, 1327881600L
      )
    )

    testPushdown(
      s"""SELECT
         |  ( DATE_PART('epoch_second',
         |    TO_TIMESTAMP ( "SUBQUERY_0"."S1_STANDARD" ,
         |      'yyyy-MM-dd HH:MI:ss' ) ) ) AS "SUBQUERY_1_COL_0" ,
         |  ( DATE_PART('epoch_second',
         |    TO_TIMESTAMP ( "SUBQUERY_0"."S2_DDMMYY" ,
         |      'ddMMyy' ) ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( DATE_PART('epoch_second',
         |    TO_TIMESTAMP ( "SUBQUERY_0"."S3_DD_MM_YYYY" ,
         |      'dd/MM/yyyy' ) ) ) AS "SUBQUERY_1_COL_2" ,
         |  ( DATE_PART('epoch_second',
         |    TO_TIMESTAMP ( "SUBQUERY_0"."S4_YYYYMMDD" ,
         |      'yyyyMMdd' ) ) ) AS "SUBQUERY_1_COL_3" ,
         |  ( DATE_PART('epoch_second',
         |    TO_TIMESTAMP ( "SUBQUERY_0"."S5_YYMMDD" ,
         |      'yyMMdd' ) ) ) AS "SUBQUERY_1_COL_4" ,
         |  ( DATE_PART('epoch_second',
         |    TO_TIMESTAMP ( "SUBQUERY_0"."S6_MMDD" ,
         |      'MMdd' ) ) ) AS "SUBQUERY_1_COL_5" ,
         |  ( DATE_PART('epoch_second',
         |    TO_TIMESTAMP ( "SUBQUERY_0"."S7_DDMMYYYY" ,
         |      'ddMMyyyy' ) ) ) AS "SUBQUERY_1_COL_6" ,
         |  ( DATE_PART('EPOCH_SECOND',
         |    TO_TIMESTAMP ( "SUBQUERY_0"."S8_SSS" ,
         |      'yyyy/MM/dd HH:MI:ss.FF3' ) ) ) AS "SUBQUERY_1_COL_7",
         |  ( DATE_PART('EPOCH_SECOND',
         |    TO_TIMESTAMP ( "SUBQUERY_0"."S9_LITERAL1" ,
         |      'yyyy.MM.dd"T"HH:MI:ss"Z"' ) ) ) AS "SUBQUERY_1_COL_8",
         |  ( DATE_PART('EPOCH_SECOND',
         |    TO_TIMESTAMP ( "SUBQUERY_0"."S10_LITERAL2" ,
         |      '"year: "yyyy", month: "MM", day: "dd' ) ) )
         |        AS "SUBQUERY_1_COL_9"
         |FROM ( SELECT * FROM ( $test_table_unix_timestamp )
         |   AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  ignore("test pushdown function: unix_timestamp() with timestamp/date") {
    jdbcUpdate(s"create or replace table $test_table_unix_timestamp " +
      s"(d1 date, ntz timestamp_ntz, ltz timestamp_ltz, tz timestamp_tz)")
    jdbcUpdate(s"""insert into $test_table_unix_timestamp values
                  | ('2017-01-01', '2017-01-01 12:12:12',
                  | '2017-01-01 12:12:12', '2017-01-01 12:12:12')
                  | """.stripMargin)

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_unix_timestamp)
      .load()

    val resultDF = tmpDF.select(
      unix_timestamp(col("d1")),
      unix_timestamp(col("ntz")),
      unix_timestamp(col("ltz")),
      unix_timestamp(col("tz"))
    )

    // resultDF.printSchema()
    // resultDF.show(truncate = false)

    // The expected result is generated when pushdown is disabled.
    val expectedResult = Seq(
      Row(1483228800.toLong, 1483272732.toLong,
        1483272732.toLong, 1483272732.toLong
      )
    )

    testPushdown(
      s"""SELECT
         |  ( DATE_PART('epoch_second', ( "SUBQUERY_0"."D1" ) ) )
         |    AS "SUBQUERY_1_COL_0" ,
         |  ( DATE_PART('epoch_second', ( "SUBQUERY_0"."NTZ" ) ) )
         |    AS "SUBQUERY_1_COL_1" ,
         |  ( DATE_PART('epoch_second', ( "SUBQUERY_0"."LTZ" ) ) )
         |    AS "SUBQUERY_1_COL_2" ,
         |  ( DATE_PART('epoch_second', ( "SUBQUERY_0"."TZ" ) ) )
         |    AS "SUBQUERY_1_COL_3"
         |FROM ( SELECT * FROM ( $test_table_unix_timestamp )
         |   AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  ignore("test pushdown function: TimeSub()/TimeAdd()") {
    jdbcUpdate(s"create or replace table $test_table_time_add " +
      s"(d1 date, ntz timestamp_ntz, ltz timestamp_ltz, tz timestamp_tz)")
    jdbcUpdate(s"""insert into $test_table_time_add values
                  | ('2017-01-01', '2017-01-01 12:12:12',
                  | '2017-01-01 12:12:12', '2017-01-01 12:12:12')
                  | """.stripMargin)

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_time_add)
      .load()

    val resultDF = tmpDF.select(
      col("ntz"),
      col("ntz") + expr("INTERVAL 1 days"),
      col("ntz") + expr("INTERVAL -1 hours"),
      col("ntz") - expr("INTERVAL 1 minutes"),
      col("ntz") - expr("INTERVAL -1 seconds"),
      col("ltz") - expr("INTERVAL -2 months")
        + expr("INTERVAL 2 days")
        - expr("INTERVAL 2 minutes"),
      col("tz") + expr("INTERVAL 3 months -3 days 3 minutes"),
      col("tz") - expr("INTERVAL 4 months -4 days -4 seconds"),
      col("tz") + expr("INTERVAL 5 months -5 days -5 seconds")
        - expr("INTERVAL 5 months -5 days 6 minutes"),
      col("tz") + expr("INTERVAL 0 seconds"), // special value 0
      col("tz") - expr("INTERVAL 0 hours")
    )

    // resultDF.printSchema()
    // resultDF.show(truncate = false)

    // The expected result is generated when pushdown is disabled.
    val expectedResult = Seq(Row(
      Timestamp.valueOf("2017-01-01 12:12:12"), // origin
      Timestamp.valueOf("2017-01-02 12:12:12"), // + +1 days
      Timestamp.valueOf("2017-01-01 11:12:12"), // + -1 hours
      Timestamp.valueOf("2017-01-01 12:11:12"), // - +1 minutes
      Timestamp.valueOf("2017-01-01 12:12:13"), // - -1 seconds
      Timestamp.valueOf("2017-03-03 12:10:12"), // (- -2 months) (+ +2 days) (- +2 minutes)
      Timestamp.valueOf("2017-03-29 12:15:12"), // + (3 months -3 days 3 minutes)
      Timestamp.valueOf("2016-09-05 12:12:16"), // - (4 months -4 days -4 seconds)
      Timestamp.valueOf("2017-01-01 12:06:07"), // + (5 months -5 days -5 seconds)
                                                // - (5 months -5 days 6 minutes)
      Timestamp.valueOf("2017-01-01 12:12:12"), // + 0
      Timestamp.valueOf("2017-01-01 12:12:12")  // - 0
    ))

    testPushdown(
      s"""SELECT
         |  ( "SUBQUERY_0"."NTZ" ) AS "SUBQUERY_1_COL_0" ,
         |  ( DATEADD ( 'DAY', 1, "SUBQUERY_0"."NTZ" ) )
         |    AS "SUBQUERY_1_COL_1" ,
         |  ( DATEADD ( 'MICROSECOND', -3600000000, "SUBQUERY_0"."NTZ" ) )
         |    AS "SUBQUERY_1_COL_2" ,
         |  ( DATEADD ( 'MICROSECOND', (0 - (60000000)), "SUBQUERY_0"."NTZ" ) )
         |    AS "SUBQUERY_1_COL_3" ,
         |  ( DATEADD ( 'MICROSECOND', (0 - (-1000000)), "SUBQUERY_0"."NTZ" ) )
         |    AS "SUBQUERY_1_COL_4" ,
         |  ( DATEADD ( 'MICROSECOND', (0 - (120000000)),
         |      DATEADD ( 'DAY', 2,
         |        DATEADD ( 'MONTH', (0 - (-2)), "SUBQUERY_0"."LTZ" ) ) ) )
         |        AS "SUBQUERY_1_COL_5" ,
         |  ( DATEADD ( 'MONTH', 3,
         |      DATEADD ( 'DAY', -3,
         |        DATEADD ( 'MICROSECOND', 180000000, "SUBQUERY_0"."TZ" ) ) ) )
         |        AS "SUBQUERY_1_COL_6" ,
         |  ( DATEADD ( 'MONTH', (0 - (4)),
         |      DATEADD ( 'DAY', (0 - (-4)),
         |        DATEADD ( 'MICROSECOND', (0 - (-4000000)), "SUBQUERY_0"."TZ"))))
         |        AS "SUBQUERY_1_COL_7" ,
         |  ( DATEADD ( 'MONTH', (0 - (5)),
         |      DATEADD ( 'DAY', (0 - (-5)),
         |         DATEADD ( 'MICROSECOND', (0 - (360000000)),
         |           DATEADD ( 'MONTH', 5,
         |             DATEADD ( 'DAY', -5,
         |               DATEADD ( 'MICROSECOND', -5000000,
         |                 "SUBQUERY_0"."TZ" ) ) ) ) ) ) )
         |                 AS "SUBQUERY_1_COL_8" ,
         |  ( "SUBQUERY_0"."TZ" ) AS "SUBQUERY_1_COL_9" ,
         |  ( "SUBQUERY_0"."TZ" ) AS "SUBQUERY_1_COL_10"
         |FROM ( SELECT * FROM ( $test_table_time_add )
         |   AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  ignore("test TimeSub/TimeAdd combines with unix_timestamp") {
    jdbcUpdate(s"create or replace table $test_table_unix_timestamp (s1 string, s2 string)")
    // dataformat 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''
    jdbcUpdate(s"insert into $test_table_unix_timestamp values ('2017-12-31T12:12:12Z', '2017.12.31T12:12:12Z')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_unix_timestamp)
      .load()

    tmpDF.createOrReplaceTempView("test_table_unix_timestamp")

    val resultDF = sparkSession.sql(
      s"""select
         |  s1,
         |  CAST(unix_timestamp(s1, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS TIMESTAMP) - interval 4 hours,
         |  CAST(unix_timestamp(s2, "yyyy.MM.dd'T'HH:mm:ss'Z'") AS TIMESTAMP) + interval 4 minutes,
         |  CAST(unix_timestamp(s2, "yyyy.MM.dd'T'HH:mm:ssZ") AS TIMESTAMP) + interval 4 seconds
         | from test_table_unix_timestamp
         | """.stripMargin
    )

    // resultDF.printSchema()
    // resultDF.show(truncate = false)

    // The expected result is generated when pushdown is disabled.
    val expectedResult = Seq(Row(
      "2017-12-31T12:12:12Z",
      Timestamp.valueOf("2017-12-31 08:12:12"), // - 4 hours
      Timestamp.valueOf("2017-12-31 12:16:12"), // + 4 minutes
      Timestamp.valueOf("2017-12-31 12:12:16")  // + 4 seconds
    ))

    testPushdown(
      s"""SELECT
         |  ( "SUBQUERY_0"."S1" ) AS "SUBQUERY_1_COL_0" ,
         |  ( DATEADD ( 'MICROSECOND', (0 - (14400000000)),
         |    CAST ( DATE_PART('EPOCH_SECOND',
         |                     TO_TIMESTAMP ("SUBQUERY_0"."S1" ,
         |                                   'yyyy-MM-dd"T"HH:MI:ss"Z"' )
         |           ) AS TIMESTAMP ) ) ) AS "SUBQUERY_1_COL_1" ,
         |  ( DATEADD ( 'MICROSECOND', 240000000,
         |    CAST ( DATE_PART('EPOCH_SECOND',
         |                     TO_TIMESTAMP ("SUBQUERY_0"."S2" ,
         |                                   'yyyy.MM.dd"T"HH:MI:ss"Z"' )
         |           ) AS TIMESTAMP ) ) ) AS "SUBQUERY_1_COL_2",
         |  ( DATEADD ( 'MICROSECOND', 4000000,
         |    CAST ( DATE_PART('EPOCH_SECOND',
         |                     TO_TIMESTAMP ("SUBQUERY_0"."S2" ,
         |                                   'yyyy.MM.dd"T"HH:MI:ssZ' )
         |            ) AS TIMESTAMP ) ) ) AS "SUBQUERY_1_COL_3"
         |FROM ( SELECT * FROM ( $test_table_unix_timestamp )
         |   AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }
}
// scalastyle:on println

