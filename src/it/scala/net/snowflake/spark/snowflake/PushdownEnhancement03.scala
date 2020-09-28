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

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_unix_timestamp")
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

  test("test pushdown function: unix_timestamp() for String") {
    jdbcUpdate(s"create or replace table $test_table_unix_timestamp " +
      s"(s1_standard string, s2_ddMMyy string, s3_dd_MM_yyyy string," +
      s"s4_yyyyMMdd string, s5_yyMMdd string, s6_MMdd string, s7_ddMMyyyy string)")
    // Note: Spark default format is "yyyy-MM-dd HH:mm:ss"
    jdbcUpdate(s"""insert into $test_table_unix_timestamp values
               | ('2017-01-01 12:12:12', '31072020', '31/07/2020',
               | '20200731', '200731', '0731', '31072020')
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
    )

    // resultDF.printSchema()
    // resultDF.show(truncate = false)

    // The expected result is generated when pushdown is disabled.
    val expectedResult = Seq(
      Row(1483272732.toLong, 1596153600.toLong, 1596153600.toLong,
        1596153600.toLong, 1596153600.toLong, 18230400.toLong, 1596153600.toLong
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
         |      'ddMMyyyy' ) ) ) AS "SUBQUERY_1_COL_6"
         |FROM ( SELECT * FROM ( $test_table_unix_timestamp )
         |   AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  test("test pushdown function: unix_timestamp() with timestamp/date") {
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

  override def beforeEach(): Unit = {
    super.beforeEach()
  }
}
// scalastyle:on println

