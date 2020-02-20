/*
 * Copyright 2015-2016 Snowflake Computing
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

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.functions.row_number

class SimpleNewPushdownIntegrationSuite extends IntegrationSuiteBase {

  private val test_table: String = s"test_table_simple_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"create or replace table $test_table(d timestamp)")

    jdbcUpdate(
      s"insert into $test_table values('2018-06-08 12:09:20.163-07:00'), ('2005-03-08 13:12:10.115-07:00'), ('1999-09-06 12:24:20.145-07:00'), ('1492-11-03 04:47:20.194-07:00')"
    )

    SnowflakeConnectorUtils.enablePushdownSession(sparkSession)

    val df1 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", s"$test_table")
      .load()

    df1.createOrReplaceTempView("df1")

  }


  test("Basic Truncate Date") {
    val result =
      sparkSession.sql("""
      select D as "DATE",
       date_trunc("YEAR", "DATE") as "TRUNCATED TO YEAR",
       date_trunc("MONTH", "DATE") as "TRUNCATED TO MONTH",
       date_trunc("DAY", "DATE") as "TRUNCATED TO DAY"
       from df1;
      """.stripMargin)

    testPushdown(s"""SELECT ("subquery_0"."D") AS "subquery_1_col_0",
          | date_trunc("YEAR", "subquery_0"."D") AS "subquery_1_col_1"
          | date_trunc("MONTH", "subquery_0"."D") AS "subquery_1_col_2"
          | date_trunc("DAY", "subquery_0"."D") AS "subquery_1_col_3"
          |FROM
          |	(SELECT * FROM ($test_table) AS "sf_connector_query_alias"
          |) AS "subquery_0"
      """.stripMargin,
      result,
      Seq(
        Row("2018-06-08 12:09:20.163","2018-01-01 00:00:00.000","2018-06-01 00:00:00.000","2018-06-08 00:00:00.000"),
        Row("2005-03-08 13:12:10.115","2005-01-01 00:00:00.000","2005-03-01 00:00:00.000","2005-03-08 00:00:00.000"),
        Row("1999-09-06 12:24:20.145","1999-01-01 00:00:00.000","1999-09-01 00:00:00.000","1999-09-06 00:00:00.000"),
        Row("1492-11-03 04:47:20.194","1492-01-01 00:00:00.000","1492-11-01 00:00:00.000","1492-11-03 00:00:00.000"))

    )
  }


  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table")
    } finally {
      super.afterAll()
      SnowflakeConnectorUtils.disablePushdownSession(sqlContext.sparkSession)
    }
  }
}
