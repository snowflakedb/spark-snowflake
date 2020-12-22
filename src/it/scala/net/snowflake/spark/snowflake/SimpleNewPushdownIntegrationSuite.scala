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
  private val test_table2 = s"test_table_simple2_$randomSuffix"
  private val test_table3 = s"test_table_simple3_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"create or replace table $test_table(i int, s string)")
    jdbcUpdate(s"create or replace table $test_table2(o int, p int)")
    jdbcUpdate(s"create or replace table $test_table3(o string, p int)")
    jdbcUpdate(
      s"insert into $test_table values(null, 'Hello'), (2, 'Snowflake'), (3, 'Spark'), (4, null)"
    )
    jdbcUpdate(
      s"insert into $test_table2 values(null, 1), (2, 2), (3, 2), (4, 3)"
    )
    jdbcUpdate(
      s"insert into $test_table3 values('hi', 1), ('hi', 2), ('bye', 2), ('bye', 3)"
    )

    SnowflakeConnectorUtils.enablePushdownSession(sparkSession)

    val df1 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", s"$test_table")
      .load()

    val df2 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", s"$test_table2")
      .load()

    val df3 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", s"$test_table3")
      .load()

    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
    df3.createOrReplaceTempView("df3")
  }

  test("Basic join") {

    val result = sparkSession.sql("""
  SELECT first.s,
         second.p
  FROM df1 first
  JOIN df2 second
  ON first.i = second.p""".stripMargin)

    testPushdown(
      s"""SELECT ("subquery_5"."subquery_5_col_1") AS "subquery_6_col_0",
          ("subquery_5"."subquery_5_col_2") AS "subquery_6_col_1" FROM
	(SELECT ("subquery_1"."I") AS "subquery_5_col_0", ("subquery_1"."S")
	AS "subquery_5_col_1", ("subquery_4"."subquery_4_col_0") AS "subquery_5_col_2" FROM
		(SELECT * FROM
			(SELECT * FROM ($test_table) AS "sf_connector_query_alias"
		) AS "subquery_0"
	 WHERE ("subquery_0"."I" IS NOT NULL)
	) AS "subquery_1"
 INNER JOIN
	(SELECT ("subquery_3"."P") AS "subquery_4_col_0" FROM
		(SELECT * FROM
			(SELECT * FROM ($test_table2) AS "sf_connector_query_alias"
			) AS "subquery_2"
		 WHERE ("subquery_2"."P" IS NOT NULL)
		) AS "subquery_3"
	) AS "subquery_4"
 ON ("subquery_1"."I" = "subquery_4"."subquery_4_col_0")
) AS "subquery_5"""".stripMargin,
      result,
      Seq(Row("Snowflake", 2), Row("Snowflake", 2), Row("Spark", 3))
    )
  }

  test("Basic aggregation") {
    val result =
      sparkSession.sql("""
        select p, count(distinct o) as avg from df2
        group by p
      """.stripMargin)

    testPushdown(s"""SELECT ("subquery_0"."P") AS "subquery_1_col_0",
         |(COUNT(DISTINCT "subquery_0"."O")) AS "subquery_1_col_1"
          |FROM
          |	(SELECT * FROM ($test_table2) AS "sf_connector_query_alias"
          |) AS "subquery_0"
          | GROUP BY "subquery_0"."P"
      """.stripMargin, result, Seq(Row(1, 0), Row(2, 2), Row(3, 1)))
  }

  test("Basic filters") {
    val result =
      sparkSession.sql("""
        select * from df2
        where p > 1 AND p < 3
      """.stripMargin)

    testPushdown(
      s"""SELECT * FROM
                     |	(SELECT * FROM ($test_table2) AS "sf_connector_query_alias"
                     |) AS "subquery_0"
                     | WHERE ((("subquery_0"."P" IS NOT NULL) AND ("subquery_0"."P" > 1)) AND ("subquery_0"."P" < 3))
      """.stripMargin,
      result,
      Seq(Row(2, 2), Row(3, 2))
    )
  }

  test("LIMIT and SORT") {
    val result =
      sparkSession.sql("""
        select * from df2
        where p > 1 AND p < 3 order by p desc limit 1
      """.stripMargin)

    testPushdown(
      s"""SELECT * FROM
                     |	(SELECT * FROM
                     |		(SELECT * FROM
                     |			(SELECT * FROM ($test_table2) AS "sf_connector_query_alias"
                     |		) AS "subquery_0"
                     |	 WHERE ((("subquery_0"."P" IS NOT NULL) AND ("subquery_0"."P" > 1)) AND ("subquery_0"."P" < 3))
                     |	) AS "subquery_1"
                     | ORDER BY ("subquery_1"."P") DESC
                     |) AS "subquery_2"
                     | LIMIT 1
      """.stripMargin,
      result,
      Seq(Row(2, 2))
    )
  }

  test("Nested query with column alias") {
    val result =
      sparkSession.sql("""
        select f, o from (select o, p as f from df2
        where p > 1 AND p < 3) as foo order by f,o desc
      """.stripMargin)

    testPushdown(
      s"""SELECT * FROM
                     |	(SELECT ("subquery_1"."P") AS "subquery_2_col_0", ("subquery_1"."O") AS "subquery_2_col_1"
                     | FROM
                     |		(SELECT * FROM
                     |			(SELECT * FROM ($test_table2) AS "sf_connector_query_alias"
                     |		) AS "subquery_0"
                     |	 WHERE ((("subquery_0"."P" IS NOT NULL) AND ("subquery_0"."P" > 1)) AND ("subquery_0"."P" < 3))
                     |	) AS "subquery_1"
                     |) AS "subquery_2"
                     | ORDER BY ("subquery_2"."subquery_2_col_0") ASC, ("subquery_2"."subquery_2_col_1") DESC
      """.stripMargin,
      result,
      Seq(Row(2, 3), Row(2, 2))
    )
  }

  test("Sum and RPAD") {
    val result =
      sparkSession.sql(
        """
        select sum(i) as hi, rpad(s,10,"*") as ho from df1 group by s
                       """.stripMargin
      )

    testPushdown(
      s"""SELECT (SUM("subquery_0"."I")) AS "subquery_1_col_0", (RPAD("subquery_0"."S", 10, '*')) AS
          |"subquery_1_col_1" FROM
          |	(SELECT * FROM ($test_table) AS "sf_connector_query_alias"
          |) AS "subquery_0"
          | GROUP BY "subquery_0"."S"
      """.stripMargin,
      result,
      Seq(
        Row(null, "Hello*****"),
        Row(2, "Snowflake*"),
        Row(3, "Spark*****"),
        Row(4, null)
      )
    )

  }

  test("row_number() window function") {

    val input = sparkSession.sql(s"""
        |select * from df3
        |""".stripMargin)

    val windowSpec =
      Window.partitionBy(new Column("o")).orderBy(new Column("p").desc)
    val result =
      input.withColumn("rank", row_number.over(windowSpec)).filter("rank=1")

    testPushdown(
      s"""SELECT * FROM (SELECT ("SUBQUERY_0"."O") AS "SUBQUERY_1_COL_0",
          | ("SUBQUERY_0"."P") AS"SUBQUERY_1_COL_1", (ROW_NUMBER() OVER
          | (PARTITIONBY "SUBQUERY_0"."O" ORDERBY ("SUBQUERY_0"."P") DESC))
          | AS "SUBQUERY_1_COL_2" FROM (SELECT * FROM ($test_table3)
          | AS "SF_CONNECTOR_QUERY_ALIAS") AS "SUBQUERY_0") AS "SUBQUERY_1" WHERE
          | (("SUBQUERY_1"."SUBQUERY_1_COL_2" ISNOTNULL) AND ("SUBQUERY_1"."SUBQUERY_1_COL_2" = 1))
      """.stripMargin,
      result,
      Seq(Row("bye", 3, 1), Row("hi", 2, 1))
    )
  }

  test("test binary arithmetic operators on Decimal") {
    // This test can be run manually in non-pushdown mode,
    // The expected result is the same, if the query is not pushdown,
    // pass by the expected sql check.
    val disablePushDown = false
    if (disablePushDown) {
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
    }

    // Test addition(+)
    var operator = "+"
    var result =
      sparkSession.sql(s"select o $operator p from df2 where o IS NOT NULL")

    testPushdown(
      s"""SELECT ( CAST ( ( "SUBQUERY_1"."O" $operator "SUBQUERY_1"."P" )
                    |AS DECIMAL(38, 0) ) ) AS "SUBQUERY_2_COL_0" FROM
                    |( SELECT * FROM ( SELECT * FROM ( $test_table2 ) AS
                    |"SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
                    |( "SUBQUERY_0"."O" IS NOT NULL ) ) AS "SUBQUERY_1"
      """.stripMargin,
      result,
      // Data in df2 (o, p) values(null, 1), (2, 2), (3, 2), (4, 3)
      Seq(Row(4), Row(5), Row(7)),
      disablePushDown
    )

    // Test subtract(+)
    operator = "-"
    result =
      sparkSession.sql(s"select o $operator p from df2 where o IS NOT NULL")

    testPushdown(
      s"""SELECT ( CAST ( ( "SUBQUERY_1"."O" $operator "SUBQUERY_1"."P" )
                    |AS DECIMAL(38, 0) ) ) AS "SUBQUERY_2_COL_0" FROM
                    |( SELECT * FROM ( SELECT * FROM ( $test_table2 ) AS
                    |"SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
                    |( "SUBQUERY_0"."O" IS NOT NULL ) ) AS "SUBQUERY_1"
      """.stripMargin,
      result,
      // Data in df2 (o, p) values(null, 1), (2, 2), (3, 2), (4, 3)
      Seq(Row(0), Row(1), Row(1)),
      disablePushDown
    )

    // Test multiply(*)
    operator = "*"
    result =
      sparkSession.sql(s"select o $operator p from df2 where o IS NOT NULL")

    testPushdown(
      s"""SELECT ( CAST ( ( "SUBQUERY_1"."O" $operator "SUBQUERY_1"."P" )
                    |AS DECIMAL(38, 0) ) ) AS "SUBQUERY_2_COL_0" FROM
                    |( SELECT * FROM ( SELECT * FROM ( $test_table2 ) AS
                    |"SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
                    |( "SUBQUERY_0"."O" IS NOT NULL ) ) AS "SUBQUERY_1"
      """.stripMargin,
      result,
      // Data in df2 (o, p) values(null, 1), (2, 2), (3, 2), (4, 3)
      Seq(Row(4), Row(6), Row(12)),
      disablePushDown
    )

    // Test division(/)
    operator = "/"
    result =
      sparkSession.sql(s"select o $operator p from df2 where o IS NOT NULL")

    testPushdown(
      s"""SELECT ( CAST ( ( "SUBQUERY_1"."O" $operator "SUBQUERY_1"."P" )
                    |AS DECIMAL(38, 6) ) ) AS "SUBQUERY_2_COL_0" FROM
                    |( SELECT * FROM ( SELECT * FROM ( $test_table2 ) AS
                    |"SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
                    |( "SUBQUERY_0"."O" IS NOT NULL ) ) AS "SUBQUERY_1"
      """.stripMargin,
      result,
      // Data in df2 (o, p) values(null, 1), (2, 2), (3, 2), (4, 3)
      Seq(Row(1.000000), Row(1.333333), Row(1.500000)),
      disablePushDown
    )

    // Test mod(%)
    operator = "%"
    result =
      sparkSession.sql(s"select o $operator p from df2 where o IS NOT NULL")

    testPushdown(
      s"""SELECT ( CAST ( ( "SUBQUERY_1"."O" $operator "SUBQUERY_1"."P" )
                    |AS DECIMAL(38, 0) ) ) AS "SUBQUERY_2_COL_0" FROM
                    |( SELECT * FROM ( SELECT * FROM ( $test_table2 ) AS
                    |"SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" WHERE
                    |( "SUBQUERY_0"."O" IS NOT NULL ) ) AS "SUBQUERY_1"
      """.stripMargin,
      result,
      // Data in df2 (o, p) values(null, 1), (2, 2), (3, 2), (4, 3)
      Seq(Row(0), Row(1), Row(1)),
      disablePushDown
    )

    // Test unary minus( -(number) )
    operator = "-"
    result = sparkSession.sql(
      s"select -o, - o + p, - o - p, - ( o + p ), - 3 + o from df2 where o IS NOT NULL"
    )

    testPushdown(
      s"""SELECT ( - ( "SUBQUERY_1"."O" ) ) AS "SUBQUERY_2_COL_0" , ( CAST (
                    |( - ( "SUBQUERY_1"."O" ) + "SUBQUERY_1"."P" ) AS DECIMAL(38, 0) ) )
                    |AS "SUBQUERY_2_COL_1" , ( CAST ( ( - ( "SUBQUERY_1"."O" ) - "SUBQUERY_1"."P" )
                    |AS DECIMAL(38, 0) ) ) AS "SUBQUERY_2_COL_2" , ( - ( CAST ( ( "SUBQUERY_1"."O"
                    |+ "SUBQUERY_1"."P" ) AS DECIMAL(38, 0) ) ) ) AS "SUBQUERY_2_COL_3" , ( CAST ( (
                    |-3 + "SUBQUERY_1"."O" ) AS DECIMAL(38, 0) ) ) AS "SUBQUERY_2_COL_4" FROM ( SELECT
                    |* FROM ( SELECT * FROM ( $test_table2 ) AS "SF_CONNECTOR_QUERY_ALIAS" ) AS
                    |"SUBQUERY_0" WHERE ( "SUBQUERY_0"."O" IS NOT NULL ) ) AS "SUBQUERY_1"
      """.stripMargin,
      result,
      // Data in df2 (o, p) values(2, 2), (3, 2), (4, 3)
      Seq(
        Row(-2, 0, -4, -4, -1),
        Row(-3, -1, -5, -5, 0),
        Row(-4, -1, -7, -7, 1)
      ),
      disablePushDown
    )

    // Set back.
    if (disablePushDown) {
      SnowflakeConnectorUtils.enablePushdownSession(sparkSession)
    }
  }

  test("test POWER function") {
    // This test can be run manually in non-pushdown mode,
    // The expected result is the same, if the query is not pushdown,
    // pass by the expected sql check.
    val disablePushDown = false
    if (disablePushDown) {
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
    }

    val result =
      sparkSession.sql(s"select power(2, p), pow(p, 3), pow(p, p) from df2")

    testPushdown(
      s"""SELECT ( POWER ( 2.0 , CAST ( "SUBQUERY_0"."P" AS DOUBLE
                    |) ) ) AS "SUBQUERY_1_COL_0" , ( POWER ( CAST (
                    |"SUBQUERY_0"."P" AS DOUBLE ) , 3.0 ) ) AS "SUBQUERY_1_COL_1"
                    |, ( POWER ( CAST ( "SUBQUERY_0"."P" AS DOUBLE ) , CAST
                    |( "SUBQUERY_0"."P" AS DOUBLE ) ) ) AS "SUBQUERY_1_COL_2"
                    |FROM ( SELECT * FROM ( $test_table2 ) AS
                    |"SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
      """.stripMargin,
      result,
      // Data in df2 (o, p) values(null, 1), (2, 2), (3, 2), (4, 3)
      Seq(
        Row(2.0, 1.0, 1.0),
        Row(4.0, 8.0, 4.0),
        Row(4.0, 8.0, 4.0),
        Row(8.0, 27.0, 27.0)
      ),
      disablePushDown
    )

    // Set back.
    if (disablePushDown) {
      SnowflakeConnectorUtils.enablePushdownSession(sparkSession)
    }
  }

  test("test EXP function") {
    // This test can be run manually in non-pushdown mode,
    // The expected result is the same, if the query is not pushdown,
    // pass by the expected sql check.
    val disablePushDown = false
    // EXP of Snowflake supports 9 significant digits for COPY UNLOAD
    var expResultSet = Seq(
      Row(1.0, 2.718281828),
      Row(1.0, 7.389056099),
      Row(1.0, 7.389056099),
      Row(1.0, 20.085536923)
    )

    // EXP of Spark supports 15 significant digits
    // EXP with Snowflake SELECT query supports 15 significant digits
    if (!params.useCopyUnload || disablePushDown) {
      expResultSet = Seq(
        Row(1.0, 2.718281828459045),
        Row(1.0, 7.38905609893065),
        Row(1.0, 7.38905609893065),
        Row(1.0, 20.085536923187668)
      )
    }

    if (disablePushDown) {
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
    }

    val result =
      sparkSession.sql(s"select exp(0), exp(p) from df2")

    testPushdown(s"""SELECT ( 1.0 ) AS "SUBQUERY_1_COL_0" , (
                    |EXP ( CAST ( "SUBQUERY_0"."P" AS DOUBLE ) ) )
                    |AS "SUBQUERY_1_COL_1" FROM ( SELECT * FROM (
                    |$test_table2 ) AS "SF_CONNECTOR_QUERY_ALIAS" )
                    |AS "SUBQUERY_0"
      """.stripMargin, result, expResultSet, disablePushDown, testPushdownOff = false)

    // Set back.
    if (disablePushDown) {
      SnowflakeConnectorUtils.enablePushdownSession(sparkSession)
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table")
      jdbcUpdate(s"drop table if exists $test_table2")
      jdbcUpdate(s"drop table if exists $test_table3")
    } finally {
      super.afterAll()
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
    }
  }
}
