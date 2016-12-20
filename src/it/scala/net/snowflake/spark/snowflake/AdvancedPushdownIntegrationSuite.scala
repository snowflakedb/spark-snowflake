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
import org.apache.spark.sql.{DataFrame, Row}

class AdvancedPushdownIntegrationSuite extends IntegrationSuiteBase {

  private val test_table: String = s"test_table_$randomSuffix"
  val test_table2                = test_table + "j"

  // Values used for comparison
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "Snowflake")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)

  // Join Table
  private val row1b = Row(null, 1)
  private val row2b = Row(2, 2)
  private val row3b = Row(3, 2)
  private val row4b = Row(4, 3)

  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"create or replace table $test_table(i int, s string)")
    jdbcUpdate(s"create or replace table $test_table2(o int, p int)")
    jdbcUpdate(
      s"insert into $test_table values(null, 'Hello'), (2, 'Snowflake'), (3, 'Spark'), (4, null)")
    jdbcUpdate(
      s"insert into $test_table2 values(null, 1), (2, 2), (3, 2), (4, 3)")

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

    df1.createOrReplaceTempView("df1")
    df2.createOrReplaceTempView("df2")
  }

  test("Basic join") {

    val joinedResult = sparkSession.sql("""
  SELECT first.s,
         second.p
  FROM df1 first
  JOIN df2 second
  ON first.i = second.p""")

    testPushdown(
      s"""SELECT "subquery_5"."S", "subquery_5"."P" FROM
      (SELECT * FROM
      (SELECT * FROM
      (SELECT * FROM $test_table) AS "subquery_0"
        WHERE ("subquery_0"."I" IS NOT NULL)) AS "subquery_1"
    INNER JOIN
      (SELECT "subquery_3"."P" FROM
      (SELECT * FROM
      (SELECT * FROM $test_table2) AS "subquery_2"
        WHERE ("subquery_2"."P" IS NOT NULL)) AS "subquery_3") AS "subquery_4"
    ON ("subquery_1"."I"="subquery_4"."P")) AS "subquery_5"""",
      joinedResult,
      Seq(Row("Snowflake", 2), Row("Snowflake", 2), Row("Spark", 3)))
  }

  test("Basic aggregation") {
    val aggResult =
      sparkSession.sql("""
        select p, count(distinct o) as avg from df2
        group by p
      """.stripMargin)

    testPushdown(
      s"""SELECT "subquery_0"."P", (count(DISTINCT "subquery_0"."O")) AS "avg" FROM
          (SELECT * FROM $test_table2) AS "subquery_0" GROUP BY "subquery_0"."P"
      """,
      aggResult,
      Seq(Row(1, 0), Row(2, 2), Row(3, 1)))
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SnowflakeConnectorUtils.disablePushdownSession(sqlContext.sparkSession)
  }

  /**
    * Verify that the pushdown was done by looking at the generated SQL,
    * and check the results are as expected
    */
  def testPushdown(reference: String,
                   result: DataFrame,
                   expectedAnswer: Seq[Row]): Unit = {
    // Clean reference string
    val ref: String = reference.trim.replaceAll(" +", " ").replaceAll("\n", "")

    checkAnswer(result, expectedAnswer)
    // Verify the query issued is what we expect
    val o = Utils.getLastSelect
  //  assert(Utils.getLastSelect == ref)
      assert(o == ref)
  }
}
