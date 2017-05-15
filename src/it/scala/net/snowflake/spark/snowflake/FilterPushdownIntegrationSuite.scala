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

import org.apache.spark.sql.Row

import Utils.SNOWFLAKE_SOURCE_NAME

/**
  * Created by mzukowski on 8/13/16.
  */
class FilterPushdownIntegrationSuite extends IntegrationSuiteBase {

  private val test_table: String = s"test_table_$randomSuffix"

  // Values used for comparison
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "Snowflake")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)

  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"create or replace table $test_table(i int, s string)")
    jdbcUpdate(s"insert into $test_table values(null, 'Hello'), (2, 'Snowflake'), (3, 'Spark'), (4, null)")
  }

  test("Test Simple Comparisons") {
    testFilter("s = 'Hello'", s"""(S IS NOT NULL) AND S = 'Hello'""", Seq(row1))
    testFilter("i > 2", s"""(I IS NOT NULL) AND I > 2""", Seq(row3, row4))
    testFilter("i < 3", s"""(I IS NOT NULL) AND I < 3""", Seq(row2))
  }

  // Doesn't work with Spark 1.4.1
  test("Test >= and <=") {
    testFilter("i >= 2", s"""(I IS NOT NULL) AND I >= 2""", Seq(row2, row3, row4))
    testFilter("i <= 3", s"""(I IS NOT NULL) AND I <= 3""", Seq(row2, row3))
  }

  test("Test logical operators") {
    testFilter("i >= 2 AND i <= 3", s"""(I IS NOT NULL) AND I >= 2 AND I <= 3""", Seq(row2, row3))
    testFilter("NOT i = 3", s"""(I IS NOT NULL) AND (NOT (I = 3))""", Seq(row2, row4))
    testFilter("NOT i = 3 OR i IS NULL",
      s"""(((NOT (I = 3))) OR ((I IS NULL)))""",
      Seq(row1, row2, row4))
    testFilter("i IS NULL OR i > 2 AND s IS NOT NULL",
      s"""(((I IS NULL)) OR (((I > 2) AND ((S IS NOT NULL)))))""",
      Seq(row1, row3))
  }

  test("Test IN") {
    testFilter("i IN (2, 3)", s"""(I IN (2, 3))""", Seq(row2, row3))
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    runSql(
      s"""
         | create temporary table test_table(
         |   i int,
         |   s string
         | )
         | using net.snowflake.spark.snowflake
         | options(
         |   $connectorOptionsString
         |   , dbtable \"$test_table\"
         | )
       """.stripMargin
    )
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table")
    } finally {
      super.afterAll()
    }
  }

  /**
    * Verify that the filter is pushed down by looking at the generated SQL,
    * and check the results are as expected
    */
  def testFilter(filter: String, expectedWhere: String, expectedAnswer: Seq[Row]): Unit = {
    val loadedDf = sqlContext.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", s"$test_table")
      .load().filter(filter).sort("i")
    checkAnswer(loadedDf, expectedAnswer)
    // Verify the query issued is what we expect
    var expectedQuery = s"""SELECT "I", "S" FROM $test_table WHERE $expectedWhere"""
    assert(Utils.getLastSelect == expectedQuery)
  }
}
