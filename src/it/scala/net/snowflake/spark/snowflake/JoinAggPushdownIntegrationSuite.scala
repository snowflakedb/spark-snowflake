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
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, GenericMutableRow, SpecificMutableRow, UnsafeRow}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}

class JoinAggPushdownIntegrationSuite extends IntegrationSuiteBase {

  private val test_table: String = s"test_table_$randomSuffix"
  val join_table = test_table + "j"

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
    jdbcUpdate(s"create or replace table $join_table(o int, p int)")
    jdbcUpdate(s"insert into $test_table values(null, 'Hello'), (2, 'Snowflake'), (3, 'Spark'), (4, null)")
    jdbcUpdate(s"insert into $join_table values(null, 1), (2, 2), (3, 2), (4, 3)")

    //SnowflakeConnectorUtils.enablePushdownSession(sparkSession);

     val df1 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", s"$test_table").load()

     val df2 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", s"$join_table").load()

      df1.createOrReplaceTempView("df1")
      df2.createOrReplaceTempView("df2")
  }

  /*
  // Dummy test
  test("Basic join") {

    val joinedResult = sparkSession.sql("""
  SELECT first.s,
         second.p
  FROM df1 first
  JOIN df2 second
  ON first.i = second.p""")

    joinedResult.show()
  }
*/
  // Dummy test
  test("Basic aggregation") {

    val sumDF = sparkSession.sql("""
  SELECT sum(o) as sum, p
  FROM df2
  GROUP BY p""")

    sumDF.show()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SnowflakeConnectorUtils.disablePushdownSession(sqlContext.sparkSession);
  }
}