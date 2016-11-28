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
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

class JoinAggPushdownIntegrationSuite extends IntegrationSuiteBase {

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

    SnowflakeConnectorUtils.enablePushdownSession(sparkSession);
  }

  // Dummy test
  test("Dummy") {
    val df = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", s"$test_table").load()

    assert(df.count == 4)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SnowflakeConnectorUtils.enablePushdownSession(sqlContext.sparkSession);
  }
}