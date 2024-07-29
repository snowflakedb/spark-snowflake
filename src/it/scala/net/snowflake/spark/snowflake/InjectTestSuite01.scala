/*
 * Copyright 2015-2020 Snowflake Computing
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
import net.snowflake.spark.snowflake.test.{TestHook, TestHookFlag}

import scala.util.Random

// scalastyle:off println
class InjectTestSuite01 extends IntegrationSuiteBase {
  private val test_table_write: String = s"test_table_write_$randomSuffix"
  private val internal_stage_name = s"test_stage_$randomSuffix"

  private val largeStringValue = Random.alphanumeric take 1024 mkString ""
  private val test_table_basic: String = s"test_table_basic_$randomSuffix"
  private val LARGE_TABLE_ROW_COUNT = 1000

  private def setupLargeResultTable(sfOptions: Map[String, String]): Unit = {
    val param = Parameters.MergedParameters(sfOptions)
    val connection = TestUtils.getServerConnection(param)

    connection.createStatement.executeQuery(
      s"""create or replace table $test_table_basic (
         | int_c int, c_string string(1024) )""".stripMargin
    )

    connection.createStatement.executeQuery(
      s"""insert into $test_table_basic select
      | seq4(), '$largeStringValue'
      | from table(generator(rowcount => $LARGE_TABLE_ROW_COUNT))
      | """.stripMargin
    )

    connection.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    setupLargeResultTable(connectorOptionsNoTable)
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_basic")
    } finally {
      TestHook.disableTestHook()
      super.afterAll()
    }
  }

  test("inject exceptions for Arrow read") {
    try {
      if (!params.useCopyUnload) {
        // Enable test hook to simulate error when closing a result set on driver.
        // This exception doesn't affect the final result
        TestHook.enableTestFlagOnly(TestHookFlag.TH_ARROW_DRIVER_FAIL_CLOSE_RESULT_SET)
        sparkSession.read
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(connectorOptionsNoTable)
          .option("dbtable", s"$test_table_basic")
          .load()
          .collect()

        // Enable test hook to simulate error when opening a result set.
        TestHook.enableTestFlagOnly(TestHookFlag.TH_ARROW_FAIL_OPEN_RESULT_SET)
        assertThrows[Exception]({
          sparkSession.read
            .format(SNOWFLAKE_SOURCE_NAME)
            .options(connectorOptionsNoTable)
            .option("dbtable", s"$test_table_basic")
            .load()
            .collect()
        })

        // Enable test hook to simulate error when reading a result set.
        TestHook.enableTestFlagOnly(TestHookFlag.TH_ARROW_FAIL_READ_RESULT_SET)
        assertThrows[Exception]({
          sparkSession.read
            .format(SNOWFLAKE_SOURCE_NAME)
            .options(connectorOptionsNoTable)
            .option("dbtable", s"$test_table_basic")
            .load()
            .collect()
        })
      }
    } finally {
      TestHook.disableTestHook()
    }
  }
}
// scalastyle:on println
