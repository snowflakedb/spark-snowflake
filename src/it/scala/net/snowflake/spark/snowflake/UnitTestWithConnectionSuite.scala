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

import java.io.File
import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.{TestHook, TestHookFlag}
import net.snowflake.spark.snowflake._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalactic.source.Position
import org.scalatest.Tag

import scala.util.Random

// scalastyle:off println
/*
 * This test suite includes for misc unit test which needs connection to snowflake.
 */
class UnitTestWithConnectionSuite extends IntegrationSuiteBase {
  // This test suite only run when env EXTRA_TEST_FOR_COVERAGE is set as true
  override def test(testName: String, testTags: Tag*)(
    testFun: => Any
  )(implicit pos: Position): Unit = {
    if (extraTestForCoverage) {
      super.test(testName, testTags: _*)(testFun)(pos)
    } else {
      super.ignore(testName, testTags: _*)(testFun)(pos)
    }
  }

  private val largeStringValue = Random.alphanumeric take 1024 mkString ""
  private val test_table_large_result: String =
    s"test_table_large_result_$randomSuffix"
  private val LARGE_TABLE_ROW_COUNT = 1000

  private def setupLargeResultTable(sfOptions: Map[String, String]): Unit = {
    val param = Parameters.MergedParameters(sfOptions)
    val connection = DefaultJDBCWrapper.getConnector(param)

    connection.createStatement.executeQuery(
      s"""create or replace table $test_table_large_result (
         | int_c int, c_string string(1024) )""".stripMargin
    )

    connection.createStatement.executeQuery(
      s"""insert into $test_table_large_result select
      | seq4(), '$largeStringValue'
      | from table(generator(rowcount => $LARGE_TABLE_ROW_COUNT))
      | """.stripMargin
    )

    connection.close()
  }

  lazy private val javaSfOptionsNoTable: java.util.Map[String, String] = {
    val javaMap  = new java.util.HashMap[String, String]()
    connectorOptionsNoTable.foreach(tup => {javaMap.put(tup._1, tup._2)})
    javaMap
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    SnowflakeConnectorUtils.setPushdownSession(sparkSession, true)

    setupLargeResultTable(connectorOptionsNoTable)
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_large_result")
    } finally {
      TestHook.disableTestHook()
      SnowflakeConnectorUtils.setPushdownSession(sparkSession, false)
      super.afterAll()
    }
  }

  test("test Utils.printQuery") {
    Utils.printQuery(connectorOptionsNoTable,
      s"select * from $test_table_large_result limit 10")
  }

  test("test Utils functions for java.util.Map version") {
    // runQuery(params: java.util.Map[String, String], query: String): ResultSet
    val rs1 = Utils.runQuery(javaSfOptionsNoTable,
      s"select count(*) from $test_table_large_result")
    assert(rs1.next())
    assert(rs1.getInt(1).equals(LARGE_TABLE_ROW_COUNT))

    // getJDBCConnection(params: java.util.Map[String, String])
    val connection = Utils.getJDBCConnection(javaSfOptionsNoTable)
    try {
      val stmt = connection.createStatement()
      val rs2 = stmt.executeQuery(s"select count(*) from $test_table_large_result")
      assert(rs2.next())
      assert(rs2.getInt(1).equals(LARGE_TABLE_ROW_COUNT))
    } finally {
      connection.close()
    }
  }

  test("test DefaultJDBCWrapper functions") {
    val connection = Utils.getJDBCConnection(javaSfOptionsNoTable)
    try {
      val jdbcWrapper = new JDBCWrapper
      assert(jdbcWrapper.executePreparedInterruptibly(connection,
        s"select count(*) from $test_table_large_result"))

      val rs1 = jdbcWrapper.executePreparedQueryInterruptibly(connection,
        s"select count(*) from $test_table_large_result")
      assert(rs1.next())
      assert(rs1.getInt(1).equals(LARGE_TABLE_ROW_COUNT))
    } finally {
      connection.close()
    }
  }

}
// scalastyle:on println
