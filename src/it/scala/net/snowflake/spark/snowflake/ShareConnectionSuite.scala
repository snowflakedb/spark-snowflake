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

import java.util.TimeZone
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.TestHook
import org.apache.spark.sql.{DataFrame, SaveMode}

class ShareConnectionSuite extends IntegrationSuiteBase {

  import testImplicits._

  private val test_table1: String = s"test_table_$randomSuffix"
  private val test_temp_table1: String = s"test_temp_table_$randomSuffix"
  private val test_table_write: String = s"test_table_write_$randomSuffix"
  private lazy val localDF: DataFrame = Seq((1000, "str11"), (2000, "str22")).toDF("c1", "c2")

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table1")
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

    jdbcUpdate(s"create or replace table $test_table1(c1 int, c2 string)")
    jdbcUpdate(s"insert into $test_table1 values (100, 'str1'),(200, 'str2')")

    // Run a DataFrane action so that the test result is consistent no matter
    // running one test or the whole suite.
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table1)
      .load()
      .collect()
  }

  test("DataFrame(dbtable) action with connection default (enabled)") {
    val df1 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table1)
      .load()

    var oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    var oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    df1.collect()
    // With connection sharing enabled, JDBC connection count doesn't increases.
    assert(ServerConnection.jdbcConnectionCount.get() == oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)

    val df2 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_PREACTIONS, s"create or replace table $test_temp_table1(c1 int)")
      .option("dbtable", test_table1)
      .option(Parameters.PARAM_POSTACTIONS, s"drop table $test_temp_table1")
      .load()
    // Test with pre/post actions
    oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    df2.collect()
    // With connection sharing enabled, JDBC connection count doesn't increases.
    assert(ServerConnection.jdbcConnectionCount.get() == oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)
  }

  test("DataFrame(query) action with connection default (enabled)") {
    val df1 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", "select 123, 'abc'")
      .load()

    var oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    var oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    df1.collect()
    // With connection sharing enabled, JDBC connection count doesn't increases.
    assert(ServerConnection.jdbcConnectionCount.get() == oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)

    val df2 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_PREACTIONS, s"create or replace table $test_temp_table1(c1 int)")
      .option(Parameters.PARAM_POSTACTIONS, s"drop table $test_temp_table1")
      .option("query", "select 123, 'abc'")
      .load()
    // Test with pre/post actions
    oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    df2.collect()
    // With connection sharing enabled, JDBC connection count doesn't increases.
    assert(ServerConnection.jdbcConnectionCount.get() == oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)
  }

  test("DataFrame save with connection default (enabled)") {
    var oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    var oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    localDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()
    // With connection sharing enabled, JDBC connection count doesn't increases.
    assert(ServerConnection.jdbcConnectionCount.get() == oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)

    // Test with pre/post actions
    oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    localDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_PREACTIONS, s"create or replace table $test_temp_table1(c1 int)")
      .option(Parameters.PARAM_POSTACTIONS, s"drop table $test_temp_table1")
      .option("dbtable", test_table_write)
      .mode(SaveMode.Overwrite)
      .save()
    // With connection sharing enabled, JDBC connection count doesn't increases.
    assert(ServerConnection.jdbcConnectionCount.get() == oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)
  }

  test("negative test: pre/post actions cause not-share-connection") {
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_PREACTIONS, s"use schema ${params.sfSchema}")
      .option("dbtable", test_table1)
      .load()
      .collect()
    var oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    var oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    // Read with the same SfOptions in 2nd time.
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_PREACTIONS, s"use schema ${params.sfSchema}")
      .option("dbtable", test_table1)
      .load()
      .collect()
    // With connection sharing disabled, JDBC connection count increases.
    assert(ServerConnection.jdbcConnectionCount.get() > oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)

    localDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      // preactions are not in white list
      .option(Parameters.PARAM_PREACTIONS, s"use schema ${params.sfSchema}")
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()
    oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    // new JDBC connection even for the 2nd write for same sfOptions
    localDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      // preactions are not in white list
      .option(Parameters.PARAM_PREACTIONS, s"use schema ${params.sfSchema}")
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()
    // With connection sharing disabled, JDBC connection count increases.
    assert(ServerConnection.jdbcConnectionCount.get() > oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)

    oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    localDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      // post actions are not in white list
      .option(Parameters.PARAM_POSTACTIONS, s"use schema ${params.sfSchema}")
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()
    // With connection sharing disabled, JDBC connection count increases.
    assert(ServerConnection.jdbcConnectionCount.get() > oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)
  }

  test("negative test: disable with spark connector options") {
    val sfOptionsWithoutSharingConnection =
      connectorOptionsNoTable ++ Map(Parameters.PARAM_SUPPORT_SHARE_CONNECTION -> "off")

    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsWithoutSharingConnection)
      .option("dbtable", test_table1)
      .load()
      .collect()
    var oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    var oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    // new JDBC connection is created even if collect() is called in second time
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsWithoutSharingConnection)
      .option("dbtable", test_table1)
      .load()
      .collect()
    // With connection sharing disabled, JDBC connection count increases.
    assert(ServerConnection.jdbcConnectionCount.get() > oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)

    // Skip the first save
    localDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsWithoutSharingConnection)
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()
    oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    localDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsWithoutSharingConnection)
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()
    // new JDBC connection is created even if same SfOptions is used to save
    assert(ServerConnection.jdbcConnectionCount.get() > oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)
  }

  test("negative test: disable with SnowflakeConnectorUtils.disableSharingJDBCConnection()") {
    var oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    var oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    SnowflakeConnectorUtils.disableSharingJDBCConnection()
    // new JDBC connection is created even if collect() is called in second time
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table1)
      .load()
      .collect()
    // With connection sharing disabled, JDBC connection count increases.
    assert(ServerConnection.jdbcConnectionCount.get() > oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)

    oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    SnowflakeConnectorUtils.enableSharingJDBCConnection()
    // JDBC connection is reused after changing back: enableSharingJDBCConnection()
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table1)
      .load()
      .collect()
    // With connection sharing enable, JDBC connection count is equal.
    assert(ServerConnection.jdbcConnectionCount.get() == oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)

    oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    SnowflakeConnectorUtils.disableSharingJDBCConnection()
    // new JDBC connection is created even if collect() is called in second time
    localDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()
    // new JDBC connection is created even if same SfOptions is used to save
    assert(ServerConnection.jdbcConnectionCount.get() > oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)

    oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    SnowflakeConnectorUtils.enableSharingJDBCConnection()
    // new JDBC connection is created even if collect() is called in second time
    localDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()
    // new JDBC connection is created even if same SfOptions is used to save
    assert(ServerConnection.jdbcConnectionCount.get() == oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)
  }

  test("test force_skip_pre_post_action_check_for_session_sharing") {
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_PREACTIONS, s"use schema ${params.sfSchema}")
      .option(Parameters.PARAM_FORCE_SKIP_PRE_POST_ACTION_CHECK_FOR_SESSION_SHARING, "false")
      .option("dbtable", test_table1)
      .load()
      .collect()

    // case 1: READ with force_skip_pre_post_action_check_for_session_sharing = false
    var oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    var oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    // Read with the same SfOptions in 2nd time.
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_PREACTIONS, s"use schema ${params.sfSchema}")
      .option(Parameters.PARAM_FORCE_SKIP_PRE_POST_ACTION_CHECK_FOR_SESSION_SHARING, "false")
      .option("dbtable", test_table1)
      .load()
      .collect()
    // JDBC connection count increases.
    assert(ServerConnection.jdbcConnectionCount.get() > oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)

    // case 2: WRITE with force_skip_pre_post_action_check_for_session_sharing = false
    oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    localDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      // preactions are not in white list
      .option(Parameters.PARAM_PREACTIONS, s"use schema ${params.sfSchema}")
      .option(Parameters.PARAM_FORCE_SKIP_PRE_POST_ACTION_CHECK_FOR_SESSION_SHARING, "false")
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()
    // JDBC connection count increases.
    assert(ServerConnection.jdbcConnectionCount.get() > oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)

    // case 3: READ with force_skip_pre_post_action_check_for_session_sharing = true
    oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    localDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      // preactions are not in white list
      .option(Parameters.PARAM_PREACTIONS, s"use schema ${params.sfSchema}")
      .option(Parameters.PARAM_FORCE_SKIP_PRE_POST_ACTION_CHECK_FOR_SESSION_SHARING, "true")
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()
    // With force_skip_pre_post_action_check_for_session_sharing = true,
    // JDBC connection count is the same.
    assert(ServerConnection.jdbcConnectionCount.get() == oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)

    // case 4: WRITE with force_skip_pre_post_action_check_for_session_sharing = true
    oldJdbcConnectionCount = ServerConnection.jdbcConnectionCount.get()
    oldServerConnectionCount = ServerConnection.serverConnectionCount.get()
    localDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      // post actions are not in white list
      .option(Parameters.PARAM_POSTACTIONS, s"use schema ${params.sfSchema}")
      .option(Parameters.PARAM_FORCE_SKIP_PRE_POST_ACTION_CHECK_FOR_SESSION_SHARING, "true")
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()
    // With connection sharing disabled, JDBC connection count increases.
    assert(ServerConnection.jdbcConnectionCount.get() == oldJdbcConnectionCount)
    assert(ServerConnection.serverConnectionCount.get() > oldServerConnectionCount)
  }

  test("abort_detached_query") {
    // run dummy query to create session
    sparkSession
      .read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", "select 1")
      .load()
      .show()
    val conn1 = ServerConnection
      .getServerConnection(Parameters.mergeParameters(connectorOptions))
    val result1 = conn1.jdbcConnection
      .prepareStatement("show parameters like 'abort_detached_query'")
      .executeQuery()
    assert(result1.next())
    assert(result1.getString(1).equals("ABORT_DETACHED_QUERY"))
    assert(!result1.getBoolean(2))

    // run dummy query to create session
    sparkSession
      .read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("abort_detached_query", "true")
      .option("query", "select 1")
      .load()
      .show()
    val conn2 = ServerConnection
      .getServerConnection(Parameters
        .mergeParameters(connectorOptions + ("abort_detached_query" -> "true")))
    val result2 = conn2.jdbcConnection
      .prepareStatement("show parameters like 'abort_detached_query'")
      .executeQuery()
    assert(result2.next())
    assert(result2.getString(1).equals("ABORT_DETACHED_QUERY"))
    assert(result2.getBoolean(2))
  }
}
