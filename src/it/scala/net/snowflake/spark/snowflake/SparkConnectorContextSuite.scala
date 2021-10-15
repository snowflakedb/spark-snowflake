/*
 * Copyright 2015-2021 Snowflake Computing
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

import java.sql.Connection

import net.snowflake.client.jdbc.{SnowflakeConnectionV1, SnowflakeResultSet, SnowflakeStatement}
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class SparkConnectorContextSuite extends IntegrationSuiteBase {
  private val logger = LoggerFactory.getLogger(getClass)

  test("SparkConnectorContext: add/remove running query unit test") {
    val sc = sparkSession.sparkContext
    val appId = sc.applicationId
    val param = Parameters.MergedParameters(connectorOptions)
    val conn = DefaultJDBCWrapper.getConnector(param)
    val conn2 = DefaultJDBCWrapper.getConnector(param)

    // Add one running query
    SparkConnectorContext.addRunningQuery(sc, conn, "test_query_id_1")
    var runningQueries = SparkConnectorContext.getRunningQueries
    assert(runningQueries.size == 1)
    assert(
      runningQueries.contains(appId) &&
        runningQueries(appId).contains(RunningQuery(conn, "test_query_id_1"))
    )
    // Remove one running query
    SparkConnectorContext.removeRunningQuery(sc, conn, "test_query_id_1")
    runningQueries = SparkConnectorContext.getRunningQueries
    assert(runningQueries.size == 1)
    assert(runningQueries(appId).isEmpty)

    // Add 2 running queries
    SparkConnectorContext.addRunningQuery(sc, conn, "test_query_id_1")
    SparkConnectorContext.addRunningQuery(sc, conn2, "test_query_id_2")
    runningQueries = SparkConnectorContext.getRunningQueries
    assert(runningQueries.size == 1)
    assert(runningQueries(appId).size == 2)
    assert(
      runningQueries.contains(appId) &&
        runningQueries(appId).contains(RunningQuery(conn, "test_query_id_1"))
    )
    assert(
      runningQueries.contains(appId) &&
        runningQueries(appId).contains(RunningQuery(conn2, "test_query_id_2"))
    )
    // Remove one running query
    SparkConnectorContext.removeRunningQuery(sc, conn, "test_query_id_1")
    runningQueries = SparkConnectorContext.getRunningQueries
    assert(runningQueries.size == 1)
    assert(runningQueries(appId).size == 1)
    assert(
      runningQueries.contains(appId) &&
        runningQueries(appId).contains(RunningQuery(conn2, "test_query_id_2"))
    )
    // Remove another running query
    SparkConnectorContext.removeRunningQuery(sc, conn2, "test_query_id_2")
    runningQueries = SparkConnectorContext.getRunningQueries
    assert(runningQueries.size == 1)
    assert(runningQueries(appId).isEmpty)

    conn.close()
    conn2.close()
  }

  test("SparkConnectorContext: add running query and cancel query unit test") {
    val sc = sparkSession.sparkContext
    val appId = sc.applicationId
    val param = Parameters.MergedParameters(connectorOptions)
    val conn = DefaultJDBCWrapper.getConnector(param)
    val conn2 = DefaultJDBCWrapper.getConnector(param)

    // Add one running query
    SparkConnectorContext.addRunningQuery(sc, conn, "test_query_id_1")
    var runningQueries = SparkConnectorContext.getRunningQueries
    assert(runningQueries.size == 1)
    assert(
      runningQueries.contains(appId) &&
        runningQueries(appId).contains(RunningQuery(conn, "test_query_id_1"))
    )
    // cancel all running query
    SparkConnectorContext.cancelRunningQueries(appId)
    runningQueries = SparkConnectorContext.getRunningQueries
    assert(runningQueries.isEmpty)

    // Add 2 running queries
    SparkConnectorContext.addRunningQuery(sc, conn, "test_query_id_1")
    SparkConnectorContext.addRunningQuery(sc, conn2, "test_query_id_2")
    runningQueries = SparkConnectorContext.getRunningQueries
    assert(runningQueries.size == 1)
    assert(runningQueries(appId).size == 2)
    assert(
      runningQueries.contains(appId) &&
        runningQueries(appId).contains(RunningQuery(conn, "test_query_id_1"))
    )
    assert(
      runningQueries.contains(appId) &&
        runningQueries(appId).contains(RunningQuery(conn2, "test_query_id_2"))
    )
    // cancel all running query
    SparkConnectorContext.cancelRunningQueries(appId)
    runningQueries = SparkConnectorContext.getRunningQueries
    assert(runningQueries.isEmpty)

    conn.close()
    conn2.close()
  }

  test("SparkConnectorContext: test a running query is canceled by Application End") {
    val sc = sparkSession.sparkContext
    val appId = sc.applicationId
    val param = Parameters.MergedParameters(connectorOptions)
    val conn = DefaultJDBCWrapper.getConnector(param)

    // Execute a 2 minutes query in async mode and get the query ID
    val rs = conn
      .createStatement()
      .asInstanceOf[SnowflakeStatement]
      .executeAsyncQuery("call system$wait(2, 'MINUTES')")
    val queryID = rs.asInstanceOf[SnowflakeResultSet].getQueryID
    val sessionID = getSessionID(conn)

    // Add the running query
    SparkConnectorContext.addRunningQuery(sc, conn, queryID)
    val runningQueries = SparkConnectorContext.getRunningQueries
    assert(runningQueries.size == 1)
    assert(
      runningQueries.contains(appId) &&
        runningQueries(appId).contains(RunningQuery(conn, queryID))
    )

    // Stop the application, it will trigger the Application End event.
    Thread.sleep(5000)
    sparkSession.stop()
    Thread.sleep(5000)

    var (message, _) = getQueryMessage(conn, queryID, sessionID)
    var tryCount: Int = 0
    // Check the query history, and the query must be cancelled
    // There may be latency to get query history
    while (message == null && tryCount < 10) {
      Thread.sleep(10000)
      message = getQueryMessage(conn, queryID, sessionID)._1
      tryCount = tryCount + 1
      logger.warn(s"Retry count: $tryCount, Get query history message: $message")
    }
    assert("SQL execution canceled".equals(message))

    conn.close()

    // Recreate spark session to avoid affect following test cases
    sparkSession = SparkSession.builder
      .master("local")
      .appName("SnowflakeSourceSuite")
      .config("spark.sql.shuffle.partitions", "6")
      // "spark.sql.legacy.timeParserPolicy = LEGACY" is added to allow
      // spark 3.0 to support legacy conversion for unix_timestamp().
      // It may not be necessary for spark 2.X.
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
  }

  private def getSessionID(connection: Connection): String = {
    connection.asInstanceOf[SnowflakeConnectionV1].getSessionID
  }

  private def getQueryMessage(connection: Connection,
                              queryID: String,
                              sessionID: String): (String, String) = {
    val rs = connection
      .createStatement()
      .executeQuery(
        s"select * from table(information_schema.query_history_by_session" +
          s"(SESSION_ID => $sessionID)) where QUERY_ID = '$queryID'"
      )
    if (rs.next()) {
      (rs.getString("ERROR_MESSAGE"), rs.getString("QUERY_TEXT"))
    } else {
      (null, null)
    }
  }

  test("SparkConnectorContext: end-2-end test to cancel a running query") {
    // This test only available for async execution query.
    if (!params.isExecuteQueryWithSyncMode) {
      val sc = sparkSession.sparkContext
      val appId = sc.applicationId
      val param = Parameters.MergedParameters(connectorOptions)
      val conn = DefaultJDBCWrapper.getConnector(param)

      val schema = StructType(
        List(
          StructField("str1", StringType)
        )
      )
      val query = "SELECT SYSTEM$WAIT(2, 'MINUTES')"
      // Execute a 2 minutes query with DataFrame in a Future
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .schema(schema)
        .options(connectorOptionsNoTable)
        .option("query", query)
        .load()
      Future {
        df.collect()
      }

      // We can see one running query sooner or later
      var runningQueries = SparkConnectorContext.getRunningQueries
      var retryCount = 0
      while (retryCount < 20 && !runningQueries.contains(appId)) {
        Thread.sleep(10000)
        retryCount = retryCount + 1
        runningQueries = SparkConnectorContext.getRunningQueries
        logger.info("retry to check running query, retryCount = " + retryCount)
      }
      assert(runningQueries.size == 1)
      assert(runningQueries(appId).size == 1)
      // Get the running query ID for later check
      val queryID = runningQueries(appId).head.queryID
      val sessionID = getSessionID(runningQueries(appId).head.conn)

      // Stop the application, it will trigger the Application End event.
      sparkSession.stop()
      Thread.sleep(5000)

      var (message, queryText) = getQueryMessage(conn, queryID, sessionID)
      var tryCount: Int = 0
      // Check the query history, and the query must be cancelled
      // There may be latency to get query history
      while (message == null && tryCount < 10) {
        Thread.sleep(10000)
        val res = getQueryMessage(conn, queryID, sessionID)
        message = res._1
        queryText = res._2
        tryCount = tryCount + 1
        logger.warn(s"Retry count: $tryCount, Get query history message: $message")
      }
      assert("SQL execution canceled".equals(message))
      assert(queryText.contains(query))

      // Clean up
      conn.close()
      // Recreate spark session to avoid affect following test cases
      sparkSession = SparkSession.builder
        .master("local")
        .appName("SnowflakeSourceSuite")
        .config("spark.sql.shuffle.partitions", "6")
        // "spark.sql.legacy.timeParserPolicy = LEGACY" is added to allow
        // spark 3.0 to support legacy conversion for unix_timestamp().
        // It may not be necessary for spark 2.X.
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    }
  }

}
