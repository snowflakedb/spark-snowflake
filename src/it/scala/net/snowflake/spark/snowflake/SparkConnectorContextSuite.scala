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

import net.snowflake.client.jdbc.{SnowflakeResultSet, SnowflakeStatement}

class SparkConnectorContextSuite extends IntegrationSuiteBase {

  test("SparkConnectorContext: add/remove running query") {
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

  test("SparkConnectorContext: add running query and cancel query") {
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

  // NOTE: This test closes the sparkSession, so put the below test case in the end.
  test("SparkConnectorContext: test a running query is canceled") {
    val sc = sparkSession.sparkContext
    val appId = sc.applicationId
    val param = Parameters.MergedParameters(connectorOptions)
    val conn = DefaultJDBCWrapper.getConnector(param)

    // Execute a 60 second query in async mode and get the query ID
    val rs = conn
      .createStatement()
      .asInstanceOf[SnowflakeStatement]
      .executeAsyncQuery(
        "select seq4(), random() from table(generator(timelimit => 60))"
      )
    val queryID = rs.asInstanceOf[SnowflakeResultSet].getQueryID

    // Add the running query
    SparkConnectorContext.addRunningQuery(sc, conn, queryID)
    val runningQueries = SparkConnectorContext.getRunningQueries
    assert(runningQueries.size == 1)
    assert(
      runningQueries.contains(appId) &&
        runningQueries(appId).contains(RunningQuery(conn, queryID))
    )

    // Stop the application, it will trigger the Application End event.
    sparkSession.stop()
    Thread.sleep(10000)

    // Check the query history, and the query must be cancelled
    val rs2 = conn
      .createStatement()
      .executeQuery(
        "select * from table(information_schema.query_history_by_session())" +
          s" where QUERY_ID = '$queryID'"
      )
    assert(rs2.next())
    assert(rs2.getString("ERROR_MESSAGE").equals("SQL execution canceled"))

    conn.close()
  }
  // NOTE: This test closes the sparkSession, so put the above test case in the end.
}
