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

import java.time.Instant

class StressTestResult(builder: StressTestResultBuilder) extends ClusterTestResult {
  val testName: String = builder.overallTestContext.testName
  val testStatus: String = builder.overallTestContext.testStatus
  val startTime: String = TestUtils.formatTimestamp(builder.overallTestContext.taskStartTime)
  val testRunTime: String = TestUtils.formatTimeElapsed(builder.overallTestContext)
  val reason: String =
    builder.overallTestContext.reason.getOrElse(TestUtils.TEST_RESULT_REASON_NO_REASON)

  def writeToSnowflake(): Unit = {
    val connection = DefaultJDBCWrapper.getConnector(TestUtils.param)

    // Create the sequence for id generation, if it does not exist
    DefaultJDBCWrapper.executeInterruptibly(
      connection,
      s"create sequence if not exists ${TestUtils.STRESS_TEST_SEQ_NAME}")

    // Create test result table if it doesn't exist.
    if (!DefaultJDBCWrapper.tableExists(connection, TestUtils.STRESS_TEST_RESULT_TABLE)) {
      DefaultJDBCWrapper.executeInterruptibly(
        connection,
        s"""create table ${TestUtils.STRESS_TEST_RESULT_TABLE} (
           | testRevision Integer,
           | runId Integer,
           | testName String,
           | testStatus String,
           | startTime String,
           | testRunTime String,
           | reason String )
           |""".stripMargin)
    }

    // Get the next runID
    val res = DefaultJDBCWrapper
      .executeQueryInterruptibly(connection, s"select ${TestUtils.STRESS_TEST_SEQ_NAME}.nextVal")
    res.next()

    val runId = res.getInt(1)
    val runTableName = TestUtils.STRESS_TEST_RUN_TABLE_PREFIX + runId

    // A table with this name should not exist; if it does, something is wrong.
    // We need to terminate this early
    if (DefaultJDBCWrapper.tableExists(connection, runTableName)) {
      throw new RuntimeException(
        s"Error: Subtask-result table for stress test run id $runId already exists.")
    }

    // Write test result into the main result table
    DefaultJDBCWrapper.executeInterruptibly(
      connection,
      s"""insert into ${TestUtils.STRESS_TEST_RESULT_TABLE} values (
         |  ${builder.testRevisionNumber},
         |  $runId,
         | '$testName' ,
         | '$testStatus' ,
         | '$startTime' ,
         | '$testRunTime' ,
         | '$reason'
         | ) """.stripMargin)

    // Create the subtask-result table for this run
    DefaultJDBCWrapper.executeInterruptibly(
      connection,
      s"""create table $runTableName (
         | revisionNumber Integer,
         | testName String,
         | testStatus String,
         | startTime String,
         | testRunTime String,
         | reason String )
         |""".stripMargin)

    // Now write the results of the individual subtasks.
    builder.subTaskResults.foreach(subTask => {
      DefaultJDBCWrapper.executeInterruptibly(
        connection,
        s"""insert into $runTableName values (
           | ${builder.testRevisionNumber},
           | '${subTask.testName}' ,
           | '${subTask.testStatus}' ,
           | '${TestUtils.formatTimestamp(subTask.taskStartTime)}' ,
           | '${TestUtils.formatTimeElapsed(subTask)}' ,
           | '${subTask.reason.getOrElse(TestUtils.TEST_RESULT_REASON_NO_REASON)}'
           | ) """.stripMargin)
    })

    connection.close()
  }
}
