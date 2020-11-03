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

private[snowflake] class StressTestResult(builder: StressTestResultBuilder) extends ClusterTestResult {
  val testSuiteName: String = builder.overallTestStatus.testName
  val testStatus: String = builder.overallTestStatus.testStatus
  val startTime: String = TestUtils.formatTimestamp(builder.overallTestStatus.taskStartTime)
  val testRunTime: String = TestUtils.formatTimeElapsed(builder.overallTestStatus)
  val reason: String =
    builder.overallTestStatus.reason.getOrElse(TestUtils.TEST_RESULT_REASON_NO_REASON)

  def writeToSnowflake(): Unit = {
    val connection = DefaultJDBCWrapper.getConnector(TestUtils.param)

    // Create the sequence for id generation, if it does not exist
    DefaultJDBCWrapper.executeInterruptibly(
      connection,
      s"create sequence if not exists ${TestUtils.STRESS_TEST_SEQ_NAME}")

    // Create the overall test-result table if it doesn't exist.
    if (!DefaultJDBCWrapper.tableExists(connection, TestUtils.STRESS_TEST_RESULTS_TABLE)) {
      DefaultJDBCWrapper.executeInterruptibly(
        connection,
        s"""create table ${TestUtils.STRESS_TEST_RESULTS_TABLE} (
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

    val runId = res.getLong(1)

    // Create the detailed test-result table if it doesn't exist.
    if (!DefaultJDBCWrapper.tableExists(connection, TestUtils.STRESS_TEST_DETAILED_RESULTS_TABLE)) {
      // Create the subtask-result table for this run
      DefaultJDBCWrapper.executeInterruptibly(
        connection,
        s"""create table ${TestUtils.STRESS_TEST_DETAILED_RESULTS_TABLE} (
           | revisionNumber Integer,
           | runId Integer,
           | testSuiteName String,
           | testName String,
           | testStatus String,
           | startTime String,
           | testRunTime String,
           | reason String )
           |""".stripMargin)
    }

    // Write test result into the main result table
    DefaultJDBCWrapper.executeInterruptibly(
      connection,
      s"""insert into ${TestUtils.STRESS_TEST_RESULTS_TABLE} values (
         |  ${builder.testRevisionNumber},
         |  $runId,
         | '$testSuiteName' ,
         | '$testStatus' ,
         | '$startTime' ,
         | '$testRunTime' ,
         | '$reason'
         | ) """.stripMargin)


    // Now write the results of the individual subtasks.
    builder.subTaskResults.foreach(subTask => {
      DefaultJDBCWrapper.executeInterruptibly(
        connection,
        s"""insert into ${TestUtils.STRESS_TEST_DETAILED_RESULTS_TABLE} values (
           | ${builder.testRevisionNumber},
           | $runId,
           | '$testSuiteName',
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
