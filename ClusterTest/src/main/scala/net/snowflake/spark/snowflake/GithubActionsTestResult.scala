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

private[snowflake] class GithubActionsTestResult(builder: GithubActionsTestResultBuilder)
    extends ClusterTestResult {
  val testType: String = builder.testType
  val testCaseName: String = builder.overallTestStatus.testName
  val testStatus: String = builder.overallTestStatus.testStatus
  val commitID: String = builder.commitID
  val githubRunId: String = builder.githubRunId
  val startTime: String = TestUtils.formatTimestamp(builder.overallTestStatus.taskStartTime)
  val testRunTime: String = TestUtils.formatTimeElapsed(builder.overallTestStatus)
  val reason: String = builder.overallTestStatus.reason.getOrElse(TestUtils.TEST_RESULT_REASON_NO_REASON)

  def writeToSnowflake(): Unit = {
    val connection = DefaultJDBCWrapper.getConnector(TestUtils.param)

    // Create test result table if it doesn't exist.
    if (!DefaultJDBCWrapper.tableExists(connection, TestUtils.GITHUB_TEST_RESULTS_TABLE)) {
      DefaultJDBCWrapper.executeInterruptibly(
        connection,
        s"""create table ${TestUtils.GITHUB_TEST_RESULTS_TABLE} (
           | testCaseName String,
           | testStatus String,
           | githubRunId String,
           | commitID String,
           | testType String,
           | startTime String,
           | testRunTime String,
           | reason String )
           |""".stripMargin)
    }

    // Write test result into table
    DefaultJDBCWrapper.executeInterruptibly(
      connection,
      s"""insert into ${TestUtils.GITHUB_TEST_RESULTS_TABLE} values (
         | '$testCaseName' ,
         | '$testStatus' ,
         | '$githubRunId' ,
         | '$commitID' ,
         | '$testType',
         | '$startTime' ,
         | '$testRunTime' ,
         | '$reason'
         | ) """.stripMargin)

    connection.close()
  }
}
