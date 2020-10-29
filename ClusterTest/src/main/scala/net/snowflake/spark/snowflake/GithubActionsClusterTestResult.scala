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

class GithubActionsClusterTestResult(builder: GithubActionsClusterTestResultBuilder)
    extends ClusterTestResult {
  val testType: String = builder.testType
  val testCaseName: String = builder.overallTestContext.testName
  val testStatus: String = builder.overallTestContext.testStatus
  val commitID: String = builder.commitID
  val githubRunId: String = builder.githubRunId
  val startTime: String =
    Instant.ofEpochMilli(builder.overallTestContext.taskStartTime).toString
  val testRunTime: String = {
    val usedTime = builder.overallTestContext.taskEndTime - builder.overallTestContext.taskStartTime
    if (usedTime < 0) {
      s"Wrong time: Start ${builder.overallTestContext.taskStartTime} end: ${builder.overallTestContext.taskEndTime}"
    } else if (usedTime < 1000) {
      s"$usedTime ms"
    } else if (usedTime < 1000 * 60) {
      "%.2f seconds".format(usedTime.toDouble / 1000)
    } else {
      "%.2f minutes".format(usedTime.toDouble / 1000 / 60)
    }
  }
  val reason: String = builder.overallTestContext.reason.getOrElse(TestUtils.TEST_RESULT_REASON_NO_REASON)

  def writeToSnowflake(): Unit = {
    val connection = DefaultJDBCWrapper.getConnector(TestUtils.param)

    // Create test result table if it doesn't exist.
    if (!DefaultJDBCWrapper.tableExists(connection, TestUtils.CLUSTER_TEST_RESULT_TABLE)) {
      DefaultJDBCWrapper.executeInterruptibly(
        connection,
        s"""create table ${TestUtils.CLUSTER_TEST_RESULT_TABLE} (
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
      s"""insert into ${TestUtils.CLUSTER_TEST_RESULT_TABLE} values (
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
