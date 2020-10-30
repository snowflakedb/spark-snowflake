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

// This class is used separately with others in .github/docker/check_result.sh
// If more libraries are used, the script needs to download the libraries and
// update classpath.
object ClusterTestCheckResult {

// Driver function to run the test.
  def main(args: Array[String]): Unit = {
    val usage = s"Two parameters are need: testCaseCount and timeoutInSeconds"
    println(usage)

    if (args.length < 2) {
      throw new Exception(s"At least two parameters are need. Usage: $usage")
    }

    val testCaseCount = args(0).toInt
    var leftTime = args(1).toInt
    // Check result in every 10 seconds
    val checkInterval: Int = 10000
    val commitID = scala.util.Properties.envOrNone(TestUtils.GITHUB_SHA)
    if (commitID.isEmpty) {
      throw new Exception(
        s"Caller has to set env variable ${TestUtils.GITHUB_SHA}"
      )
    }

    val connection = TestUtils.getJDBCConnection(TestUtils.param)

    // Wait for all test cases are done.
    var testDone = false
    while (!testDone && leftTime > 0) {
      // Sleep some time and then check result
      Thread.sleep(checkInterval)
      leftTime = leftTime - checkInterval / 1000

      val resultSet = connection
        .createStatement()
        .executeQuery(
          s"""select count(*) from ${TestUtils.GITHUB_TEST_RESULT_TABLE}
         | where githubRunId = '${TestUtils.githubRunId}'
         |""".stripMargin
        )
      resultSet.next()

      val finishedTestCount = resultSet.getInt(1)
      println(
        s"Finished test cases: $finishedTestCount, expected : $testCaseCount"
      )

      if (finishedTestCount == testCaseCount) {
        testDone = true
      }
    }

    // Retrieve successful test case count
    val resultSet = connection
      .createStatement()
      .executeQuery(
        s"""select count(*) from ${TestUtils.GITHUB_TEST_RESULT_TABLE}
       | where githubRunId = '${TestUtils.githubRunId}'
       |       and testStatus = '${TestUtils.TEST_RESULT_STATUS_SUCCESS}'
       |""".stripMargin
      )
    resultSet.next()
    val successCount = resultSet.getInt(1)

    // Print out all test cases results
    val rs = connection
      .createStatement()
      .executeQuery(s"""select * from ${TestUtils.GITHUB_TEST_RESULT_TABLE}
         | where githubRunId = '${TestUtils.githubRunId}'
         |""".stripMargin)
    val rsmd = rs.getMetaData
    val columnsNumber = rsmd.getColumnCount

    // Output column name
    var sb = new StringBuilder
    for (i <- 1 to columnsNumber) {
      sb.append(rsmd.getColumnName(i)).append(", ")
    }
    println(sb.toString())

    // Output test case result
    while (rs.next) {
      sb = new StringBuilder
      for (i <- 1 to columnsNumber) {
        sb.append(rs.getString(i)).append(", ")
      }
      println(sb.toString())
    }

    if (successCount != testCaseCount) {
      throw new Exception(
        s"Some test case fail: expected $testCaseCount, actual: $successCount"
      )
    } else {
      println(s"All test cases are PASS $testCaseCount")
    }
  }
}
