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

import net.snowflake.spark.snowflake
import net.snowflake.spark.snowflake.StressTestResult.{fromEmailAddress, tryToSendEmail}
import net.snowflake.spark.snowflake.testsuite.ClusterTestSuiteBase
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object ClusterTest extends Enumeration {
  type ClusterTest = Value

  // Different environments for running the ClusterTest
  val githubTest: snowflake.ClusterTest.Value = Value("github")
  val stressTest: snowflake.ClusterTest.Value = Value("stress")

  val log: Logger = LoggerFactory.getLogger(getClass)

  val RemoteMode = "remote"
  val LocalMode = "local"

  val TestSuiteSeparator = ";"

  // Driver function to run the test.
  def main(args: Array[String]): Unit = {

    // Test it!
    tryToSendEmail("connector-regress-watchers-dl@snowflake.com",
      fromEmailAddress, "testing stress-test email, please ignore", "test email body")

    log.info(s"Test Spark Connector: ${net.snowflake.spark.snowflake.Utils.VERSION}")

    val usage = s"""Three parameters are needed: [local | remote],
                    | testClassNames (using ';' to separate multiple classes), and
                    | [github | stress] to indicate the test environment
                    |""".stripMargin
    log.info(usage)

    if (args.length < 3) {
      throw new Exception(s"At least three parameters are needed. Usage: $usage")
    }

    var envType: ClusterTest = null
    try {
      envType = ClusterTest.withName(args(2).toLowerCase())
    } catch {
      case e: NoSuchElementException =>
        throw new IllegalArgumentException(
          s"Run type ${args(2)} not found. Allowed values are: {${ClusterTest.values.mkString(", ")}}.")
    }

    // Setup Spark session.
    // local mode is introduced for debugging purpose
    val runMode = args(0)
    var sparkSessionBuilder = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
    if (runMode.equalsIgnoreCase(LocalMode)) {
      sparkSessionBuilder = sparkSessionBuilder
        .config("spark.master", "local")
    }
    val spark = sparkSessionBuilder.getOrCreate()

    // Run specified test suites
    val testSuiteNames = args(1).split(TestSuiteSeparator)
    for (testSuiteName <- testSuiteNames) {
      if (!testSuiteName.trim.isEmpty) {
        var resultBuilder: BaseTestResultBuilder = null
        if (envType == ClusterTest.githubTest) {
          // Retrieve commit ID from env.
          val commitID = scala.util.Properties
            .envOrElse(TestUtils.GITHUB_SHA, "commit id not set")

          resultBuilder = new GithubActionsTestResultBuilder()
            .withTestType("Scala")
            .withCommitID(commitID)
            .withGithubRunId(TestUtils.githubRunId)
        } else if (envType == ClusterTest.stressTest) {

          if (args.length < 4) {
            throw new Exception(
              s"At least four parameters are needed in stress-test mode. " +
                s"The test-revision number is missing.")
          }

          // We keep a version number for the revision/version of the input test data and config
          val testInputRevisionNumber = Integer.valueOf(args(3))

          // Email address for sending failed stress test alerts
          val emailAddress = if (args.length > 4) Some(args(4)) else None

          resultBuilder = new StressTestResultBuilder(emailAddress)
            .withTestRevision(testInputRevisionNumber)
        } else {
          throw new RuntimeException(s"Bad ClusterTest env type:$envType")
        }

        resultBuilder
          .withTestCaseName(testSuiteName)
          .withTestStatus(TestUtils.TEST_RESULT_STATUS_INIT)
          .withStartTimeInMill(System.currentTimeMillis)

        try {
          Class
            .forName(testSuiteName)
            .newInstance()
            .asInstanceOf[ClusterTestSuiteBase]
            .run(spark, resultBuilder)
        } catch {
          case e: Throwable =>
            log.error(e.getMessage)
            resultBuilder
              .withTestStatus(TestUtils.TEST_RESULT_STATUS_EXCEPTION)
              .withReason(Some(e.getMessage))
        } finally {
          // Set test end time.
          resultBuilder
            .withEndTimeInMill(System.currentTimeMillis())
          // Write test result
          resultBuilder.build().writeToSnowflake()
        }
      }
    }

    spark.stop()
  }
}
