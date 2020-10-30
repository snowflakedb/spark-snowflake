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
import net.snowflake.spark.snowflake.testsuite.ClusterTestSuiteBase
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object ClusterTest extends Enumeration {
  type ClusterTest = Value

  // Different environments for running the ClusterTest
  val githubTest: snowflake.ClusterTest.Value = Value("Github")
  val stressTest: snowflake.ClusterTest.Value = Value("StressTest")

  val log: Logger = LoggerFactory.getLogger(getClass)

  val RemoteMode = "remote"
  val LocalMode = "local"

  val TestSuiteSeparator = ";"

  // Driver function to run the test.
  def main(args: Array[String]): Unit = {
    // If there are more than 2 arguments, we're running in the stress test environment
    val envType: ClusterTest =
      if (args.length < 3) ClusterTest.githubTest else ClusterTest.stressTest

    log.info(s"Test Spark Connector: ${net.snowflake.spark.snowflake.Utils.VERSION}")

    val usage = s"""Two parameters are need: [local | remote] and
                    | testClassNames (using ';' to separate multiple classes)
                    |""".stripMargin
    log.info(usage)

    if (args.length < 2) {
      throw new Exception(s"At least two parameters are need. Usage: $usage")
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
          // We keep a version number for the revision/version of the input test data and config
          val testInputRevisionNumber = Integer.valueOf(args(2))

          resultBuilder = new StressTestResultBuilder()
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
