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

import net.snowflake.spark.snowflake.testsuite.ClusterTestSuiteBase
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object ClusterTest {
  val log: Logger = LoggerFactory.getLogger(getClass)

  val RemoteMode = "remote"
  val LocalMode = "local"

  val TestSuiteSeparator = ";"

  // Driver function to run the test.
  def main(args: Array[String]): Unit = {
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
        // Retrieve commit ID from env.
        val commitID = scala.util.Properties
          .envOrElse(TestUtils.GITHUB_SHA, "commit id not set")

        // val testSuiteName = "net.snowflake.spark.snowflake.testsuite.BasicReadWriteSuite"
        val resultBuilder = new GithubActionsClusterTestResultBuilder()
          .withTestType("Scala")
          .withTestCaseName(testSuiteName)
          .withCommitID(commitID)
          .withTestStatus(TestUtils.TEST_RESULT_STATUS_INIT)
          .withStartTimeInMill(System.currentTimeMillis())
          .withGithubRunId(TestUtils.githubRunId)

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
