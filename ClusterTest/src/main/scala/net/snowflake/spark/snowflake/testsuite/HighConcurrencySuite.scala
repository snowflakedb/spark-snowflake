package net.snowflake.spark.snowflake.testsuite
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

import net.snowflake.spark.snowflake.{ClusterTestResultBuilder, DefaultJDBCWrapper, TestUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

class HighConcurrencySuite extends ClusterTestSuiteBase {
  private val log = LoggerFactory.getLogger(getClass)

  override def runImpl(sparkSession: SparkSession,
                       resultBuilder: ClusterTestResultBuilder): Unit = {

    val test_big_partition = s"test_big_partition_$randomSuffix"

    // This data frame is of size 227MB
    val df = generateDataFrame(sparkSession, 200, 10)

    // Write to snowflake
    df.write
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(TestUtils.sfOptionsNoTable)
      .option("dbtable", test_big_partition)
      .mode(SaveMode.Overwrite)
      .save()
    val executorMem = sparkSession.sparkContext.getExecutorMemoryStatus
    log.info(s"""executors memory: $executorMem""")

    // Test succeed.
    resultBuilder
      .withTestStatus(TestUtils.TEST_RESULT_STATUS_SUCCESS)
      .withReason("Success")

    // If test is successful, drop the target table,
    // otherwise, keep it for further investigation.
    if (resultBuilder.testStatus == TestUtils.TEST_RESULT_STATUS_SUCCESS) {
      val connection = DefaultJDBCWrapper.getConnector(TestUtils.param)
      connection
        .createStatement()
        .execute(s"drop table $test_big_partition")
      connection.close()
    }
  }
}
