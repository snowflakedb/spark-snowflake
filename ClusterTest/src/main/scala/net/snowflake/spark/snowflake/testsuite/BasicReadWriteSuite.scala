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

package net.snowflake.spark.snowflake.testsuite

import net.snowflake.spark.snowflake.{BaseTestResultBuilder, DefaultJDBCWrapper, TaskContext, TestUtils}
import org.apache.spark.sql.SparkSession

class BasicReadWriteSuite extends ClusterTestSuiteBase {
  override def runImpl(
      sparkSession: SparkSession,
      resultBuilder: BaseTestResultBuilder): Unit = {
    // its row count is 6.0M, compressed data size in SF is 157.7 MB.
    val sourceSchema = "TPCH_SF1"
    val sourceTableName = "LINEITEM"
    val targetSchema = "spark_test"
    val targetTableName = s"test_write_table_$randomSuffix"

    val taskContext = TaskContext("BasicReadWriteSuite: TPCH_SF1")
    taskContext.taskStartTime = System.currentTimeMillis

    // Read write a basic table:
    super.readWriteSnowflakeTable(
      taskContext,
      sparkSession,
      TestUtils.sfOptionsNoTable,
      sourceSchema,
      sourceTableName,
      targetSchema,
      targetTableName)

    taskContext.taskEndTime = System.currentTimeMillis
    resultBuilder.withNewSubTaskResult(taskContext)

    // This is a simple test suite. The overall result of the suite is the same as that of the single subtask.
    resultBuilder.withTestStatus(taskContext.testStatus).withReason(taskContext.reason)

    // If test is successful, drop the target table,
    // otherwise, keep it for further investigation.
    if (taskContext.testStatus == TestUtils.TEST_RESULT_STATUS_SUCCESS) {
      val connection = DefaultJDBCWrapper.getConnector(TestUtils.param)
      connection
        .createStatement()
        .execute(s"drop table $targetSchema.$targetTableName")
      connection.close()
    }
  }
}
