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

import net.snowflake.spark.snowflake.{BaseTestResultBuilder, DefaultJDBCWrapper, TestStatus, Parameters, TestUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

import scala.util.Random

class LowMemoryStressSuite extends ClusterTestSuiteBase {
  private val log = LoggerFactory.getLogger(getClass)

  override def runImpl(sparkSession: SparkSession,
                       resultBuilder: BaseTestResultBuilder): Unit = {

    val taskStatus = TestStatus("LowMemoryStressSuite")

    def getRandomString(len: Int): String = {
      Random.alphanumeric take len mkString ""
    }

    val randomString1 = getRandomString(50000)
    val randomString2 = getRandomString(50000)
    val randomString3 = getRandomString(50000)
    val randomString4 = getRandomString(50000)
    val randomString5 = getRandomString(50000)
    val randomString6 = getRandomString(50000)
    val randomString7 = getRandomString(50000)
    val randomString8 = getRandomString(50000)
    val partitionCount = 1
    val rowCountPerPartition = 800
    // Create RDD which generates data with multiple partitions
    val testRDD: RDD[Row] = sparkSession.sparkContext
      .parallelize(Seq[Int](), partitionCount)
      .mapPartitions { _ => {
        (1 to rowCountPerPartition).map { i => {
          Row(randomString1, randomString2,
            randomString3, randomString4,
            randomString5, randomString6,
            randomString7, randomString8)
        }
        }.iterator
      }
      }
    val schema = StructType(
      List(
        StructField("str1", StringType),
        StructField("str2", StringType),
        StructField("str3", StringType),
        StructField("str4", StringType),
        StructField("str5", StringType),
        StructField("str6", StringType),
        StructField("str7", StringType),
        StructField("str8", StringType)
      )
    )
    val test_big_partition = s"test_big_partition_$randomSuffix"

    taskStatus.taskStartTime = System.currentTimeMillis

    // Convert RDD to DataFrame
    val df = sparkSession.createDataFrame(testRDD, schema)

    // Write to snowflake
    df.write
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(TestUtils.sfOptionsNoTable)
      .option("dbtable", test_big_partition)
      .mode(SaveMode.Overwrite)
      .save()

    log.info(
      s"""Finished the first multi-part upload test.""".stripMargin)

    var noOOMError = true
    try {
      // Write to snowflake with multi-part feature off
      df.write
        .format(TestUtils.SNOWFLAKE_NAME)
        .options(TestUtils.sfOptionsNoTable)
        .option("dbtable", test_big_partition)
        .option(Parameters.PARAM_USE_AWS_MULTIPLE_PARTS_UPLOAD, "off")
        .mode(SaveMode.Overwrite)
        .save()
    }
    catch {
      case e: Throwable => {
        // Test succeed
        noOOMError = false
        taskStatus.testStatus = TestUtils.TEST_RESULT_STATUS_SUCCESS
        taskStatus.reason = Some("Success")
      }
    }
    finally {
      taskStatus.taskEndTime = System.currentTimeMillis()
      resultBuilder.withNewSubTaskResult(taskStatus)

      // This is a simple test suite. The overall result of the suite is the same as that of the single subtask.
      resultBuilder.withTestStatus(taskStatus.testStatus).withReason(taskStatus.reason)

      if (noOOMError) {
        throw new Exception("Expecting OOM error but didn't catch that.")
      }

      // If test is successful, drop the target table,
      // otherwise, keep it for further investigation.
      if (taskStatus.testStatus == TestUtils.TEST_RESULT_STATUS_SUCCESS) {
        val connection = DefaultJDBCWrapper.getConnector(TestUtils.param)
        connection
          .createStatement()
          .execute(s"drop table if exists $test_big_partition")
        connection.close()
      }
    }
  }
}
