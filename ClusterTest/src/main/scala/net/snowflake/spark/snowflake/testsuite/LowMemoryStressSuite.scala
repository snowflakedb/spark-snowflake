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

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.{ClusterTestResultBuilder, DefaultJDBCWrapper, TestUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.util.SizeEstimator
import org.slf4j.LoggerFactory

import scala.util.Random

class LowMemoryStressSuite extends ClusterTestSuiteBase {
  private val log = LoggerFactory.getLogger(getClass)

  override def runImpl(sparkSession: SparkSession,
                       resultBuilder: ClusterTestResultBuilder): Unit = {

    def getRandomString(len: Int): String = {
      Random.alphanumeric take len mkString ""
    }

    val partitionCount = 1
    val rowCountPerPartition = 1300000
    // Create RDD which generates data with multiple partitions
    val testRDD: RDD[Row] = sparkSession.sparkContext
      .parallelize(Seq[Int](), partitionCount)
      .mapPartitions { _ => {
        (1 to rowCountPerPartition).map { i => {
          Row(Random.nextInt, Random.nextDouble(), getRandomString(50),
            Random.nextInt, Random.nextDouble(), getRandomString(50),
            Random.nextInt, Random.nextDouble(), getRandomString(50))
        }
        }.iterator
      }
      }
    val schema = StructType(
      List(
        StructField("int1", IntegerType),
        StructField("double1", DoubleType),
        StructField("str1", StringType),
        StructField("int2", IntegerType),
        StructField("double2", DoubleType),
        StructField("str2", StringType),
        StructField("int3", IntegerType),
        StructField("double3", DoubleType),
        StructField("str3", StringType)
      )
    )
    val test_big_partition = s"test_big_partition_$randomSuffix"

    val sampleRow = Row(Random.nextInt, Random.nextDouble(), getRandomString(50),
      Random.nextInt, Random.nextDouble(), getRandomString(50),
      Random.nextInt, Random.nextDouble(), getRandomString(50))

    val rowSize = SizeEstimator.estimate(sampleRow.toSeq.map {
      value => value.asInstanceOf[AnyRef]
    })

    val partitionSize = rowSize * rowCountPerPartition / 1024 / 1024

    log.info(s"""partition count: $partitionCount, row per partition: $rowCountPerPartition""")
    log.info(s"""size of a row: $rowSize Bytes, partition size: $partitionSize MB""")

    var executorMem = sparkSession.sparkContext.getExecutorMemoryStatus
    log.info(s"""executors memory: $executorMem""")

    // Convert RDD to DataFrame
    val df = sparkSession.createDataFrame(testRDD, schema)

    executorMem = sparkSession.sparkContext.getExecutorMemoryStatus
    log.info(s"""executors memory: $executorMem""")

    // Write to snowflake
    df.write
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(TestUtils.sfOptionsNoTable)
      .option("dbtable", test_big_partition)
      .mode(SaveMode.Overwrite)
      .save()
    executorMem = sparkSession.sparkContext.getExecutorMemoryStatus
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
