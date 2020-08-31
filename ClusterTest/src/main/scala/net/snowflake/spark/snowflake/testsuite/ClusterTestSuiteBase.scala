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

import net.snowflake.spark.snowflake.ClusterTest.log
import net.snowflake.spark.snowflake.{ClusterTestResultBuilder, TestUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.util.Random

trait ClusterTestSuiteBase {
  def run(sparkSession: SparkSession,
          resultBuilder: ClusterTestResultBuilder): Unit = {
    // Start to run the test.
    resultBuilder.withTestStatus(TestUtils.TEST_RESULT_STATUS_START)

    // Run the test implementation
    runImpl(sparkSession, resultBuilder)

    // The test implementation should set up the test status
    assert(resultBuilder.testStatus != TestUtils.TEST_RESULT_STATUS_START)
  }

  // Each test case MUST implement this function.
  def runImpl(sparkSession: SparkSession,
              resultBuilder: ClusterTestResultBuilder): Unit

  protected def randomSuffix: String = Math.abs(Random.nextLong()).toString

  protected def generateDataFrame(sparkSession: SparkSession,
                                   partitionCount: Int,
                                  rowCountPerPartition: Int): DataFrame = {
    def getRandomString(len: Int): String = {
      Random.alphanumeric take len mkString ""
    }

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

    // Convert RDD to DataFrame
    return sparkSession.createDataFrame(testRDD, schema)
  }

  // Utility function to read one table and write to another.
  protected def readWriteSnowflakeTable(sparkSession: SparkSession,
                                        resultBuilder: ClusterTestResultBuilder,
                                        sfOptionsNoTable: Map[String, String],
                                        sourceSchema: String,
                                        sourceTableName: String,
                                        targetSchema: String,
                                        targetTableName: String): Unit = {
    val sqlContext = sparkSession.sqlContext
    val tableNameInfo =
      s"Source:$sourceSchema.$sourceTableName Target=$targetSchema.$targetTableName"

    // Read DataFrame.
    val df = sqlContext.read
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(sfOptionsNoTable)
      // .option("query", s"select * from $sourceSchema.$sourceTableName limit 100000")
      .option("dbtable", sourceTableName)
      .option("sfSchema", sourceSchema)
      .load()

    // Write DataFrame
    df.write
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", targetTableName)
      .option("sfSchema", targetSchema)
      .mode(SaveMode.Overwrite)
      .save()

    // Source rowCount
    val sourceRowCount = sparkSession.read
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", sourceTableName)
      .option("sfSchema", sourceSchema)
      .load()
      .count()

    // Target rowCount
    val targetRowCount = sparkSession.read
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", targetTableName)
      .option("sfSchema", targetSchema)
      .load()
      .count()

    // verify row count to be equal
    val rowCountInfo =
      s"sourceRowCount=$sourceRowCount, targetRowCount=$targetRowCount"
    log.info(rowCountInfo)
    if (sourceRowCount != targetRowCount) {
      resultBuilder
        .withTestStatus(TestUtils.TEST_RESULT_STATUS_FAIL)
        .withReason(
          s"Read Write row count is incorrect: $tableNameInfo $rowCountInfo"
        )
      return
    }

    // Source HASH_AGG result
    val sourceHashAgg = sparkSession.read
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(sfOptionsNoTable)
      .option(
        "query",
        s"select HASH_AGG(*) from $sourceSchema.$sourceTableName"
      )
      .load()
      .collect()(0)(0)

    // Target HASH_AGG result
    val targetHashAgg = sparkSession.read
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(sfOptionsNoTable)
      .option(
        "query",
        s"select HASH_AGG(*) from $targetSchema.$targetTableName"
      )
      .load()
      .collect()(0)(0)

    val hashAggInfo =
      s"sourceHashAgg=$sourceHashAgg targetHashAgg=$targetHashAgg"
    // Verify hash agg to be equal
    if (sourceHashAgg != targetHashAgg) {
      resultBuilder
        .withTestStatus(TestUtils.TEST_RESULT_STATUS_FAIL)
        .withReason(
          s"hash agg result is incorrect: $tableNameInfo $hashAggInfo"
        )
      return
    }

    // Test succeed.
    resultBuilder
      .withTestStatus(TestUtils.TEST_RESULT_STATUS_SUCCESS)
      .withReason("Success")
  }
}
