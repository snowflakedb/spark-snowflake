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
import net.snowflake.spark.snowflake.{BaseClusterTestResultBuilder, TestUtils}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Random

trait ClusterTestSuiteBase {
  def run(sparkSession: SparkSession, resultBuilder: BaseClusterTestResultBuilder): Unit = {
    // Start to run the test.
    resultBuilder.withTestStatus(TestUtils.TEST_RESULT_STATUS_START)

    // Run the test implementation
    runImpl(sparkSession, resultBuilder)

    // The test implementation should set up the test status
    assert(resultBuilder.testStatus != TestUtils.TEST_RESULT_STATUS_START)
  }

  // Each test case MUST implement this function.
  def runImpl(sparkSession: SparkSession, resultBuilder: BaseClusterTestResultBuilder): Unit

  protected def randomSuffix: String = Math.abs(Random.nextLong()).toString

  // Utility function to read one table and write to another.
  protected def readWriteSnowflakeTable(
      sparkSession: SparkSession,
      resultBuilder: BaseClusterTestResultBuilder,
      sfOptionsNoTable: Map[String, String],
      sourceSchema: String,
      sourceTableName: String,
      targetSchema: String,
      targetTableName: String): Unit = {
    readWriteSnowflakeTableWithDatabase(
      sparkSession,
      resultBuilder,
      sfOptionsNoTable,
      sfOptionsNoTable("sfdatabase"),
      sourceSchema,
      sourceTableName,
      sfOptionsNoTable("sfdatabase"),
      targetSchema,
      targetTableName)
  }

  protected def readWriteSnowflakeTableWithDatabase(
      sparkSession: SparkSession,
      resultBuilder: BaseClusterTestResultBuilder,
      sfOptions: Map[String, String],
      sourceDatabase: String,
      sourceSchema: String,
      sourceTableName: String,
      targetDatabase: String,
      targetSchema: String,
      targetTableName: String): Unit = {

    val options = sfOptions.filterKeys(param =>
      !Set("sfdatabase", "sfschema", "dbtable").contains(param.toLowerCase))

    val tableNameInfo =
      s"Source:$sourceSchema.$sourceTableName Target=$targetSchema.$targetTableName"

    val sourceOptions = options ++ Map(
      "dbtable" -> sourceTableName,
      "sfschema" -> sourceSchema,
      "sfdatabase" -> sourceDatabase)
    val targetOptions = options ++ Map(
      "dbtable" -> targetTableName,
      "sfschema" -> targetSchema,
      "sfdatabase" -> targetDatabase)

    // Read DataFrame.
    val sourceDF = readDataFrameFromSnowflake(sparkSession, sourceOptions)
    // Write DataFrame
    val targetDF = writeDataFrameToSnowflake(sourceDF, targetOptions)

    // Source rowCount
    val sourceRowCount = sourceDF.count
    // Target rowCount
    val targetRowCount = targetDF.count

    // verify row count to be equal
    val rowCountInfo =
      s"sourceRowCount=$sourceRowCount, targetRowCount=$targetRowCount"
    log.info(rowCountInfo)
    if (sourceRowCount != targetRowCount) {
      resultBuilder
        .withTestStatus(TestUtils.TEST_RESULT_STATUS_FAIL)
        .withReason(s"Read Write row count is incorrect: $tableNameInfo $rowCountInfo")
      return
    }

    val sourceTableHashAgg = options ++ Map(
      "query" -> s"select HASH_AGG(*) from $sourceSchema.$sourceTableName",
      "sfdatabase" -> sourceDatabase,
      "sfschema" -> sourceSchema)
    val targetTableHashAgg = options ++ Map(
      "query" -> s"select HASH_AGG(*) from $targetSchema.$targetTableName",
      "sfdatabase" -> targetDatabase,
      "sfschema" -> targetSchema)

    // Source HASH_AGG result
    val sourceHashAgg = readDataFrameFromSnowflake(sparkSession, sourceTableHashAgg)
      .collect()(0)(0)

    // Target HASH_AGG result
    val targetHashAgg = readDataFrameFromSnowflake(sparkSession, targetTableHashAgg)
      .collect()(0)(0)

    val hashAggInfo =
      s"sourceHashAgg=$sourceHashAgg targetHashAgg=$targetHashAgg"
    // Verify hash agg to be equal
    if (sourceHashAgg != targetHashAgg) {
      resultBuilder
        .withTestStatus(TestUtils.TEST_RESULT_STATUS_FAIL)
        .withReason(s"hash agg result is incorrect: $tableNameInfo $hashAggInfo")
      return
    }

    // Test succeed.
    resultBuilder
      .withTestStatus(TestUtils.TEST_RESULT_STATUS_SUCCESS)
      .withReason("Success")
  }

  protected def writeDataFrameToSnowflake(
      df: DataFrame,
      options: Map[String, String]): DataFrame = {
    df.write
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(options)
      .mode(SaveMode.Overwrite)
      .save()

    // Return the written DataFrame
    readDataFrameFromSnowflake(df.sparkSession, options)
  }

  protected def readDataFrameFromSnowflake(
      sparkSession: SparkSession,
      options: Map[String, String]): DataFrame = {
    val sqlContext = sparkSession.sqlContext

    sqlContext.read
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(options)
      .load()
  }
}
