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

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.spark.snowflake.ClusterTest.log
import net.snowflake.spark.snowflake.{ClusterTestResultBuilder, DefaultJDBCWrapper, TestUtils}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source

/**
 * Similar to BasicReadWriteSuite, but configurable with a JSON file as to which tables
 * to perform the read and write on.
 */
class StressReadWriteSuite extends ClusterTestSuiteBase {

  // A source table to read from for a stress test is defined by its database, schema, and table name
  case class StressTestSourceTable(database: String, schema: String, table: String)

  /**
   * Reads the json file as configured by the system env variable defined in TestUtils.STRESS_TEST_SOURCES
   * to get the tables to perform the read/write stress test on.
   *
   * The file should be in the format:
   *
   * [
   *   {
   *     "database": "table1_db",
   *     "schema": "table1_schema",
   *     "tableName": "table1_name"
   *   },
   *   {
   *     "database": "table2_db",
   *     "schema": "table2_schema",
   *     "tableName": "table2_name"
   *   },
   *   ...
   * ]
   *
   * @param configFile the path to the sources file
   * @return A list of StressTestSourceTable
   */
  private def getTablesToReadFromSourcesFile(configFile: String): List[StressTestSourceTable] = {

    val jsonConfigFile = Source.fromFile(configFile)
    val file = jsonConfigFile.mkString
    val mapper: ObjectMapper = new ObjectMapper()
    val json = mapper.readTree(file)

    val sourceTables =
      mapper.convertValue(json, classOf[java.util.List[java.util.Map[String, String]]])

    sourceTables.asScala
      .map(s => {
        StressTestSourceTable(s.get("database"), s.get("schema"), s.get("table"))
      })
      .toList
  }

  lazy val tablesToRead: List[StressTestSourceTable] =
    getTablesToReadFromSourcesFile(System.getenv(TestUtils.STRESS_TEST_SOURCES))

  lazy val targetDatabase: String = TestUtils.sfStressOptions("sfdatabase")
  lazy val targetSchema: String = TestUtils.sfStressOptions("sfschema")

  // These values will be replaced by those in StressTestSourceTable
  lazy val baseStressTestOptions: Map[String, String] = TestUtils.sfStressOptions.filterKeys(param =>
    !Set("sfdatabase", "sfschema", "dbtable").contains(param.toLowerCase))

  protected def readWriteSnowflakeTableWithDatabase(
      sparkSession: SparkSession,
      resultBuilder: ClusterTestResultBuilder,
      sfOptionsNoTable: Map[String, String],
      sourceDatabase: String,
      sourceSchema: String,
      sourceTableName: String,
      targetDatabase: String,
      targetSchema: String,
      targetTableName: String): Unit = {

    val sqlContext = sparkSession.sqlContext
    val tableNameInfo =
      s"Source:$sourceSchema.$sourceTableName Target=$targetSchema.$targetTableName"

    // Read DataFrame.
    val df = sqlContext.read
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", sourceTableName)
      .option("sfschema", sourceSchema)
      .option("sfdatabase", sourceDatabase)
      .load()

    // Write DataFrame
    df.write
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", targetTableName)
      .option("sfschema", targetSchema)
      .option("sfdatabase", targetDatabase)
      .mode(SaveMode.Overwrite)
      .save()

    // Source rowCount
    val sourceRowCount = sparkSession.read
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", sourceTableName)
      .option("sfschema", sourceSchema)
      .option("sfdatabase", sourceDatabase)
      .load()
      .count()

    // Target rowCount
    val targetRowCount = sparkSession.read
      .format(TestUtils.SNOWFLAKE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", targetTableName)
      .option("sfschema", targetSchema)
      .option("sfdatabase", targetDatabase)
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
      .option("sfdatabase", sourceDatabase)
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
      .option("sfdatabase", targetDatabase)
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

  override def runImpl(
      sparkSession: SparkSession,
      resultBuilder: ClusterTestResultBuilder): Unit = {

    tablesToRead.foreach(source => {
      val targetTableName = s"test_write_table_${source.table}_$randomSuffix"

      // Read write a basic table:
      readWriteSnowflakeTableWithDatabase(
        sparkSession,
        resultBuilder,
        TestUtils.sfOptionsNoTable,
        source.database,
        source.schema,
        source.table,
        targetDatabase,
        targetSchema,
        targetTableName)

      // If test is successful, drop the target table,
      // otherwise, keep it for further investigation.
      if (resultBuilder.testStatus == TestUtils.TEST_RESULT_STATUS_SUCCESS) {
        val connection = DefaultJDBCWrapper.getConnector(TestUtils.param)
        connection
          .createStatement()
          .execute(s"drop table $targetSchema.$targetTableName")
        connection.close()
      }
    })
  }
}
