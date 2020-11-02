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
import net.snowflake.spark.snowflake.{BaseTestResultBuilder, DefaultJDBCWrapper, TestStatus, TestUtils}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source

/**
 * Similar to BasicReadWriteSuite, but configurable with a JSON file as to which tables
 * to perform the read and write on.
 */
class StressReadWriteSuite extends ClusterTestSuiteBase {

  lazy val tablesToRead: List[StressTestSourceTable] =
    getTablesToReadFromSourcesFile(System.getenv(TestUtils.STRESS_TEST_SOURCES))
  lazy val targetDatabase: String = TestUtils.sfOptions("sfdatabase")
  lazy val targetSchema: String = TestUtils.sfOptions("sfschema")

  override def runImpl(
      sparkSession: SparkSession,
      resultBuilder: BaseTestResultBuilder): Unit = {

    var numSuccessfulTableReads: Int = 0

    tablesToRead.foreach(source => {
      val sourceTableContext = TestStatus(source.toString)
      sourceTableContext.taskStartTime = System.currentTimeMillis

      val targetTableName = s"test_write_table_${source.table}_$randomSuffix"

      // Read write a basic table:
      super.readWriteSnowflakeTableWithDatabase(
        sourceTableContext,
        sparkSession,
        TestUtils.sfOptionsNoTable,
        source.database,
        source.schema,
        source.table,
        targetDatabase,
        targetSchema,
        targetTableName)

      sourceTableContext.taskEndTime = System.currentTimeMillis
      resultBuilder.withNewSubTaskResult(sourceTableContext)

      // If test is successful, drop the target table,
      // otherwise, keep it for further investigation.
      if (sourceTableContext.testStatus == TestUtils.TEST_RESULT_STATUS_SUCCESS) {
        numSuccessfulTableReads += 1
        val connection = DefaultJDBCWrapper.getConnector(TestUtils.param)
        connection
          .createStatement()
          .execute(s"drop table $targetSchema.$targetTableName")
        connection.close()
      }
    })

    if (numSuccessfulTableReads == 0) {
      resultBuilder
        .withTestStatus(TestUtils.TEST_RESULT_STATUS_FAIL)
        .withReason(Some("No successful reads of any table."))
    } else if (numSuccessfulTableReads < tablesToRead.size) {
      resultBuilder
        .withTestStatus(TestUtils.TEST_RESULT_STATUS_FAIL)
        .withReason(Some(
          s"Partial failure:$numSuccessfulTableReads were successfully read, " +
            s"but the source file had ${tablesToRead.size} source tables defined."))
    } else
      resultBuilder.withTestStatus(TestUtils.TEST_RESULT_STATUS_SUCCESS)

  }

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

  // A source table to read from for a stress test is defined by its database, schema, and table name
  case class StressTestSourceTable(database: String, schema: String, table: String) {
    override def toString: String = s"stress_test_suite_$database.$schema.$table"
  }
}
