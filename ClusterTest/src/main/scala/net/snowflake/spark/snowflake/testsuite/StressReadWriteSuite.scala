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
import net.snowflake.spark.snowflake.{ClusterTestResultBuilder, DefaultJDBCWrapper, TestUtils}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.io.Source

/**
  * Similar to BasicReadWriteSuite, but configurable with a JSON file as to which tables
  * to perform the read and write on.
  */
class StressReadWriteSuite extends ClusterTestSuiteBase {

  // A source table to read from for a stress test is defined by its schema and table name
  case class StressTestSourceTable(schema: String,
                                   tableName: String)

  /**
    * Reads the json file as configured by the system env variable defined in TestUtils.STRESS_TEST_SOURCES
    * to get the tables to perform the read/write stress test on.
    *
    * The file should be in the format:
    *
    * [
    *   {
    *     "schema": "table1_schema",
    *     "tableName": "table1_name"
    *   },
    *   {
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

    val sourceTables = mapper.convertValue(json, classOf[java.util.List[java.util.Map[String, String]]])

    sourceTables.asScala.map(s => {
      StressTestSourceTable(s.get("schema"), s.get("tableName"))
    }).toList
  }

  lazy val tablesToRead: List[StressTestSourceTable] =
    getTablesToReadFromSourcesFile(System.getenv(TestUtils.STRESS_TEST_SOURCES))

  override def runImpl(sparkSession: SparkSession,
                       resultBuilder: ClusterTestResultBuilder): Unit = {

    tablesToRead.foreach(source => {
      val targetSchema = "spark_test"
      val targetTableName = s"test_write_table_${source.tableName}_$randomSuffix"

      // Read write a basic table:
      super.readWriteSnowflakeTable(
        sparkSession,
        resultBuilder,
        TestUtils.sfOptionsNoTable,
        source.schema,
        source.tableName,
        targetSchema,
        targetTableName
      )

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
