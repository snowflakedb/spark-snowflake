/*
 * Copyright 2015-2019 Snowflake Computing
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

import java.util.TimeZone

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.{TestHook, TestHookFlag}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._

// scalastyle:off println
class UploadRetrySuite extends IntegrationSuiteBase{
  // Add some options for default for testing.
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()

  private val test_table_large_result: String =
    s"test_table_large_result_$randomSuffix"
  private val test_table_write: String = s"test_table_write_$randomSuffix"

  private val largeStringValue =
    s"""spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |""".stripMargin.filter(_ >= ' ')

  val LARGE_TABLE_ROW_COUNT = 900000
  lazy val setupLargeResultTable = {
    jdbcUpdate(s"""create or replace table $test_table_large_result (
                  | int_c int, c_string string(1024) )""".stripMargin)

    jdbcUpdate(s"""insert into $test_table_large_result select
                  | seq4(), '$largeStringValue'
                  | from table(generator(rowcount => $LARGE_TABLE_ROW_COUNT))""".stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_large_result")
      .load()

    tmpdf.createOrReplaceTempView("test_table_large_result")
    true
  }

  // Disable super beforeAll as this test initialize it's own Spark Context
  override def beforeAll(): Unit = {}

  def setUpSparkContext(retryCount: Int): Unit = {
    // Always run in UTC
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"))

    val conf = new SparkConf()
      conf.setMaster(s"local[*, $retryCount]")
      conf.setAppName("SnowflakeRetryTest")
      conf.setExecutorEnv("spark.task.maxFailures", s"$retryCount")

    sc = SparkContext.getOrCreate(conf)

    // Initialize variables
    connectorOptions = loadConfig()
    connectorOptionsNoTable = connectorOptions.filterKeys(_ != "dbtable")
    connectorOptionsNoExternalStageNoTable =
      connectorOptionsNoTable.filterKeys(_ != "tempdir")
    params = Parameters.mergeParameters(connectorOptions)
    // Create a single string with the Spark SQL options
    connectorOptionsString = connectorOptionsNoTable
      .map {
        case (key, value) => s"""$key "$value""""
      }
      .mkString(" , ")

    conn = DefaultJDBCWrapper.getConnector(params)

    // Force UTC also on the JDBC connection
    jdbcUpdate("alter session set timezone='UTC'")


    // Use fewer partitions to make tests faster
    sparkSession = SparkSession.builder
      .master(s"local[*, $retryCount]")
      .appName("SnowflakeRetryTest")
      .config("spark.sql.shuffle.partitions", "6")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.task.maxFailures", s"$retryCount")
      .getOrCreate()


    // There is bug for Date.equals() to compare Date with different timezone,
    // so set up the timezone to work around it.
    val gmtTimezone = TimeZone.getTimeZone("GMT")
    TimeZone.setDefault(gmtTimezone)

    connectorOptionsNoTable.foreach(tup => {
      thisConnectorOptionsNoTable += tup
    })

    // Setup special options for this test
    thisConnectorOptionsNoTable += ("partition_size_in_mb" -> "20")
    thisConnectorOptionsNoTable += ("time_output_format" -> "HH24:MI:SS.FF")
    thisConnectorOptionsNoTable += ("s3maxfilesize" -> "1000001")
    thisConnectorOptionsNoTable += ("jdbc_query_result_format" -> "arrow")
  }

  def getDataFrame(): DataFrame = {
    jdbcUpdate(s"""create or replace table $test_table_large_result (
                  | int_c int, c_string string(1024) )""".stripMargin)

    jdbcUpdate(s"""insert into $test_table_large_result select
                  | seq4(), '$largeStringValue'
                  | from table(generator(rowcount => $LARGE_TABLE_ROW_COUNT))""".stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_large_result")
      .load()

    tmpdf.createOrReplaceTempView("test_table_large_result")

    // Set max_retry_count to small value to avoid bock testcase too long.
    thisConnectorOptionsNoTable += ("max_retry_count" -> "2")

    // Use fewer partitions to make tests faster
    val tmpDF = sparkSession
      .sql(s"select * from test_table_large_result where int_c < 1000")

    // Write the Data back to snowflake
    jdbcUpdate(s"drop table if exists $test_table_write")

    tmpDF
  }

  // Test for hitting exception when uploading data to cloud with retry
  test("Test uploading retry works") {
    setUpSparkContext(3)

    // Enable test hook to simulate upload error
    TestHook.enableTestFlagOnly(TestHookFlag.TH_UPLOAD_RAISE_EXCEPTION_WITH_COUNT)

    val tmpDF = getDataFrame()
    // Test use_exponential_backoff is 'on' or 'off'
    // Test fails for two times and pass the third time
    tmpDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .option("truncate_table", "off")
      .option("usestagingtable", "on")
      .option("use_exponential_backoff", "on")
      .mode(SaveMode.Overwrite)
      .save()
    tmpDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .option("truncate_table", "off")
      .option("usestagingtable", "on")
      .option("use_exponential_backoff", "off")
      .mode(SaveMode.Overwrite)
      .save()

    // Reset env.
    thisConnectorOptionsNoTable -= "max_retry_count"
    TestHook.disableTestHook()
  }

  // Negative test for hitting exception when uploading data to cloud with retry
  test("Test uploading retry works negative test") {
    setUpSparkContext(2)

    // Enable test hook to simulate upload error
    TestHook.enableTestFlagOnly(TestHookFlag.TH_UPLOAD_RAISE_EXCEPTION_WITH_COUNT)

    val tmpDF = getDataFrame()
    // Test use_exponential_backoff is 'on' or 'off'
    // Test fails for two times and pass the third time
    assertThrows[Exception] {
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_write)
        .option("truncate_table", "off")
        .option("usestagingtable", "on")
        .option("use_exponential_backoff", "on")
        .mode(SaveMode.Overwrite)
        .save()
    }
    assertThrows[Exception] {
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_write)
        .option("truncate_table", "off")
        .option("usestagingtable", "on")
        .option("use_exponential_backoff", "off")
        .mode(SaveMode.Overwrite)
        .save()
    }

    // Reset env.
    thisConnectorOptionsNoTable -= "max_retry_count"
    TestHook.disableTestHook()
  }


  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_large_result")
      jdbcUpdate(s"drop table if exists $test_table_write")
    } finally {
      TestHook.disableTestHook()
      super.afterAll()
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
      sc.stop()
      sparkSession.stop()
    }
  }
}
// scalastyle:on println

