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

package net.snowflake.spark.snowflake.io

import java.sql.{Date, Timestamp}
import java.util.TimeZone

import net.snowflake.client.jdbc.SnowflakeConnectionV1
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.{TestHook, TestHookFlag}
import net.snowflake.spark.snowflake._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalactic.source.Position
import org.scalatest.Tag

import scala.collection.immutable.HashMap
import scala.util.Random

// scalastyle:off println
class ExternalStageSuite extends IntegrationSuiteBase {
  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    if (extraTestForCoverage) {
      super.test(testName, testTags: _*)(testFun)(pos)
    } else {
      println(s"Skip test StreamingSuite.'$testName'.")
    }
  }

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

  val LARGE_TABLE_ROW_COUNT = 100000
  private def setupLargeResultTable(sfOptions: Map[String, String]): Unit = {
    val param = Parameters.MergedParameters(sfOptions)
    val connection = DefaultJDBCWrapper.getConnector(param)
    connection.createStatement.executeQuery(
      s"""create or replace table $test_table_large_result (
         | int_c int, c_string string(1024) )""".stripMargin)

    connection.createStatement.executeQuery(
      s"""insert into $test_table_large_result select
      | seq4(), '$largeStringValue'
      | from table(generator(rowcount => $LARGE_TABLE_ROW_COUNT))
      | """.stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("dbtable", s"$test_table_large_result")
      .load()

    tmpdf.createOrReplaceTempView("test_table_large_result")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

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
  }

  val internal_stage_name = s"test_stage_for_external_$randomSuffix"

  private def getAWSCredential(sfOptions: Map[String, String],
                               stageName: String,
                               isWrite: Boolean,
                               fileName: String = ""
                              ): Map[String, String] = {
    val param = Parameters.MergedParameters(sfOptions)
    val connection = DefaultJDBCWrapper.getConnector(param)
    val stageManager =
      new SFInternalStage(
        isWrite,
        param,
        stageName,
        connection.asInstanceOf[SnowflakeConnectionV1],
        fileName
      )
    @transient val keyIds = stageManager.getKeyIds

    var storageInfo: Map[String, String] = new HashMap[String, String]

    val (_, queryId, smkId) = if (keyIds.nonEmpty) keyIds.head else ("", "", "")
    storageInfo += StorageInfo.QUERY_ID -> queryId
    storageInfo += StorageInfo.SMK_ID -> smkId
    storageInfo += StorageInfo.MASTER_KEY -> stageManager.masterKey

    val stageLocation = stageManager.stageLocation
    val url = "([^/]+)/?(.*)".r
    val url(bucket, path) = stageLocation
    storageInfo += StorageInfo.BUCKET_NAME -> bucket
    storageInfo += StorageInfo.AWS_ID -> stageManager.awsId.get
    storageInfo += StorageInfo.AWS_KEY -> stageManager.awsKey.get
    stageManager.awsToken.foreach(storageInfo += StorageInfo.AWS_TOKEN -> _)

    val prefix: String =
      if (path.isEmpty) path else if (path.endsWith("/")) path else path + "/"
    storageInfo += StorageInfo.PREFIX -> prefix
    storageInfo
  }

  private def getAzureCredential(sfOptions: Map[String, String],
                               stageName: String,
                               isWrite: Boolean,
                               fileName: String = ""
                              ): Map[String, String] = {
    val param = Parameters.MergedParameters(sfOptions)
    val connection = DefaultJDBCWrapper.getConnector(param)
    val stageManager =
      new SFInternalStage(
        isWrite,
        param,
        stageName,
        connection.asInstanceOf[SnowflakeConnectionV1],
        fileName
      )

    val keyIds = stageManager.getKeyIds

    var storageInfo: Map[String, String] = new HashMap[String, String]()

    val (_, queryId, smkId) = if (keyIds.nonEmpty) keyIds.head else ("", "", "")
    storageInfo += StorageInfo.QUERY_ID -> queryId
    storageInfo += StorageInfo.SMK_ID -> smkId
    storageInfo += StorageInfo.MASTER_KEY -> stageManager.masterKey

    val stageLocation = stageManager.stageLocation
    val url = "([^/]+)/?(.*)".r
    val url(container, path) = stageLocation
    storageInfo += StorageInfo.CONTAINER_NAME -> container
    storageInfo += StorageInfo.AZURE_SAS -> stageManager.azureSAS.get
    storageInfo += StorageInfo.AZURE_ACCOUNT -> stageManager.azureAccountName.get
    storageInfo += StorageInfo.AZURE_END_POINT -> stageManager.azureEndpoint.get

    val prefix: String =
      if (path.isEmpty) path else if (path.endsWith("/")) path else path + "/"
    storageInfo += StorageInfo.PREFIX -> prefix
    storageInfo
  }

  private def replaceOption(originalOptions: Map[String, String],
                            optionName: String,
                            optionValue: String): Map[String, String] = {
    var resultOptions: Map[String, String] = Map()
    originalOptions.foreach(tup => {
      if (!tup._1.equalsIgnoreCase(optionName)) {
        resultOptions += tup
      }
    })
    resultOptions += (optionName -> optionValue)
    resultOptions
  }

  // large table read and write.
  test("test with s3 external stage") {
    var sfOptionsNoTable: Map[String, String] =
      replaceOption(thisConnectorOptionsNoTable,
        "sfurl", "sfctest0.snowflakecomputing.com")
    sfOptionsNoTable = replaceOption(sfOptionsNoTable,
      "use_copy_unload", "true")
    setupLargeResultTable(sfOptionsNoTable)

    Utils.runQuery(sfOptionsNoTable,
      s"create or replace stage $internal_stage_name " +
        s"FILE_FORMAT = (TYPE='CSV' COMPRESSION = NONE)")

    val storageInfo = getAWSCredential(sfOptionsNoTable, internal_stage_name, true)
    // Add external stage options.

    sfOptionsNoTable += ("tempdir" -> s"s3n://${storageInfo.get(StorageInfo.BUCKET_NAME).get}/${storageInfo.get(StorageInfo.PREFIX).get}")
    sfOptionsNoTable += ("temporary_aws_access_key_id" -> s"${storageInfo.get(StorageInfo.AWS_ID).get}")
    sfOptionsNoTable += ("temporary_aws_secret_access_key" -> s"${storageInfo.get(StorageInfo.AWS_KEY).get}")
    sfOptionsNoTable += ("temporary_aws_session_token" -> s"${storageInfo.get(StorageInfo.AWS_TOKEN).get}")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", s"$test_table_large_result")
      .load()

    // Write the Data back to snowflake
    jdbcUpdate(s"drop table if exists $test_table_write")
    tmpDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", test_table_write)
      .option("truncate_table", "off")
      .option("usestagingtable", "on")
      .mode(SaveMode.Overwrite)
      .save()

    // check row count is correct
    val targetRowCount = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", s"$test_table_write")
      .load()
      .count()
    assert(targetRowCount == LARGE_TABLE_ROW_COUNT)

    Utils.runQuery(sfOptionsNoTable,
      s"drop stage $internal_stage_name")
  }

  test("test with Azure external stage") {
    var sfOptionsNoTable: Map[String, String] =
      replaceOption(thisConnectorOptionsNoTable,
        "sfurl", "sfctest0.east-us-2.azure.snowflakecomputing.com")
    sfOptionsNoTable = replaceOption(sfOptionsNoTable,
      "use_copy_unload", "true")
    setupLargeResultTable(sfOptionsNoTable)

    Utils.runQuery(sfOptionsNoTable,
      s"create or replace stage $internal_stage_name " +
        s"FILE_FORMAT = (TYPE='CSV' COMPRESSION = NONE)")

    val storageInfo = getAzureCredential(sfOptionsNoTable, internal_stage_name, true)
    // Add external stage options.
    sfOptionsNoTable += ("tempdir" -> s"wasb://${storageInfo.get(StorageInfo.CONTAINER_NAME).get}@${storageInfo.get(StorageInfo.AZURE_ACCOUNT).get}.${storageInfo.get(StorageInfo.AZURE_END_POINT).get}/")
    sfOptionsNoTable += ("temporary_azure_sas_token" -> s"${storageInfo.get(StorageInfo.AZURE_SAS).get}")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", s"$test_table_large_result")
      .load()

    // Write the Data back to snowflake
    jdbcUpdate(s"drop table if exists $test_table_write")
    tmpDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", test_table_write)
      .option("truncate_table", "off")
      .option("usestagingtable", "on")
      .option("purge", "true")
      .mode(SaveMode.Overwrite)
      .save()

    // check row count is correct
    val targetRowCount = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", s"$test_table_write")
      .load()
      .count()
    assert(targetRowCount == LARGE_TABLE_ROW_COUNT)

    Utils.runQuery(sfOptionsNoTable,
      s"drop stage $internal_stage_name")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_large_result")
      jdbcUpdate(s"drop table if exists $test_table_write")

      jdbcUpdate(s"drop stage if exists $internal_stage_name")
      //  s"drop stage $internal_stage_name")
    } finally {
      TestHook.disableTestHook()
      super.afterAll()
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
    }
  }
}
// scalastyle:on println

