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

package net.snowflake.spark.snowflake.io

import java.io.File
import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.{TestHook, TestHookFlag}
import net.snowflake.spark.snowflake._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.scalactic.source.Position
import org.scalatest.Tag

import scala.util.Random

// scalastyle:off println
class StageSuite extends IntegrationSuiteBase {
  // This test suite only run when env EXTRA_TEST_FOR_COVERAGE is set as true
  override def test(testName: String, testTags: Tag*)(
    testFun: => Any
  )(implicit pos: Position): Unit = {
    if (extraTestForCoverage) {
      super.test(testName, testTags: _*)(testFun)(pos)
    } else {
      super.ignore(testName, testTags: _*)(testFun)(pos)
    }
  }

  private val temp_file1 = File.createTempFile("test_file_", ".csv")
  private val temp_file_full_name1 = temp_file1.getPath
  private val temp_file_name1 = temp_file1.getName

  private val temp_file2 = File.createTempFile("test_file_", ".csv")
  private val temp_file_full_name2 = temp_file2.getPath
  private val temp_file_name2 = temp_file2.getName

  private val temp_file3 = File.createTempFile("test_file_", ".csv")
  private val temp_file_full_name3 = temp_file3.getPath
  private val temp_file_name3 = temp_file3.getName

  private val test_table_write: String = s"test_table_write_$randomSuffix"
  private val internal_stage_name = s"test_stage_$randomSuffix"

  private val largeStringValue = Random.alphanumeric take 1024 mkString ""
  private val test_table_large_result: String =
    s"test_table_large_result_$randomSuffix"
  private val LARGE_TABLE_ROW_COUNT = 1000

  private def setupLargeResultTable(sfOptions: Map[String, String]): Unit = {
    val param = Parameters.MergedParameters(sfOptions)
    val connection = DefaultJDBCWrapper.getConnector(param)

    connection.createStatement.executeQuery(
      s"""create or replace table $test_table_large_result (
         | int_c int, c_string string(1024) )""".stripMargin
    )

    connection.createStatement.executeQuery(
      s"""insert into $test_table_large_result select
      | seq4(), '$largeStringValue'
      | from table(generator(rowcount => $LARGE_TABLE_ROW_COUNT))
      | """.stripMargin
    )

    connection.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Setup 3 temp files for testing.
    FileUtils.write(temp_file1, "abc, 123,\n")
    FileUtils.write(temp_file2, "abc, 123,\n")
    FileUtils.write(temp_file3, "abc, 123,\n")
  }

  override def afterAll(): Unit = {
    try {
      FileUtils.deleteQuietly(temp_file1)
      FileUtils.deleteQuietly(temp_file2)
      FileUtils.deleteQuietly(temp_file3)
    } finally {
      TestHook.disableTestHook()
      super.afterAll()
    }
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

  test("test s3 internal stage functions") {
    val sfOptionsNoTable: Map[String, String] =
      replaceOption(
        connectorOptionsNoTable,
        "sfurl",
        "sfctest0.snowflakecomputing.com"
      )

    val param = Parameters.MergedParameters(sfOptionsNoTable)
    val connection = DefaultJDBCWrapper.getConnector(param)

    try {
      val stmt = connection.createStatement()
      stmt.execute(s"create or replace stage $internal_stage_name")
      stmt.execute(s"put file://$temp_file_full_name1 @$internal_stage_name/")
      stmt.execute(s"put file://$temp_file_full_name2 @$internal_stage_name/")
      stmt.execute(s"put file://$temp_file_full_name3 @$internal_stage_name/")
      var rs = stmt.executeQuery(s"ls @$internal_stage_name")
      Utils.printResultSet(rs)

      val s3InternalStage =
        InternalS3Storage(param, internal_stage_name, connection,
          CloudStorageOperations.DEFAULT_PARALLELISM)

      // Test interface functions: fileExists()/deleteFile()/deleteFiles()
      assert(s3InternalStage.fileExists(s"$temp_file_name1.gz"))
      assert(!s3InternalStage.fileExists(s"non_exist_file"))
      s3InternalStage.deleteFile(s"$temp_file_name1.gz")
      s3InternalStage.deleteFiles(
        List(s"$temp_file_name2.gz", s"$temp_file_name3.gz")
      )

      // after delete functions, the stage is empty
      rs = stmt.executeQuery(s"ls @$internal_stage_name")
      assert(!rs.next())
    } finally {
      val stmt = connection.createStatement()
      stmt.execute(s"drop stage if exists $internal_stage_name")
      stmt.execute(s"drop table if exists $test_table_large_result")
      stmt.execute(s"drop table if exists $test_table_write")
      connection.close()
    }
  }

  test("test azure internal stage functions") {
    val sfOptionsNoTable: Map[String, String] =
      replaceOption(
        connectorOptionsNoTable,
        "sfurl",
        "sfctest0.east-us-2.azure.snowflakecomputing.com"
      )

    val param = Parameters.MergedParameters(sfOptionsNoTable)
    val connection = DefaultJDBCWrapper.getConnector(param)

    try {
      val stmt = connection.createStatement()
      stmt.execute(s"create or replace stage $internal_stage_name")
      stmt.execute(s"put file://$temp_file_full_name1 @$internal_stage_name/")
      stmt.execute(s"put file://$temp_file_full_name2 @$internal_stage_name/")
      stmt.execute(s"put file://$temp_file_full_name3 @$internal_stage_name/")
      var rs = stmt.executeQuery(s"ls @$internal_stage_name")
      Utils.printResultSet(rs)

      val azureInternalStage =
        InternalAzureStorage(param, internal_stage_name, connection)

      // Test interface functions: fileExists()/deleteFile()/deleteFiles()
      assert(azureInternalStage.fileExists(s"$temp_file_name1.gz"))
      assert(!azureInternalStage.fileExists(s"non_exist_file"))
      azureInternalStage.deleteFile(s"$temp_file_name1.gz")
      azureInternalStage.deleteFiles(
        List(s"$temp_file_name2.gz", s"$temp_file_name3.gz")
      )

      // after delete functions, the stage is empty
      rs = stmt.executeQuery(s"ls @$internal_stage_name")
      assert(!rs.next())
    } finally {
      val stmt = connection.createStatement()
      stmt.execute(s"drop stage if exists $internal_stage_name")
      stmt.execute(s"drop table if exists $test_table_large_result")
      stmt.execute(s"drop table if exists $test_table_write")
      connection.close()
    }
  }

  test("negative test with s3 external stage") {
    var sfOptionsNoTable: Map[String, String] =
      replaceOption(
        connectorOptionsNoTable,
        "sfurl",
        "sfctest0.snowflakecomputing.com"
      )
    setupLargeResultTable(sfOptionsNoTable)

    // Set test external key, the dataframe reading should fail.
    sfOptionsNoTable += ("tempdir" -> s"s3n://test_bucket_name/test_prefix/test_prefix2")
    sfOptionsNoTable += ("awsaccesskey" -> s"TEST_TEST_TEST_TEST1")
    sfOptionsNoTable += ("awssecretkey" -> s"TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST2")

    // Use copy unload for read, the read will fail
    sfOptionsNoTable =
      replaceOption(sfOptionsNoTable, "use_copy_unload", "true")
    assertThrows[Exception] {
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptionsNoTable)
        .option("dbtable", s"$test_table_large_result")
        .load()
        .collect()
    }

    // disable copy unload, the write will fail
    sfOptionsNoTable =
      replaceOption(sfOptionsNoTable, "use_copy_unload", "false")
    assertThrows[Throwable] {
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptionsNoTable)
        .option("dbtable", s"$test_table_large_result")
        .load()

      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptionsNoTable)
        .option("dbtable", test_table_write)
        .option("truncate_table", "off")
        .option("usestagingtable", "on")
        .mode(SaveMode.Overwrite)
        .save()
    }

    Utils.runQuery(
      sfOptionsNoTable,
      s"drop table if exists $test_table_large_result"
    )
  }

  test("negative test with azure external stage") {
    var sfOptionsNoTable: Map[String, String] =
      replaceOption(
        connectorOptionsNoTable,
        "sfurl",
        "sfctest0.east-us-2.azure.snowflakecomputing.com"
      )
    setupLargeResultTable(sfOptionsNoTable)

    // Set test external key, the dataframe reading should fail.
    sfOptionsNoTable += ("tempdir" -> s"wasb://test_container_name@test_account.blob.core.windows.net/")
    sfOptionsNoTable += ("temporary_azure_sas_token" -> s"?sig=test_test_test_test_test_test_test_test_test_test_test_test_test_test_test_test_test_fak&spr=https&sp=rwdl&sr=c")

    // Use copy unload for read, the read will fail
    sfOptionsNoTable =
      replaceOption(sfOptionsNoTable, "use_copy_unload", "true")
    assertThrows[Exception] {
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptionsNoTable)
        .option("dbtable", s"$test_table_large_result")
        .load()
        .collect()
    }

    // disable copy unload , the write will fail
    sfOptionsNoTable =
      replaceOption(sfOptionsNoTable, "use_copy_unload", "false")
    assertThrows[Throwable] {
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptionsNoTable)
        .option("dbtable", s"$test_table_large_result")
        .load()

      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptionsNoTable)
        .option("dbtable", test_table_write)
        .option("truncate_table", "off")
        .option("usestagingtable", "on")
        .mode(SaveMode.Overwrite)
        .save()
    }

    Utils.runQuery(
      sfOptionsNoTable,
      s"drop table if exists $test_table_large_result"
    )
  }

  test("Negative test Azure external stage interface functions") {
    var sfOptionsNoTable: Map[String, String] =
      replaceOption(
        connectorOptionsNoTable,
        "sfurl",
        "sfctest0.east-us-2.azure.snowflakecomputing.com"
      )

    val param = Parameters.MergedParameters(sfOptionsNoTable)
    val connection = DefaultJDBCWrapper.getConnector(param)

    try {
      // The credential for the external stage is fake.
      val azureExternalStage = ExternalAzureStorage(
        containerName = "test_fake_container",
        azureAccount = "test_fake_account",
        azureEndpoint = "blob.core.windows.net",
        azureSAS =
          "?sig=test_test_test_test_test_test_test_test_test_test_test_test_test_test_test_test_test_fak&spr=https&sp=rwdl&sr=c",
        param.proxyInfo,
        param.maxRetryCount,
        param.sfURL,
        param.useExponentialBackoff,
        param.expectedPartitionCount,
        pref = "test_dir",
        connection = connection
      )

      assertThrows[Exception]({
        azureExternalStage.fileExists(s"non_exist_file")
      })
      assertThrows[Exception]({
        azureExternalStage.deleteFile(s"$temp_file_name1.gz")
      })
      assertThrows[Exception]({
        azureExternalStage.deleteFiles(
          List(s"$temp_file_name2.gz", s"$temp_file_name3.gz")
        )
      })
      assertThrows[Exception]({
        azureExternalStage.download(sc, SupportedFormat.CSV, true, "test_dir")
      })
      assertThrows[Exception]({
        azureExternalStage.createDownloadStreamWithRetry(
          "test_file", true, Map.empty, 2)
      })
    } finally {
      val stmt = connection.createStatement()
      stmt.execute(s"drop stage if exists $internal_stage_name")
      stmt.execute(s"drop table if exists $test_table_large_result")
      stmt.execute(s"drop table if exists $test_table_write")
      connection.close()
    }
  }

  test("Negative test S3 external stage interface functions") {
    val sfOptionsNoTable: Map[String, String] =
      replaceOption(
        connectorOptionsNoTable,
        "sfurl",
        "sfctest0.snowflakecomputing.com"
      )

    val param = Parameters.MergedParameters(sfOptionsNoTable)
    val connection = DefaultJDBCWrapper.getConnector(param)

    try {
      // The credential for the external stage is fake.
      val s3ExternalStage = ExternalS3Storage(
        bucketName = "test_fake_bucket",
        awsId = "TEST_TEST_TEST_TEST1",
        awsKey = "TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST2",
        param.proxyInfo,
        param.maxRetryCount,
        param.sfURL,
        param.useExponentialBackoff,
        param.expectedPartitionCount,
        pref = "test_dir",
        connection = connection
      )

      assertThrows[Exception]({
        s3ExternalStage.fileExists(s"non_exist_file")
      })
      assertThrows[Exception]({
        s3ExternalStage.deleteFile(s"$temp_file_name1.gz")
      })
      assertThrows[Exception]({
        s3ExternalStage.deleteFiles(
          List(s"$temp_file_name2.gz", s"$temp_file_name3.gz")
        )
      })
      assertThrows[Exception]({
        s3ExternalStage.download(sc, SupportedFormat.CSV, true, "test_dir")
      })
      assertThrows[Exception]({
        s3ExternalStage.createDownloadStreamWithRetry(
          "test_file", true, Map.empty, 2)
      })
    } finally {
      val stmt = connection.createStatement()
      stmt.execute(s"drop stage if exists $internal_stage_name")
      stmt.execute(s"drop table if exists $test_table_large_result")
      stmt.execute(s"drop table if exists $test_table_write")
      connection.close()
    }
  }

  test("misc for CloudStorageOperations") {
    // Below test are for test coverage only.
    // If their value needs to be changed in the future,
    // feel free to update this test.
    assert(CloudStorageOperations.DEFAULT_PARALLELISM.equals(10))
    assert(CloudStorageOperations.S3_MAX_RETRIES.equals(6))
    assert(CloudStorageOperations.S3_MAX_TIMEOUT_MS.equals(30 * 1000))
  }
}
// scalastyle:on println
