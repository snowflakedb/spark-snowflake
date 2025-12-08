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
import net.snowflake.client.jdbc.{SnowflakeFileTransferMetadataV1, SnowflakeSQLException}
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
  private val test_table_write_with_quote: String =
    s""""test_table_write_$randomSuffix""""
  private val internal_stage_name = s"test_stage_$randomSuffix"

  private val largeStringValue = Random.alphanumeric take 1024 mkString ""
  private val test_table_large_result: String =
    s"test_table_large_result_$randomSuffix"
  private val LARGE_TABLE_ROW_COUNT = 1000

  private def setupLargeResultTable(sfOptions: Map[String, String]): Unit = {
    val param = Parameters.MergedParameters(sfOptions)
    val connection = TestUtils.getServerConnection(param)

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

  test("test s3 internal stage functions") {
    var sfOptionsNoTable: Map[String, String] =
      replaceOption(
        connectorOptionsNoTable,
        "sfurl",
        "sfctest0.snowflakecomputing.com"
      )
    sfOptionsNoTable += ("upload_chunk_size_in_mb" -> "5")

    val param = Parameters.MergedParameters(sfOptionsNoTable)
    val connection = TestUtils.getServerConnection(param)

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
    val connection = TestUtils.getServerConnection(param)

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
    sfOptionsNoTable += ("tempdir" ->
      s"wasb://test_container_name@test_account.blob.core.windows.net/")
    sfOptionsNoTable += ("temporary_azure_sas_token" ->
      (s"?sig=test_test_test_test_test_test_test_test_test_test_test" +
      s"_test_test_test_test_test_test_fak&spr=https&sp=rwdl&sr=c"))

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
    val connection = TestUtils.getServerConnection(param)

    try {
      // The credential for the external stage is fake.
      val azureExternalStage = ExternalAzureStorage(
        param,
        containerName = "test_fake_container",
        azureAccount = "test_fake_account",
        azureEndpoint = "blob.core.windows.net",
        azureSAS =
          "?sig=test_test_test_test_test_test_test_test_test_test_test_test" +
            "_test_test_test_test_test_fak&spr=https&sp=rwdl&sr=c",
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
    val connection = TestUtils.getServerConnection(param)

    try {
      // The credential for the external stage is fake.
      val s3ExternalStage = ExternalS3Storage(
        param,
        bucketName = "test_fake_bucket",
        awsId = "TEST_TEST_TEST_TEST1",
        awsKey = "TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST2",
        param.expectedPartitionCount,
        pref = "test_dir",
        connection = connection,
        useRegionUrl = None,
        regionName = None,
        stageEndPoint = None
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

  // Manually test AWS external stage
  // You need to set below 3 environment variables.
  private val MAN_TEST_AWS_TEMPDIR = "MAN_TEST_AWS_TEMPDIR"
  private val MAN_TEST_AWS_ACCESS_KEY = "MAN_TEST_AWS_ACCESS_KEY"
  private val MAN_TEST_AWS_SECRET_KEY = "MAN_TEST_AWS_SECRET_KEY"
  ignore ("manual test with s3 external stage") {
    if (System.getenv(MAN_TEST_AWS_TEMPDIR) != null &&
      System.getenv(MAN_TEST_AWS_ACCESS_KEY) != null &&
      System.getenv(MAN_TEST_AWS_SECRET_KEY) != null) {
      var sfOptionsNoTable = connectorOptionsNoTable
      setupLargeResultTable(sfOptionsNoTable)

      // Set AWS external stage options
      sfOptionsNoTable += ("tempdir" -> System.getenv(MAN_TEST_AWS_TEMPDIR))
      sfOptionsNoTable += ("awsaccesskey" -> System.getenv(MAN_TEST_AWS_ACCESS_KEY))
      sfOptionsNoTable += ("awssecretkey" -> System.getenv(MAN_TEST_AWS_SECRET_KEY))

      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptionsNoTable)
        .option("dbtable", test_table_large_result)
        .load()

      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptionsNoTable)
        .option("dbtable", test_table_write)
        .option("truncate_table", "off")
        .option("usestagingtable", "on")
        .option("purge", "true")
        .mode(SaveMode.Overwrite)
        .save()

      val dfTarget = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptionsNoTable)
        .option("dbtable", test_table_write)
        .load()
      assert(dfTarget.count() == LARGE_TABLE_ROW_COUNT)

      Utils.runQuery(
        sfOptionsNoTable,
        s"drop table if exists $test_table_large_result"
      )
    }
  }

  // Manually test AWS external stage with temporary aws credentials
  // You need to set below 3 environment variables and MAN_TEST_AWS_TEMPDIR
  private val MAN_TEST_AWS_TMP_ACCESS_KEY = "MAN_TEST_AWS_TMP_ACCESS_KEY"
  private val MAN_TEST_AWS_TMP_SECRET_KEY = "MAN_TEST_AWS_TMP_SECRET_KEY"
  private val MAN_TEST_AWS_TMP_SESSION_TOKEN = "MAN_TEST_AWS_TMP_SESSION_TOKEN"
  ignore ("manual test with s3 external stage using temporary credentials") {
    if (System.getenv(MAN_TEST_AWS_TEMPDIR) != null &&
      System.getenv(MAN_TEST_AWS_TMP_ACCESS_KEY) != null &&
      System.getenv(MAN_TEST_AWS_TMP_SECRET_KEY) != null &&
      System.getenv(MAN_TEST_AWS_TMP_SESSION_TOKEN) != null) {
      var sfOptionsNoTable = connectorOptionsNoTable
      setupLargeResultTable(sfOptionsNoTable)

      // Set AWS external stage options
      sfOptionsNoTable += ("tempdir" -> System.getenv(MAN_TEST_AWS_TEMPDIR))
      sfOptionsNoTable += ("temporary_aws_access_key_id" -> System.getenv(MAN_TEST_AWS_TMP_ACCESS_KEY))
      sfOptionsNoTable += ("temporary_aws_secret_access_key" -> System.getenv(MAN_TEST_AWS_TMP_SECRET_KEY))
      sfOptionsNoTable += ("temporary_aws_session_token" -> System.getenv(MAN_TEST_AWS_TMP_SESSION_TOKEN))

      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptionsNoTable)
        .option("dbtable", test_table_large_result)
        .load()

      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptionsNoTable)
        .option("dbtable", test_table_write)
        .option("truncate_table", "off")
        .option("usestagingtable", "on")
        .option("purge", "true")
        .mode(SaveMode.Overwrite)
        .save()

      val dfTarget = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptionsNoTable)
        .option("dbtable", test_table_write)
        .load()
      assert(dfTarget.count() == LARGE_TABLE_ROW_COUNT)

      Utils.runQuery(
        sfOptionsNoTable,
        s"drop table if exists $test_table_large_result"
      )
    }
  }

  test("misc for CloudStorageOperations") {
    println(CloudStorageOperations.DEFAULT_PARALLELISM)
    println(CloudStorageOperations.S3_MAX_RETRIES)
    println(CloudStorageOperations.S3_MAX_TIMEOUT_MS)
    println(CloudStorageOperations.AES)
    println(CloudStorageOperations.AMZ_KEY)
    println(CloudStorageOperations.AMZ_IV)
    println(CloudStorageOperations.DATA_CIPHER)
    println(CloudStorageOperations.KEY_CIPHER)
    println(CloudStorageOperations.AMZ_MATDESC)
    println(CloudStorageOperations.AZ_ENCRYPTIONDATA)
    println(CloudStorageOperations.AZ_IV)
    println(CloudStorageOperations.AZ_KEY_WRAP)
    println(CloudStorageOperations.AZ_KEY)
    println(CloudStorageOperations.AZ_MATDESC)
  }

  test("test CloudStorage.checkUploadMetadata") {
    val sfOptionsNoTable: Map[String, String] =
      replaceOption(
        connectorOptionsNoTable,
        "sfurl",
        "sfctest0.snowflakecomputing.com"
      )

    val param = Parameters.MergedParameters(sfOptionsNoTable)
    val connection = TestUtils.getServerConnection(param)

    try {
      // The credential for the external stage is fake.
      val s3ExternalStage = ExternalS3Storage(
        param,
        bucketName = "test_fake_bucket",
        awsId = "TEST_TEST_TEST_TEST1",
        awsKey = "TEST_TEST_TEST_TEST_TEST_TEST_TEST_TEST2",
        param.expectedPartitionCount,
        pref = "test_dir",
        connection = connection,
        useRegionUrl = None,
        regionName = None,
        stageEndPoint = None
      )

      val storageInfo: Map[String, String] = Map()
      val fileTransferMetadata = new SnowflakeFileTransferMetadataV1(
        null, null, null, null, null, null, null)

      // Test the target function with negative and positive cases
      assertThrows[SnowflakeConnectorException]({
        s3ExternalStage.checkUploadMetadata(None, None)
      })
      assertThrows[SnowflakeConnectorException]({
        s3ExternalStage.checkUploadMetadata(Some(storageInfo), Some(fileTransferMetadata))
      })
      s3ExternalStage.checkUploadMetadata(None, Some(fileTransferMetadata))
      s3ExternalStage.checkUploadMetadata(Some(storageInfo), None)
    } finally {
      connection.close()
    }
  }

  test("inject test with azure internal stage") {
    var sfOptionsNoTable: Map[String, String] =
      replaceOption(
        connectorOptionsNoTable,
        "sfurl",
        "sfctest0.east-us-2.azure.snowflakecomputing.com"
      )
    // Avoid multiple retry because this is negative test
    sfOptionsNoTable = replaceOption(sfOptionsNoTable, "max_retry_count", "1")
    // setup test table
    setupLargeResultTable(sfOptionsNoTable)

    try {
      sfOptionsNoTable =
        replaceOption(sfOptionsNoTable, "use_copy_unload", "true")
      // inject exception
      TestHook.enableTestFlagOnly(TestHookFlag.TH_FAIL_CREATE_DOWNLOAD_STREAM)
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
      // inject exception
      TestHook.enableTestFlagOnly(TestHookFlag.TH_FAIL_CREATE_UPLOAD_STREAM)
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
    } finally {
      Utils.runQuery(
        sfOptionsNoTable,
        s"drop table if exists $test_table_large_result"
      )
      TestHook.disableTestHook()
    }
  }

  // Test Parameters.PARAM_INTERNAL_STAGING_TABLE_NAME_REMOVE_QUOTES_ONLY
  // This option is internal only, this test case can be removed when
  // this option is removed.
  test("test Parameters.PARAM_INTERNAL_STAGING_TABLE_NAME_REMOVE_QUOTES_ONLY") {
    try {
      setupLargeResultTable(connectorOptionsNoTable)

      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", s"$test_table_large_result")
        .load()

      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table_write_with_quote)
        .option(
          Parameters.PARAM_INTERNAL_STAGING_TABLE_NAME_REMOVE_QUOTES_ONLY,
          "true"
        )
        .mode(SaveMode.Overwrite)
        .save()

      assert(
        sparkSession.read
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(connectorOptionsNoTable)
          .option("dbtable", test_table_write_with_quote)
          .load()
          .count() == LARGE_TABLE_ROW_COUNT
      )
    } finally {
      Utils.runQuery(
        connectorOptionsNoTable,
        s"drop table if exists $test_table_large_result"
      )
      Utils.runQuery(
        connectorOptionsNoTable,
        s"drop table if exists $test_table_write_with_quote"
      )
      TestHook.disableTestHook()
    }
  }

  test("negative test Parameters.PARAM_S3_STAGE_VPCE_DNS_NAME") {
    val ex = intercept[SnowflakeSQLException] {
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option(Parameters.PARAM_S3_STAGE_VPCE_DNS_NAME, "negative_test_invalid_s3_dns")
        .option("query", "select 'any-query'")
        .load()
    }
    assert(ex.getMessage.contains("invalid value [negative_test_invalid_s3_dns] for parameter"))
  }

  // Test user-specified Snowflake stage for data loading
  // This feature allows users to reuse a persistent stage instead of creating
  // a temporary stage for each write operation.
  test("test snowflake_stage parameter for write operations") {
    val userStageName = s"user_specified_stage_$randomSuffix"
    val testTableName = s"test_snowflake_stage_$randomSuffix"

    try {
      // Setup: Create a persistent internal stage
      Utils.runQuery(
        connectorOptionsNoTable,
        s"CREATE OR REPLACE STAGE $userStageName"
      )

      // Setup: Create test data
      setupLargeResultTable(connectorOptionsNoTable)

      // Read source data
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table_large_result)
        .load()

      // Write using user-specified internal stage
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", testTableName)
        .option(Parameters.PARAM_SNOWFLAKE_STAGE, userStageName)
        .option("truncate_table", "off")
        .option("usestagingtable", "off")
        .mode(SaveMode.Overwrite)
        .save()

      // Verify data was loaded correctly
      val resultCount = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", testTableName)
        .load()
        .count()

      assert(resultCount == LARGE_TABLE_ROW_COUNT,
        s"Expected $LARGE_TABLE_ROW_COUNT rows but got $resultCount")

      // Verify the user-specified stage still exists (was not dropped)
      val param = Parameters.MergedParameters(connectorOptionsNoTable)
      val connection = TestUtils.getServerConnection(param)
      try {
        val stmt = connection.createStatement()
        val rs = stmt.executeQuery(s"SHOW STAGES LIKE '$userStageName'")
        assert(rs.next(), "User-specified stage should still exist after write operation")
      } finally {
        connection.close()
      }

    } finally {
      // Cleanup
      Utils.runQuery(connectorOptionsNoTable, s"DROP TABLE IF EXISTS $testTableName")
      Utils.runQuery(connectorOptionsNoTable, s"DROP TABLE IF EXISTS $test_table_large_result")
      Utils.runQuery(connectorOptionsNoTable, s"DROP STAGE IF EXISTS $userStageName")
    }
  }

  // Test that multiple writes with the same user-specified stage work correctly
  // This simulates the streaming/micro-batch use case
  test("test snowflake_stage parameter for multiple write operations") {
    val userStageName = s"user_stage_multi_write_$randomSuffix"
    val testTableName = s"test_multi_write_$randomSuffix"

    try {
      // Setup: Create a persistent internal stage
      Utils.runQuery(
        connectorOptionsNoTable,
        s"CREATE OR REPLACE STAGE $userStageName"
      )

      // Create a simple test DataFrame
      import testImplicits._
      val testData1 = Seq((1, "first"), (2, "second")).toDF("id", "value")
      val testData2 = Seq((3, "third"), (4, "fourth")).toDF("id", "value")

      // First write using user-specified internal stage
      testData1.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", testTableName)
        .option(Parameters.PARAM_SNOWFLAKE_STAGE, userStageName)
        .mode(SaveMode.Overwrite)
        .save()

      // Second write (append) using same user-specified internal stage
      testData2.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", testTableName)
        .option(Parameters.PARAM_SNOWFLAKE_STAGE, userStageName)
        .mode(SaveMode.Append)
        .save()

      // Verify all data was loaded correctly
      val resultCount = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", testTableName)
        .load()
        .count()

      assert(resultCount == 4,
        s"Expected 4 rows after two writes but got $resultCount")

    } finally {
      // Cleanup
      Utils.runQuery(connectorOptionsNoTable, s"DROP TABLE IF EXISTS $testTableName")
      Utils.runQuery(connectorOptionsNoTable, s"DROP STAGE IF EXISTS $userStageName")
    }
  }

  // Test PURGE functionality with user-specified Snowflake stage using fully qualified name
  // Verifies that:
  // 1. Fully qualified stage name (database.schema.stage) works correctly
  // 2. With purge=true, intermediate files are automatically cleaned up
  // 3. With purge=false, files remain in the stage
  // 4. Pre-existing files in the stage are NOT affected by purge
  test("test snowflake_stage with fully qualified name and purge option") {
    val stageName = s"user_stage_fqn_purge_test_$randomSuffix"
    val testTableName = s"test_fqn_purge_$randomSuffix"
    val preExistingFileName = "pre_existing_file.txt"

    // Get database and schema from connector options to build fully qualified name
    val database = connectorOptionsNoTable.getOrElse("sfdatabase",
      connectorOptionsNoTable.getOrElse("sfDatabase", ""))
    val schema = connectorOptionsNoTable.getOrElse("sfschema",
      connectorOptionsNoTable.getOrElse("sfSchema", "public"))
    val fullyQualifiedStageName = s"$database.$schema.$stageName"

    val param = Parameters.MergedParameters(connectorOptionsNoTable)
    val connection = TestUtils.getServerConnection(param)

    try {
      val stmt = connection.createStatement()

      // Setup: Create a persistent internal stage using fully qualified name
      stmt.execute(s"CREATE OR REPLACE STAGE $fullyQualifiedStageName")
      println(s"Created stage with fully qualified name: $fullyQualifiedStageName")

      // Setup: Add a pre-existing file to the stage that should NOT be deleted
      stmt.execute(
        s"PUT file://$temp_file_full_name1 @$fullyQualifiedStageName/$preExistingFileName AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
      )

      // Verify pre-existing file is in stage
      var rs = stmt.executeQuery(s"LIST @$fullyQualifiedStageName PATTERN='.*$preExistingFileName.*'")
      assert(rs.next(), "Pre-existing file should be in stage before test")

      // Create test DataFrame
      import testImplicits._
      val testData = Seq((1, "test1"), (2, "test2")).toDF("id", "value")

      // ============================================================
      // Test 1: Write with purge=false using FQN stage - files should remain
      // ============================================================
      testData.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", testTableName)
        .option(Parameters.PARAM_SNOWFLAKE_STAGE, fullyQualifiedStageName)
        .option(Parameters.PARAM_PURGE, "false")
        .mode(SaveMode.Overwrite)
        .save()

      // Verify data was loaded
      var resultCount = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", testTableName)
        .load()
        .count()
      assert(resultCount == 2, s"Expected 2 rows but got $resultCount")

      // Count files in stage (should have pre-existing + uploaded files)
      rs = stmt.executeQuery(s"LIST @$fullyQualifiedStageName")
      var fileCount = 0
      while (rs.next()) {
        fileCount += 1
        println(s"File in stage (after purge=false): ${rs.getString(1)}")
      }
      // Should have at least pre-existing file + uploaded data files
      assert(fileCount >= 2,
        s"With purge=false, files should remain in stage. Found $fileCount files")

      // Verify pre-existing file is still there
      rs = stmt.executeQuery(s"LIST @$fullyQualifiedStageName PATTERN='.*$preExistingFileName.*'")
      assert(rs.next(), "Pre-existing file should still be in stage after purge=false write")

      // ============================================================
      // Test 2: Write with purge=true using FQN stage - uploaded files should be cleaned
      // ============================================================
      val testData2 = Seq((3, "test3"), (4, "test4")).toDF("id", "value")

      testData2.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", testTableName)
        .option(Parameters.PARAM_SNOWFLAKE_STAGE, fullyQualifiedStageName)
        .option(Parameters.PARAM_PURGE, "true")
        .mode(SaveMode.Append)
        .save()

      // Verify data was loaded
      resultCount = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", testTableName)
        .load()
        .count()
      assert(resultCount == 4, s"Expected 4 rows after append but got $resultCount")

      // After purge=true, the files from this write should be cleaned
      // But pre-existing file and files from previous write (without purge) may still exist
      rs = stmt.executeQuery(s"LIST @$fullyQualifiedStageName PATTERN='.*$preExistingFileName.*'")
      assert(rs.next(),
        "CRITICAL: Pre-existing file should NOT be deleted by purge! " +
        "Purge should only affect files from the current COPY operation.")

      // ============================================================
      // Test 3: Verify purge only affects specific prefix, not entire stage
      // ============================================================
      // The pre-existing file should still exist after all operations
      rs = stmt.executeQuery(s"LIST @$fullyQualifiedStageName PATTERN='.*$preExistingFileName.*'")
      val preExistingStillExists = rs.next()
      assert(preExistingStillExists,
        "Pre-existing files in user stage must be preserved! " +
        "PURGE should only delete files from the current write operation's prefix.")

      println(s"SUCCESS: Fully qualified stage '$fullyQualifiedStageName' works correctly")
      println(s"SUCCESS: Pre-existing file '$preExistingFileName' was preserved after PURGE operations")

    } finally {
      // Cleanup
      try {
        connection.createStatement().execute(s"DROP TABLE IF EXISTS $testTableName")
        connection.createStatement().execute(s"DROP STAGE IF EXISTS $fullyQualifiedStageName")
      } finally {
        connection.close()
      }
    }
  }
}
// scalastyle:on println
