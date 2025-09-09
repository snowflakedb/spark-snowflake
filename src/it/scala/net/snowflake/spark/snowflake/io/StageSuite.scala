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
  test ("manual test with s3 external stage") {
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
  test ("manual test with s3 external stage using temporary credentials") {
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
}
// scalastyle:on println
