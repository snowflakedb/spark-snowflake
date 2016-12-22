/*
 * Copyright 2015-2016 Snowflake Computing
 * Copyright 2015 TouchType Ltd
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

import java.io.File
import java.net.URI

import scala.util.matching.Regex
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration.Rule
import org.mockito.Matchers._
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.InputFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.fs.s3native.S3NInMemoryFileSystem
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import net.snowflake.spark.snowflake.Parameters.MergedParameters

import Utils.SNOWFLAKE_SOURCE_NAME

/**
 * Tests main DataFrame loading and writing functionality
 */
class SnowflakeSourceSuite extends BaseTest {

  // Extract a particular value from expected data
  def expectedValue(row: Int, column: Int) : Any = {
    TestUtils.expectedData(row).get(column)
  }

  /**
   * Prepare a regexp matching an unload for a given select query
   */
  def unloadQueryPattern(select: String) : Regex = {
    // Construct a full COPY INTO query from a given select
    val baseStr = s"""
      |COPY INTO 's3://test-bucket/temp-dir/.*/'
      |FROM ($select)
      |CREDENTIALS = (
      |    AWS_KEY_ID='test1'
      |    AWS_SECRET_KEY='test2'
      |.*)
      |FILE_FORMAT = (
      |    TYPE=CSV
      |    COMPRESSION=none
      |    FIELD_DELIMITER='|'
      |.*
      |    FIELD_OPTIONALLY_ENCLOSED_BY.*
      |    NULL_IF= ()
      |  )
      |MAX_FILE_SIZE = .*
      |"""

    // Escape parens and backslash, also match multi-line
    var escapedStr = "(?s)" +
      baseStr.stripMargin.trim
      .replace("\\", "\\\\")
      .replace("(", "\\(")
      .replace(")", "\\)")

    // Return as regexp
    escapedStr.r
  }

  test(
    "DefaultSource can load Snowflake UNLOAD output to a DataFrame") {
    val expectedQuery = unloadQueryPattern(
      """SELECT "testbyte", "testdate", "testdec152", "testdouble", "testfloat", "testint", "testlong", "testshort", "teststring", "testtimestamp" FROM test_table """
    )
    val mockSF = new MockSF(
      defaultParams,
      Map(new TableName("test_table").toString -> TestUtils.testSchema))

    // Assert that we've loaded and converted all data in the test file
    val source = new DefaultSource(mockSF.jdbcWrapper, _ => mockS3Client)
    val relation = source.createRelation(testSqlContext, defaultParams)
    val df = testSqlContext.baseRelationToDataFrame(relation)
    checkAnswer(df, TestUtils.expectedData)
    mockSF.verifyThatConnectionsWereClosed()
    mockSF.verifyThatExpectedQueriesWereIssuedForUnload(Seq(expectedQuery))
  }

  test("Can load output of Snowflake queries") {
    // scalastyle:off
    val expectedQuery = unloadQueryPattern(
      """SELECT "testbyte", "testdate" FROM (select testbyte, testdate from test_table where teststring = \'Unicode\'\'s樂趣\') """
    )
    val query =
      """select testbyte, testdate from test_table where teststring = 'Unicode''s樂趣'"""
    // scalastyle:on
    val querySchema =
      StructType(Seq(StructField("testbyte", ByteType), StructField("testdate", DateType)))

    // Test with dbtable parameter that wraps the query in parens:
    {
      val params = defaultParams + ("dbtable" -> s"($query)")
      val mockSF =
        new MockSF(defaultParams, Map(params("dbtable") -> querySchema))
      val relation = new DefaultSource(
        mockSF.jdbcWrapper, _ => mockS3Client).createRelation(testSqlContext, params)
      testSqlContext.baseRelationToDataFrame(relation).collect()
      mockSF.verifyThatConnectionsWereClosed()
      mockSF.verifyThatExpectedQueriesWereIssuedForUnload(Seq(expectedQuery))
    }

    // Test with query parameter
    {
      val params = defaultParams - "dbtable" + ("query" -> query)
      val mockSF = new MockSF(defaultParams, Map(s"($query)" -> querySchema))
      val relation = new DefaultSource(
        mockSF.jdbcWrapper, _ => mockS3Client).createRelation(testSqlContext, params)
      testSqlContext.baseRelationToDataFrame(relation).collect()
      mockSF.verifyThatConnectionsWereClosed()
      mockSF.verifyThatExpectedQueriesWereIssuedForUnload(Seq(expectedQuery))
    }
  }

  test("DefaultSource supports simple column filtering") {
    val expectedQuery = unloadQueryPattern(
      """SELECT "testbyte", "testdate" FROM test_table """
    )
    val mockSF =
      new MockSF(defaultParams, Map("test_table" -> TestUtils.testSchema))
    // Construct the source with a custom schema
    val source = new DefaultSource(mockSF.jdbcWrapper, _ => mockS3Client)
    val relation = source.createRelation(testSqlContext, defaultParams, TestUtils.testSchema)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testdate"), Array.empty[Filter])
    val prunedExpectedValues = Array(
      Row(expectedValue(0, 0), expectedValue(0, 1)),
      Row(expectedValue(1, 0), expectedValue(1, 1)),
      Row(expectedValue(2, 0), expectedValue(2, 1)),
      Row(expectedValue(3, 0), expectedValue(3, 1)))
    assert(rdd.collect() === prunedExpectedValues)
    mockSF.verifyThatConnectionsWereClosed()
    mockSF.verifyThatExpectedQueriesWereIssuedForUnload(Seq(expectedQuery))
  }

  test("DefaultSource supports user schema, pruned and filtered scans") {
    // scalastyle:off
    val expectedQuery = unloadQueryPattern(
      """SELECT "testbyte", "testdate" FROM test_table WHERE "teststring" = 'Unicode''s樂趣' AND "testdouble" > 1000.0 AND "testdouble" < 1.7976931348623157E308 AND "testfloat" >= 1.0 AND "testint" <= 43"""
    )
    // scalastyle:on
    val mockSF = new MockSF(
      defaultParams,
      Map(new TableName("test_table").toString -> TestUtils.testSchema))

    // Construct the source with a custom schema
    val source = new DefaultSource(mockSF.jdbcWrapper, _ => mockS3Client)
    val relation = source.createRelation(testSqlContext, defaultParams, TestUtils.testSchema)

    // Define a simple filter to only include a subset of rows
    val filters: Array[Filter] = Array(
      // scalastyle:off
      EqualTo("teststring", "Unicode's樂趣"),
      // scalastyle:on
      GreaterThan("testdouble", 1000.0),
      LessThan("testdouble", Double.MaxValue),
      GreaterThanOrEqual("testfloat", 1.0f),
      LessThanOrEqual("testint", 43))
    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testdate"), filters)

    // Technically this assertion should check that the RDD only returns a single row, but
    // since we've mocked out Snowflake our WHERE clause won't have had any effect.
    assert(rdd.collect().contains(Row(expectedValue(0,0), expectedValue(0,1))))
    mockSF.verifyThatConnectionsWereClosed()
    mockSF.verifyThatExpectedQueriesWereIssuedForUnload(Seq(expectedQuery))
  }

  // Helper function:
  // Make sure we wrote the data out ready for Snowflake load, in the expected formats.
  // The data should have been written to a random subdirectory of `tempdir`. Since we clear
  // `tempdir` between every unit test, there should only be one directory here.
  def verifyFileContent(expectedAnswer: Seq[Row]) : Unit = {
    assert(s3FileSystem.listStatus(new Path(s3TempDir)).length === 1)
    val dirWithFiles = s3FileSystem.listStatus(new Path(s3TempDir)).head.getPath.toUri.toString
    // Read the file as single strings
    val written = testSqlContext.read
        .options(Map(
          "header" -> "false",
          "inferSchema" -> "false",
          "delimiter" -> "~",  // Read as 1 record - we don't have ~ in data
          "quote" -> "\""))
        .schema(StructType(Seq(StructField("recordstring", StringType))))
        .csv(dirWithFiles)
    checkAnswer(written, TestUtils.expectedDataAsSingleStrings)
  }

  test("DefaultSource serializes data as CSV, then sends Snowflake COPY command") {
    val params = defaultParams ++ Map(
      "postactions" -> "GRANT SELECT ON %s TO jeremy",
      "sfcompress" -> "off")

    val expectedCommands = Seq(
      "alter session .*".r,
      "DROP TABLE IF EXISTS test_table_staging_.*".r,
      "CREATE TABLE IF NOT EXISTS test_table_staging.*()".r,
      "COPY INTO test_table_staging_.*".r,
      "GRANT SELECT ON test_table_staging_.* TO jeremy".r,
      "ALTER TABLE test_table SWAP WITH test_table_staging_.*".r,
      "DROP TABLE IF EXISTS test_table_staging_.*".r)

    val mockSF = new MockSF(
      defaultParams,
      Map(new TableName("test_table").toString -> TestUtils.testSchema))

    val relation = SnowflakeRelation(
      mockSF.jdbcWrapper,
      _ => mockS3Client,
      Parameters.mergeParameters(params),
      userSchema = None)(testSqlContext)
    relation.asInstanceOf[InsertableRelation].insert(expectedDataDF, overwrite = true)

    verifyFileContent(TestUtils.expectedDataAsSingleStrings)

    mockSF.verifyThatConnectionsWereClosed()
    mockSF.verifyThatExpectedQueriesWereIssued(expectedCommands)
  }

  // Snowflake-todo: Snowflake supports ambiguous column names, but should we
  // actually prohibit these
  ignore("Cannot write table with column names that become ambiguous under case insensitivity") {
    val mockSF = new MockSF(
      defaultParams,
      Map(new TableName("test_table").toString -> TestUtils.testSchema))

    val schema = StructType(Seq(StructField("a", IntegerType), StructField("A", IntegerType)))
    val df = testSqlContext.createDataFrame(sc.emptyRDD[Row], schema)
    val writer = new SnowflakeWriter(mockSF.jdbcWrapper, _ => mockS3Client)

    intercept[IllegalArgumentException] {
      writer.saveToSnowflake(
        testSqlContext, df, SaveMode.Append, Parameters.mergeParameters(defaultParams))
    }
    mockSF.verifyThatConnectionsWereClosed()
    mockSF.verifyThatExpectedQueriesWereIssued(Seq.empty)
  }

  test("Failed copies are handled gracefully when using a staging table") {
    val params = defaultParams ++ Map("usestagingtable" -> "true")

    val mockSF = new MockSF(
      defaultParams,
      Map(new TableName("test_table").toString -> TestUtils.testSchema),
      jdbcQueriesThatShouldFail = Seq("COPY INTO test_table_staging_.*".r))

    val expectedCommands = Seq(
      "alter session .*".r,
      "DROP TABLE IF EXISTS test_table_staging_.*".r,
      "CREATE TABLE IF NOT EXISTS test_table_staging_.*".r,
      "COPY INTO test_table_staging_.*".r,
      "DROP TABLE IF EXISTS test_table_staging_.*".r
    )

    val source = new DefaultSource(mockSF.jdbcWrapper, _ => mockS3Client)
    intercept[Exception] {
      source.createRelation(testSqlContext, SaveMode.Overwrite, params, expectedDataDF)
      mockSF.verifyThatConnectionsWereClosed()
      mockSF.verifyThatExpectedQueriesWereIssued(expectedCommands)
    }
    mockSF.verifyThatConnectionsWereClosed()
    mockSF.verifyThatExpectedQueriesWereIssued(expectedCommands)
  }

  test("Append SaveMode doesn't destroy existing data") {
    val expectedCommands =
      Seq("alter session .*".r,
          "CREATE TABLE IF NOT EXISTS test_table .*".r,
          "COPY INTO test_table.*".r)

    val mockSF = new MockSF(
      defaultParams,
      Map(new TableName(defaultParams("dbtable")).toString -> null))

    val source = new DefaultSource(mockSF.jdbcWrapper, _ => mockS3Client)
    source.createRelation(testSqlContext, SaveMode.Append, defaultParams, expectedDataDF)

    // This test is "appending" to an empty table, so we expect all our test data to be
    // the only content.
    verifyFileContent(TestUtils.expectedDataAsSingleStrings)

    mockSF.verifyThatConnectionsWereClosed()
    mockSF.verifyThatExpectedQueriesWereIssued(expectedCommands)
  }

  test("configuring maxlength on string columns") {
    val longStrMetadata = new MetadataBuilder().putLong("maxlength", 512).build()
    val shortStrMetadata = new MetadataBuilder().putLong("maxlength", 10).build()
    val schema = StructType(
      StructField("long_str", StringType, metadata = longStrMetadata) ::
      StructField("short_str", StringType, metadata = shortStrMetadata) ::
      StructField("default_str", StringType) ::
      Nil)
    val df = testSqlContext.createDataFrame(sc.emptyRDD[Row], schema)
    val createTableCommand =
      DefaultSnowflakeWriter.createTableSql(df, MergedParameters.apply(defaultParams)).trim
    val expectedCreateTableCommand =
      """CREATE TABLE IF NOT EXISTS test_table ("long_str" VARCHAR(512),""" +
        """ "short_str" VARCHAR(10), "default_str" STRING)"""
    assert(createTableCommand === expectedCreateTableCommand)
  }

  test("Respect SaveMode.ErrorIfExists when table exists") {
    val mockSF = new MockSF(
      defaultParams,
      Map(new TableName(defaultParams("dbtable")).toString -> null))
    val errIfExistsSource = new DefaultSource(mockSF.jdbcWrapper, _ => mockS3Client)
    intercept[Exception] {
      errIfExistsSource.createRelation(
        testSqlContext, SaveMode.ErrorIfExists, defaultParams, expectedDataDF)
    }
    mockSF.verifyThatConnectionsWereClosed()
    mockSF.verifyThatExpectedQueriesWereIssued(Seq.empty)
  }

  test("Do nothing when table exists if SaveMode = Ignore") {
    val mockSF = new MockSF(
      defaultParams,
      Map(new TableName(defaultParams("dbtable")).toString -> null))
    val ignoreSource = new DefaultSource(mockSF.jdbcWrapper, _ => mockS3Client)
    ignoreSource.createRelation(testSqlContext, SaveMode.Ignore, defaultParams, expectedDataDF)
    mockSF.verifyThatConnectionsWereClosed()
    mockSF.verifyThatExpectedQueriesWereIssued(Seq.empty)
  }

  test("Cannot save when 'query' parameter is specified instead of 'dbtable'") {
    val invalidParams = defaultParams - "dbtable" + ("query" -> "select 1")
    val e1 = intercept[IllegalArgumentException] {
      expectedDataDF.write.format(SNOWFLAKE_SOURCE_NAME).options(invalidParams).save();
    }
    assert(e1.getMessage.contains("dbtable"))
  }

  test("Public Scala API rejects invalid parameter maps") {
    val invalidParams = Map("dbtable" -> "foo") // missing tempdir and url

    val e1 = intercept[IllegalArgumentException] {
      expectedDataDF.write.format(SNOWFLAKE_SOURCE_NAME).options(invalidParams).save();
    }
    assert(e1.getMessage.contains("tempdir"))

    val e2 = intercept[IllegalArgumentException] {
      expectedDataDF.write.format(SNOWFLAKE_SOURCE_NAME).options(invalidParams).save();

    }
    assert(e2.getMessage.contains("tempdir"))
  }

  test("DefaultSource has default constructor, required by Data Source API") {
    new DefaultSource()
  }

  test("Saves throw error message if S3 Block FileSystem would be used") {
    val params = defaultParams + ("tempdir" -> defaultParams("tempdir").replace("s3n", "s3"))
    val e = intercept[IllegalArgumentException] {
      expectedDataDF
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .mode(SaveMode.Append)
        .options(params)
        .save()
    }
    assert(e.getMessage.contains("Block FileSystem"))
  }

  test("Loads throw error message if S3 Block FileSystem would be used") {
    val params = defaultParams + ("tempdir" -> defaultParams("tempdir").replace("s3n", "s3"))
    val e = intercept[IllegalArgumentException] {
      testSqlContext.read.format(SNOWFLAKE_SOURCE_NAME).options(params).load()
    }
    assert(e.getMessage.contains("Block FileSystem"))
  }
}
