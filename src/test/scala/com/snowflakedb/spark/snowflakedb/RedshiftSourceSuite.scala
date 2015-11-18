/*
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

package com.snowflakedb.spark.snowflakedb

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
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.fs.s3native.S3NInMemoryFileSystem
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll, Matchers}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.snowflakedb.spark.snowflakedb.Parameters.MergedParameters

private class TestContext extends SparkContext("local", "RedshiftSourceSuite") {

  /**
   * A text file containing fake unloaded Redshift data of all supported types
   */
  val testData = new File("src/test/resources/snowflake_unload_data.txt").toURI.toString

  override def newAPIHadoopFile[K, V, F <: InputFormat[K, V]](
      path: String,
      fClass: Class[F],
      kClass: Class[K],
      vClass: Class[V],
      conf: Configuration = hadoopConfiguration): RDD[(K, V)] = {
    super.newAPIHadoopFile[K, V, F](testData, fClass, kClass, vClass, conf)
  }
}

/**
 * Tests main DataFrame loading and writing functionality
 */
class RedshiftSourceSuite
  extends QueryTest
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  /**
   * Spark Context with Hadoop file overridden to point at our local test data file for this suite,
   * no matter what temp directory was generated and requested.
   */
  private var sc: SparkContext = _

  private var testSqlContext: SQLContext = _

  private var expectedDataDF: DataFrame = _

  private var mockS3Client: AmazonS3Client = _

  private var s3FileSystem: FileSystem = _

  private val s3TempDir: String = "s3n://test-bucket/temp-dir/"

  // Parameters common to most tests. Some parameters are overridden in specific tests.
  private def defaultParams: Map[String, String] = Map(
    "tempdir" -> s3TempDir,
    "dbtable" -> "test_table",
    "sfurl" -> "account.snowflakecomputing.com:443",
    "sfuser" -> "username",
    "sfpassword" -> "password"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = new TestContext
    sc.hadoopConfiguration.set("fs.s3n.impl", classOf[S3NInMemoryFileSystem].getName)
    // We need to use a DirectOutputCommitter to work around an issue which occurs with renames
    // while using the mocked S3 filesystem.
    sc.hadoopConfiguration.set("spark.sql.sources.outputCommitterClass",
      classOf[DirectOutputCommitter].getName)
    sc.hadoopConfiguration.set("mapred.output.committer.class",
      classOf[DirectOutputCommitter].getName)
    sc.hadoopConfiguration.set("fs.s3.awsAccessKeyId", "test1")
    sc.hadoopConfiguration.set("fs.s3.awsSecretAccessKey", "test2")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "test1")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "test2")
    // Configure a mock S3 client so that we don't hit errors when trying to access AWS in tests.
    mockS3Client = Mockito.mock(classOf[AmazonS3Client], Mockito.RETURNS_SMART_NULLS)
    when(mockS3Client.getBucketLifecycleConfiguration(anyString())).thenReturn(
      new BucketLifecycleConfiguration().withRules(
        new Rule().withPrefix("").withStatus(BucketLifecycleConfiguration.ENABLED)
      ))
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    s3FileSystem = FileSystem.get(new URI(s3TempDir), sc.hadoopConfiguration)
    testSqlContext = new SQLContext(sc)
    expectedDataDF =
      testSqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    testSqlContext = null
    expectedDataDF = null
    mockS3Client = null
    FileSystem.closeAll()
  }

  override def afterAll(): Unit = {
    sc.stop()
    super.afterAll()
  }

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
      |)
      |FILE_FORMAT = (
      |    TYPE=CSV
      |    COMPRESSION=none
      |    FIELD_DELIMITER='|'
      |    .*
      |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
      |    NULL_IF= ()
      |  )
      |MAX_FILE_SIZE = 10000000
      |"""
    // Escape parens and backslash, make RegExp
    baseStr.stripMargin.trim.
      replace("\\", "\\\\").
      replace("(", "\\(").replace(")", "\\)").r
  }

  test(
    "DefaultSource can load Redshift UNLOAD output to a DataFrame") {
    val expectedQuery = unloadQueryPattern(
      """SELECT "testbyte", "testdate", "testdec152", "testdouble", "testfloat", "testint", "testlong", "testshort", "teststring", "testtimestamp" FROM test_table """
    )
    val mockRedshift = new MockRedshift(
      defaultParams,
      Map(new TableName("test_table").toString -> TestUtils.testSchema))

    // Assert that we've loaded and converted all data in the test file
    val source = new DefaultSource(mockRedshift.jdbcWrapper, _ => mockS3Client)
    val relation = source.createRelation(testSqlContext, defaultParams)
    val df = testSqlContext.baseRelationToDataFrame(relation)
    checkAnswer(df, TestUtils.expectedData)
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssuedForUnload(Seq(expectedQuery))
  }

  test("Can load output of Redshift queries") {
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
      val mockRedshift =
        new MockRedshift(defaultParams, Map(params("dbtable") -> querySchema))
      val relation = new DefaultSource(
        mockRedshift.jdbcWrapper, _ => mockS3Client).createRelation(testSqlContext, params)
      testSqlContext.baseRelationToDataFrame(relation).collect()
      mockRedshift.verifyThatConnectionsWereClosed()
      mockRedshift.verifyThatExpectedQueriesWereIssuedForUnload(Seq(expectedQuery))
    }

    // Test with query parameter
    {
      val params = defaultParams - "dbtable" + ("query" -> query)
      val mockRedshift = new MockRedshift(defaultParams, Map(s"($query)" -> querySchema))
      val relation = new DefaultSource(
        mockRedshift.jdbcWrapper, _ => mockS3Client).createRelation(testSqlContext, params)
      testSqlContext.baseRelationToDataFrame(relation).collect()
      mockRedshift.verifyThatConnectionsWereClosed()
      mockRedshift.verifyThatExpectedQueriesWereIssuedForUnload(Seq(expectedQuery))
    }
  }

  test("DefaultSource supports simple column filtering") {
    val expectedQuery = unloadQueryPattern(
      """SELECT "testbyte", "testdate" FROM test_table """
    )
    val mockRedshift =
      new MockRedshift(defaultParams, Map("test_table" -> TestUtils.testSchema))
    // Construct the source with a custom schema
    val source = new DefaultSource(mockRedshift.jdbcWrapper, _ => mockS3Client)
    val relation = source.createRelation(testSqlContext, defaultParams, TestUtils.testSchema)

    val rdd = relation.asInstanceOf[PrunedFilteredScan]
      .buildScan(Array("testbyte", "testdate"), Array.empty[Filter])
    val prunedExpectedValues = Array(
      Row(expectedValue(0, 0), expectedValue(0, 1)),
      Row(expectedValue(1, 0), expectedValue(1, 1)),
      Row(expectedValue(2, 0), expectedValue(2, 1)),
      Row(expectedValue(3, 0), expectedValue(3, 1)))
    assert(rdd.collect() === prunedExpectedValues)
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssuedForUnload(Seq(expectedQuery))
  }

  test("DefaultSource supports user schema, pruned and filtered scans") {
    // scalastyle:off
    val expectedQuery = unloadQueryPattern(
      """SELECT "testbyte", "testdate" FROM test_table WHERE "teststring" = 'Unicode''s樂趣' AND "testdouble" > 1000.0 AND "testdouble" < 1.7976931348623157E308 AND "testfloat" >= 1.0 AND "testint" <= 43"""
    )
    // scalastyle:on
    val mockRedshift = new MockRedshift(
      defaultParams,
      Map(new TableName("test_table").toString -> TestUtils.testSchema))

    // Construct the source with a custom schema
    val source = new DefaultSource(mockRedshift.jdbcWrapper, _ => mockS3Client)
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
    // since we've mocked out Redshift our WHERE clause won't have had any effect.
    assert(rdd.collect().contains(Row(expectedValue(0,0), expectedValue(0,1))))
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssuedForUnload(Seq(expectedQuery))
  }

  // Helper function:
  // Make sure we wrote the data out ready for Snowflake load, in the expected formats.
  // The data should have been written to a random subdirectory of `tempdir`. Since we clear
  // `tempdir` between every unit test, there should only be one directory here.
  def verifyFileContent(expectedAnswer: Seq[Row]) : Unit = {
    assert(s3FileSystem.listStatus(new Path(s3TempDir)).length === 1)
    val dirWithFiles = s3FileSystem.listStatus(new Path(s3TempDir)).head.getPath.toUri.toString
    // Read the file as single strings
    val written = testSqlContext.read.format("com.databricks.spark.csv")
        .options(Map(
          "header" -> "false",
          "inferSchema" -> "false",
          "delimiter" -> "~",  // Read as 1 record - we don't have ~ in data
          "quote" -> "\""))
        .schema(StructType(Seq(StructField("recordstring", StringType))))
        .load(dirWithFiles)
    checkAnswer(written, TestUtils.expectedDataAsSingleStrings)
  }

  test("DefaultSource serializes data as CSV, then sends Snowflake COPY command") {
    val params = defaultParams ++ Map(
      "postactions" -> "GRANT SELECT ON %s TO jeremy",
      "sfcompress" -> "off")

    val expectedCommands = Seq(
      "DROP TABLE IF EXISTS test_table_staging_.*".r,
      "CREATE TABLE IF NOT EXISTS test_table_staging.*()".r,
      "COPY INTO test_table_staging_.*".r,
      "GRANT SELECT ON test_table_staging_.* TO jeremy".r,
      "ALTER TABLE test_table SWAP WITH test_table_staging_.*".r,
      "DROP TABLE IF EXISTS test_table_staging_.*".r)

    val mockRedshift = new MockRedshift(
      defaultParams,
      Map(new TableName("test_table").toString -> TestUtils.testSchema))

    val relation = SnowflakeRelation(
      mockRedshift.jdbcWrapper,
      _ => mockS3Client,
      Parameters.mergeParameters(params),
      userSchema = None)(testSqlContext)
    relation.asInstanceOf[InsertableRelation].insert(expectedDataDF, overwrite = true)

    verifyFileContent(TestUtils.expectedDataAsSingleStrings)

    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(expectedCommands)
  }

  // Snowflake-todo: Snowflake supports ambiguous column names, but should we
  // actually prohibit these
  ignore("Cannot write table with column names that become ambiguous under case insensitivity") {
    val mockRedshift = new MockRedshift(
      defaultParams,
      Map(new TableName("test_table").toString -> TestUtils.testSchema))

    val schema = StructType(Seq(StructField("a", IntegerType), StructField("A", IntegerType)))
    val df = testSqlContext.createDataFrame(sc.emptyRDD[Row], schema)
    val writer = new SnowflakeWriter(mockRedshift.jdbcWrapper, _ => mockS3Client)

    intercept[IllegalArgumentException] {
      writer.saveToSnowflake(
        testSqlContext, df, SaveMode.Append, Parameters.mergeParameters(defaultParams))
    }
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq.empty)
  }

  test("Failed copies are handled gracefully when using a staging table") {
    val params = defaultParams ++ Map("usestagingtable" -> "true")

    val mockRedshift = new MockRedshift(
      defaultParams,
      Map(new TableName("test_table").toString -> TestUtils.testSchema),
      jdbcQueriesThatShouldFail = Seq("COPY INTO test_table_staging_.*".r))

    val expectedCommands = Seq(
      "DROP TABLE IF EXISTS test_table_staging_.*".r,
      "CREATE TABLE IF NOT EXISTS test_table_staging_.*".r,
      "COPY INTO test_table_staging_.*".r,
      "DROP TABLE IF EXISTS test_table_staging_.*".r
    )

    val source = new DefaultSource(mockRedshift.jdbcWrapper, _ => mockS3Client)
    intercept[Exception] {
      source.createRelation(testSqlContext, SaveMode.Overwrite, params, expectedDataDF)
      mockRedshift.verifyThatConnectionsWereClosed()
      mockRedshift.verifyThatExpectedQueriesWereIssued(expectedCommands)
    }
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(expectedCommands)
  }

  test("Append SaveMode doesn't destroy existing data") {
    val expectedCommands =
      Seq("CREATE TABLE IF NOT EXISTS test_table .*".r,
          "COPY INTO test_table.*".r)

    val mockRedshift = new MockRedshift(
      defaultParams,
      Map(new TableName(defaultParams("dbtable")).toString -> null))

    val source = new DefaultSource(mockRedshift.jdbcWrapper, _ => mockS3Client)
    val savedDf =
      source.createRelation(testSqlContext, SaveMode.Append, defaultParams, expectedDataDF)

    // This test is "appending" to an empty table, so we expect all our test data to be
    // the only content.
    verifyFileContent(TestUtils.expectedDataAsSingleStrings)

    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(expectedCommands)
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
    val mockRedshift = new MockRedshift(
      defaultParams,
      Map(new TableName(defaultParams("dbtable")).toString -> null))
    val errIfExistsSource = new DefaultSource(mockRedshift.jdbcWrapper, _ => mockS3Client)
    intercept[Exception] {
      errIfExistsSource.createRelation(
        testSqlContext, SaveMode.ErrorIfExists, defaultParams, expectedDataDF)
    }
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq.empty)
  }

  test("Do nothing when table exists if SaveMode = Ignore") {
    val mockRedshift = new MockRedshift(
      defaultParams,
      Map(new TableName(defaultParams("dbtable")).toString -> null))
    val ignoreSource = new DefaultSource(mockRedshift.jdbcWrapper, _ => mockS3Client)
    ignoreSource.createRelation(testSqlContext, SaveMode.Ignore, defaultParams, expectedDataDF)
    mockRedshift.verifyThatConnectionsWereClosed()
    mockRedshift.verifyThatExpectedQueriesWereIssued(Seq.empty)
  }

  test("Cannot save when 'query' parameter is specified instead of 'dbtable'") {
    val invalidParams = defaultParams - "dbtable" + ("query" -> "select 1")
    val e1 = intercept[IllegalArgumentException] {
      expectedDataDF.saveAsSnowflakeTable(invalidParams)
    }
    assert(e1.getMessage.contains("dbtable"))
  }

  test("Public Scala API rejects invalid parameter maps") {
    val invalidParams = Map("dbtable" -> "foo") // missing tempdir and url

    val e1 = intercept[IllegalArgumentException] {
      expectedDataDF.saveAsSnowflakeTable(invalidParams)
    }
    assert(e1.getMessage.contains("tempdir"))

    val e2 = intercept[IllegalArgumentException] {
      testSqlContext.snowflakeTable(invalidParams)
    }
    assert(e2.getMessage.contains("tempdir"))
  }

  test("DefaultSource has default constructor, required by Data Source API") {
    new DefaultSource()
  }

  test("Saves throw error message if S3 Block FileSystem would be used") {
    val params = defaultParams + ("tempdir" -> defaultParams("tempdir").replace("s3n", "s3"))
    val e = intercept[IllegalArgumentException] {
      expectedDataDF.saveAsSnowflakeTable(params)
    }
    assert(e.getMessage.contains("Block FileSystem"))
  }

  test("Loads throw error message if S3 Block FileSystem would be used") {
    val params = defaultParams + ("tempdir" -> defaultParams("tempdir").replace("s3n", "s3"))
    val e = intercept[IllegalArgumentException] {
      testSqlContext.read.format("com.snowflakedb.spark.snowflakedb").options(params).load()
    }
    assert(e.getMessage.contains("Block FileSystem"))
  }
}
