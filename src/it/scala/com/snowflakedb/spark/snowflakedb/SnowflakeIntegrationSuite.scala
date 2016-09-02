/*
 * Copyright 2015-2016 Snowflake Computing
 * Copyright 2015 Databricks
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

import java.sql.SQLException

import org.apache.spark.sql.{AnalysisException, Row, SQLContext, SaveMode}
import org.apache.spark.sql.types._

/**
 * End-to-end tests which run against a real Snowflake cluster.
 */
class SnowflakeIntegrationSuite extends IntegrationSuiteBase {

  private val test_table: String = s"test_table_$randomSuffix"
  private val test_table2: String = s"test_table2_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()

    conn.prepareStatement("drop table if exists test_table").executeUpdate()
    conn.prepareStatement("drop table if exists test_table2").executeUpdate()
    conn.commit()

    def createTable(tableName: String): Unit = {
      jdbcUpdate(
        s"""
           |create table $tableName (
           |   testbyte int,
           |   testdate date,
           |   testdec152 decimal(15,2),
           |   testdouble double,
           |   testfloat float,
           |   testint int,
           |   testlong bigint,
           |   testshort smallint,
           |   teststring string,
           |   testtimestamp timestamp
           )
      """.stripMargin
      )
      // scalastyle:off
      jdbcUpdate(
        s"""
           |insert into $tableName values
           |(1, '2015-07-01', 1234567890123.45, 1234152.12312498, 1.0, 42,
           |  1239012341823719, 23, 'Unicode''s樂趣', '2015-07-01 00:00:00.001'),
           |(2, '1960-01-02', 1.01, 2, 3, 4, 5, 6, '"', '2015-07-02 12:34:56.789'),
           |(3, '2999-12-31', -1.01, -2, -3, -4, -5, -6, '\\\\''"|', '1950-12-31 17:00:00.001'),
           |(null, null, null, null, null, null, null, null, null, null)
         """.stripMargin
      )
      // scalastyle:on

      conn.commit()
    }

    createTable(test_table)
    createTable(test_table2)
  }

  override def afterAll(): Unit = {
    try {
      conn.prepareStatement(s"drop table if exists $test_table").executeUpdate()
      conn.prepareStatement(s"drop table if exists $test_table2").executeUpdate()
      conn.commit()
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    runSql(
      s"""
         | create temporary table test_table(
         |   testbyte int,
         |   testdate date,
         |   testdec152 decimal(15,2),
         |   testdouble double,
         |   testfloat float,
         |   testint int,
         |   testlong bigint,
         |   testshort smallint,
         |   teststring string,
         |   testtimestamp timestamp
         | )
         | using com.snowflakedb.spark.snowflakedb
         | options(
         |   $connectorOptionsString
         |   , dbtable \"$test_table\"
         | )
       """.stripMargin
    )

    runSql(
      s"""
         | create temporary table test_table2(
         |   testbyte int,
         |   testdate date,
         |   testdec152 decimal,
         |   testdouble double,
         |   testfloat float,
         |   testint int,
         |   testlong bigint,
         |   testshort smallint,
         |   teststring string,
         |   testtimestamp timestamp
         | )
         | using com.snowflakedb.spark.snowflakedb
         | options(
         |   $connectorOptionsString
         |   , dbtable \"$test_table2\"
         | )
       """.stripMargin
    )
  }

  test("DefaultSource can load Snowflake COPY unload output to a DataFrame") {
    checkAnswer(
      sqlContext.sql("select * from test_table"),
      TestUtils.expectedData)
  }

  test("count() on DataFrame created from a Snowflake table") {
    checkAnswer(
      sqlContext.sql("select count(*) from test_table"),
      Seq(Row(TestUtils.expectedData.length))
    )
  }

  test("count() on DataFrame created from a Snowflake query") {
    val loadedDf = sqlContext.read
      .format("com.snowflakedb.spark.snowflakedb")
      .options(connectorOptionsNoTable)
      // scalastyle:off
      .option("query", s"select * from $test_table where teststring = 'Unicode''s樂趣'")
      // scalastyle:on
      .load()
    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  test("Can load output when 'dbtable' is a subquery wrapped in parentheses") {
    // scalastyle:off
    val query =
      s"""
        |(select testbyte
        |from $test_table
        |where testfloat = 3.0)
      """.stripMargin
    // scalastyle:on
    val loadedDf = sqlContext.read
      .format("com.snowflakedb.spark.snowflakedb")
      .options(connectorOptions)
      .option("dbtable", query)
      .load()
    checkAnswer(loadedDf, Seq(Row(2)))
  }

  test("Can load output when 'query' is specified instead of 'dbtable'") {
    // scalastyle:off
    val query =
      s"""
        |select testbyte
        |from $test_table
        |where testfloat = 3.0
      """.stripMargin
    // scalastyle:on
    val loadedDf = sqlContext.read
      .format("com.snowflakedb.spark.snowflakedb")
      .options(connectorOptionsNoTable)
      .option("query", query)
      .load()
    checkAnswer(loadedDf, Seq(Row(2)))
  }

  test("Can load output of Snowflake aggregation queries") {
    val loadedDf = sqlContext.read
      .format("com.snowflakedb.spark.snowflakedb")
      .options(connectorOptionsNoTable)
      .option("query", s"select mod(testbyte, 2) m, count(*) from $test_table group by 1 order by 1")
      .load()
    checkAnswer(loadedDf, Seq(Row(0, 1), Row(1, 2), Row(null, 1)))
  }

  test("DefaultSource supports simple column filtering") {
    checkAnswer(
      sqlContext.sql("select testbyte, testfloat from test_table"),
      Seq(
        Row(1, 1.0),
        Row(2, 3),
        Row(3, -3),
        Row(null, null)))
  }

  test("query with pruned and filtered scans") {
    // scalastyle:off
    checkAnswer(
      sqlContext.sql(
        """
          |select testbyte, testdouble, testint
          |from test_table
          |where testfloat = 3.0
        """.stripMargin),
      Seq(Row(2, 2, 4.0)))
    // scalastyle:on
  }

  test("roundtrip save and load") {
    // This test can be simplified once #98 is fixed.
    val tableName = s"roundtrip_save_and_load_$randomSuffix"
    try {
      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema)
        .write
        .format("com.snowflakedb.spark.snowflakedb")
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = sqlContext.read
        .format("com.snowflakedb.spark.snowflakedb")
        .options(connectorOptions)
        .option("dbtable", tableName)
        .option("tempdir", tempDir)
        .load()
      checkAnswer(loadedDf, TestUtils.expectedData)
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  test("ZD-3234") {
    val loadedDf = sqlContext.read
      .format("com.snowflakedb.spark.snowflakedb")
      .options(connectorOptionsNoTable)
      // scalastyle:off
      .option("query", s"select * from $test_table where contains(teststring, ';') or contains(teststring, 'Unicode');")
      // scalastyle:on
      .load()
    checkAnswer(
      loadedDf.selectExpr("count(*)"),
      Seq(Row(1))
    )
  }

  // TODO Enable more tests
  ignore("roundtrip save and load with uppercase column names") {
    testRoundtripSaveAndLoad(
      s"roundtrip_write_and_read_with_uppercase_column_names_$randomSuffix",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("A", IntegerType) :: Nil)),
      expectedSchemaAfterLoad = Some(StructType(StructField("a", IntegerType) :: Nil)))
  }

  ignore("save with column names that are reserved words") {
    testRoundtripSaveAndLoad(
      s"save_with_column_names_that_are_reserved_words_$randomSuffix",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("table", IntegerType) :: Nil)))
  }

  ignore("save with one empty partition (regression test for #96)") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1)), 2),
      StructType(StructField("foo", IntegerType) :: Nil))
    assert(df.rdd.glom.collect() === Array(Array.empty[Row], Array(Row(1))))
    testRoundtripSaveAndLoad(s"save_with_one_empty_partition_$randomSuffix", df)
  }

  ignore("save with all empty partitions (regression test for #96)") {
    val df = sqlContext.createDataFrame(sc.parallelize(Seq.empty[Row], 2),
      StructType(StructField("foo", IntegerType) :: Nil))
    assert(df.rdd.glom.collect() === Array(Array.empty[Row], Array.empty[Row]))
    testRoundtripSaveAndLoad(s"save_with_all_empty_partitions_$randomSuffix", df)
    // Now try overwriting that table. Although the new table is empty, it should still overwrite
    // the existing table.
    val df2 = df.withColumnRenamed("foo", "bar")
    testRoundtripSaveAndLoad(
      s"save_with_all_empty_partitions_$randomSuffix", df2, saveMode = SaveMode.Overwrite)
  }

  ignore("multiple scans on same table") {
    // .rdd() forces the first query to be unloaded from Snowflake
    val rdd1 = sqlContext.sql("select testint from test_table").rdd
    // Similarly, this also forces an unload:
    val rdd2 = sqlContext.sql("select testdouble from test_table").rdd
    // If the unloads were performed into the same directory then this call would fail: the
    // second unload from rdd2 would have overwritten the integers with doubles, so we'd get
    // a NumberFormatException.
    rdd1.count()
  }

  ignore("configuring maxlength on string columns") {
    val tableName = s"configuring_maxlength_on_string_column_$randomSuffix"
    try {
      val metadata = new MetadataBuilder().putLong("maxlength", 512).build()
      val schema = StructType(
        StructField("x", StringType, metadata = metadata) :: Nil)
      sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 512))), schema).write
        .format("com.snowflakedb.spark.snowflakedb")
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = sqlContext.read
        .format("com.snowflakedb.spark.snowflakedb")
        .options(connectorOptions)
        .option("dbtable", tableName)
        .load()
      checkAnswer(loadedDf, Seq(Row("a" * 512)))
      // This append should fail due to the string being longer than the maxlength
      intercept[SQLException] {
        sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 513))), schema).write
          .format("com.snowflakedb.spark.snowflakedb")
          .options(connectorOptions)
          .option("dbtable", tableName)
          .mode(SaveMode.Append)
          .save()
      }
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  ignore("informative error message when saving a table with string that is longer than max length") {
    val tableName = s"error_message_when_string_too_long_$randomSuffix"
    try {
      val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row("a" * 512))),
        StructType(StructField("A", StringType) :: Nil))
      val e = intercept[SQLException] {
        df.write
          .format("com.snowflakedb.spark.snowflakedb")
          .options(connectorOptions)
          .option("dbtable", tableName)
          .mode(SaveMode.ErrorIfExists)
          .save()
      }
      assert(e.getMessage.contains("while loading data into Snowflake"))
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  ignore("SaveMode.Overwrite with schema-qualified table name (#97)") {
    val tableName = s"overwrite_schema_qualified_table_name$randomSuffix"
    val df = sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
      StructType(StructField("a", IntegerType) :: Nil))
    try {
      // Ensure that the table exists:
      df.write
        .format("com.snowflakedb.spark.snowflakedb")
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, s"PUBLIC.$tableName"))
      // Try overwriting that table while using the schema-qualified table name:
      df.write
        .format("com.snowflakedb.spark.snowflakedb")
        .options(connectorOptions)
        .option("dbtable", s"PUBLIC.$tableName")
        .mode(SaveMode.Overwrite)
        .save()
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  ignore("SaveMode.Overwrite with non-existent table") {
    testRoundtripSaveAndLoad(
      s"overwrite_non_existent_table$randomSuffix",
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil)),
      saveMode = SaveMode.Overwrite)
  }

  ignore("SaveMode.Overwrite with existing table") {
    val tableName = s"overwrite_existing_table$randomSuffix"
    try {
      // Create a table to overwrite
      sqlContext.createDataFrame(sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil))
        .write
        .format("com.snowflakedb.spark.snowflakedb")
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))

      sqlContext.createDataFrame(sc.parallelize(TestUtils.expectedData), TestUtils.testSchema)
        .write
        .format("com.snowflakedb.spark.snowflakedb")
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = sqlContext.read
        .format("com.snowflakedb.spark.snowflakedb")
        .options(connectorOptions)
        .option("dbtable", tableName)
        .load()
      checkAnswer(loadedDf, TestUtils.expectedData)
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }

  // TODO:test overwrite that fails.

  ignore("Append SaveMode doesn't destroy existing data") {
    val extraData = Seq(
      Row(2.toByte, false, null, -1234152.12312498, 100000.0f, null, 1239012341823719L,
        24.toShort, "___|_123", null))

    sqlContext.createDataFrame(sc.parallelize(extraData), TestUtils.testSchema).write
      .format("com.snowflakedb.spark.snowflakedb")
      .options(connectorOptions)
      .option("dbtable", test_table2)
      .mode(SaveMode.Append)
      .saveAsTable(test_table2)

    checkAnswer(
      sqlContext.sql("select * from test_table2"),
      TestUtils.expectedData ++ extraData)
  }

  ignore("Respect SaveMode.ErrorIfExists when table exists") {
    val rdd = sc.parallelize(TestUtils.expectedData)
    val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
    df.registerTempTable(test_table) // to ensure that the table already exists

    // Check that SaveMode.ErrorIfExists throws an exception
    intercept[AnalysisException] {
      df.write
        .format("com.snowflakedb.spark.snowflakedb")
        .options(connectorOptions)
        .option("dbtable", test_table)
        .mode(SaveMode.ErrorIfExists)
        .saveAsTable(test_table)
    }
  }

  ignore("Do nothing when table exists if SaveMode = Ignore") {
    val rdd = sc.parallelize(TestUtils.expectedData.drop(1))
    val df = sqlContext.createDataFrame(rdd, TestUtils.testSchema)
    df.write
      .format("com.snowflakedb.spark.snowflakedb")
      .options(connectorOptions)
      .option("dbtable", test_table)
      .mode(SaveMode.Ignore)
      .saveAsTable(test_table)

    // Check that SaveMode.Ignore does nothing
    checkAnswer(
      sqlContext.sql("select * from test_table"),
      TestUtils.expectedData)
  }
}
