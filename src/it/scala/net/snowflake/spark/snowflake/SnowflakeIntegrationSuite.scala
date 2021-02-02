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

package net.snowflake.spark.snowflake

import java.sql.SQLException

import org.apache.spark.sql.{AnalysisException, Row, SaveMode}
import org.apache.spark.sql.types._
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_SHORT_NAME

/**
  * End-to-end tests which run against a real Snowflake cluster.
  */
class SnowflakeIntegrationSuite extends IntegrationSuiteBase {

  // test_table_tmp should be used inside of one test function.
  private val test_table_tmp: String = s"test_table_tmp_$randomSuffix"
  private val test_table: String = s"test_table_$randomSuffix"
  private val test_table2: String = s"test_table2_$randomSuffix"
  private val test_table3: String = s"test_table3_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()

    conn.createStatement.executeUpdate("drop table if exists test_table")
    conn.createStatement.executeUpdate("drop table if exists test_table2")
    conn.commit()

    def createTable(tableName: String): Unit = {
      jdbcUpdate(s"""
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
      """.stripMargin)
      // scalastyle:off
      jdbcUpdate(s"""
           |insert into $tableName values
           |(1, '2015-07-01', 1234567890123.45, 1234152.12312498, 1.0, 42,
           |  1239012341823719, 23, 'Unicode''s樂趣', '2015-07-01 00:00:00.001'),
           |(2, '1960-01-02', 1.01, 2, 3, 4, 5, 6, '"', '2015-07-02 12:34:56.789'),
           |(3, '2999-12-31', -1.01, -2, -3, -4, -5, -6, '\\\\''"|', '1950-12-31 17:00:00.001'),
           |(null, null, null, null, null, null, null, null, null, null)
         """.stripMargin)
      // scalastyle:on

      conn.commit()
    }

    createTable(test_table)
    createTable(test_table2)

    jdbcUpdate(s"""
         |create table $test_table3 (
         |   "testint1" int,
         |   testint2 int,
         |   "!#TES#STRI?NG" string
         )
      """.stripMargin)

    jdbcUpdate(s"""
         |insert into $test_table3 values
         |(1, 42, 'Unicode'),
         |(2, 3, 'Mario'),
         |(3, 42, 'Luigi')
         """.stripMargin)

    conn.commit()
  }

  override def afterAll(): Unit = {
    try {
      conn.createStatement.executeUpdate(s"drop table if exists $test_table")
      conn.createStatement.executeUpdate(s"drop table if exists $test_table2")
      conn.createStatement.executeUpdate(s"drop table if exists $test_table3")
      conn.createStatement.executeUpdate(s"drop table if exists $test_table_tmp")
      conn.commit()
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    runSql(s"""
         | create or replace temporary view test_table(
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
         | using $SNOWFLAKE_SOURCE_NAME
         | options(
         |   $connectorOptionsString
         |   , dbtable \"$test_table\"
         | )
       """.stripMargin)

    runSql(s"""
         | create or replace temporary view test_table2(
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
         | using $SNOWFLAKE_SOURCE_NAME
         | options(
         |   $connectorOptionsString
         |   , dbtable \"$test_table2\"
         | )
       """.stripMargin)
  }

  test("Quoted column names work") {
    val df = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", s"$test_table3")
      .load()

    checkAnswer(
      df.select("\"testint1\"", "\"!#TES#STRI?NG\"", "testint2"),
      Seq(Row(1, "Unicode", 42), Row(2, "Mario", 3), Row(3, "Luigi", 42))
    )
  }

  test("DefaultSource can load Snowflake COPY unload output to a DataFrame") {
    checkAnswer(
      sparkSession.sql("select * from test_table"),
      TestUtils.expectedData
    )
  }

  test("count() on DataFrame created from a Snowflake table") {
    checkAnswer(
      sparkSession.sql("select count(*) from test_table"),
      Seq(Row(TestUtils.expectedData.length))
    )
  }

  test("count() on DataFrame created from a Snowflake query") {
    val loadedDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      // scalastyle:off
      .option(
        "query",
        s"select * from $test_table where teststring = 'Unicode''s樂趣'"
      )
      // scalastyle:on
      .load()
    checkAnswer(loadedDf.selectExpr("count(*)"), Seq(Row(1)))
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
    val loadedDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
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
    val loadedDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", query)
      .load()
    checkAnswer(loadedDf, Seq(Row(2)))
  }

  test("Can load output of Snowflake aggregation queries") {
    val loadedDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(
        "query",
        s"select mod(testbyte, 2) m, count(*) from $test_table group by 1 order by 1"
      )
      .load()
    checkAnswer(loadedDf, Seq(Row(0, 1), Row(1, 2), Row(null, 1)))
  }

  test("DefaultSource supports simple column filtering") {
    checkAnswer(
      sparkSession.sql("select testbyte, testfloat from test_table"),
      Seq(Row(1, 1.0), Row(2, 3), Row(3, -3), Row(null, null))
    )
  }

  test("query with pruned and filtered scans") {
    // scalastyle:off
    checkAnswer(sparkSession.sql("""
          |select testbyte, testdouble, testint
          |from test_table
          |where testfloat = 3.0
        """.stripMargin), Seq(Row(2, 2, 4.0)))
    // scalastyle:on
  }

  test("roundtrip save and load") {
    // This test can be simplified once #98 is fixed.
    val tableName = s"roundtrip_save_and_load_$randomSuffix"
    try {
      sparkSession
        .createDataFrame(
          sc.parallelize(TestUtils.expectedData),
          TestUtils.testSchema
        )
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()

      assert(DefaultJDBCWrapper.tableExists(params, tableName))

      val loadedDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .load()
      checkAnswer(loadedDf, TestUtils.expectedData)
    } finally {
      conn.createStatement.executeUpdate(s"drop table if exists $tableName")
      conn.commit()
    }
  }

  test("ZD-3234 - query with a semicolon at the end") {
    val loadedDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      // scalastyle:off
      .option(
        "query",
        s"select * from $test_table where contains(teststring, ';') or contains(teststring, 'Unicode');"
      )
      // scalastyle:on
      .load()
    checkAnswer(loadedDf.selectExpr("count(*)"), Seq(Row(1)))
  }

  test("Ability to query a UDF") {
    val funcName = s"testudf$randomSuffix"
    try {
      jdbcUpdate(
        s"""CREATE OR REPLACE FUNCTION $funcName(a number, b number)
           | RETURNS number
           | AS 'a * b'
        """.stripMargin)

      val loadedDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        // scalastyle:off
        .option("query", s"select $funcName(3,4) as twelve")
        // scalastyle:on
        .load()

      checkAnswer(loadedDf.selectExpr("twelve"), Seq(Row(12)))
    } finally {
      // Clean up the UDF
      jdbcUpdate(s"DROP FUNCTION IF EXISTS $funcName(number, number)")
    }
  }

  test("Append SaveMode: 1. create table if not exist, 2. keep existing data if exist") {
    val schema = StructType(
      List(
        StructField("col1", IntegerType),
        StructField("col2", IntegerType)
      )
    )
    val data = Seq(Row(1, 11), Row(2, 22))

    // make sure table: test_table_tmp doesn't exist.
    conn.createStatement.executeUpdate(s"drop table if exists $test_table_tmp")

    // Append data in first time, table doesn't exist yet
    sparkSession
      .createDataFrame(sc.makeRDD(data), schema)
      .write
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_tmp)
      .mode(SaveMode.Append)
      .save()

    val result1 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_tmp)
      .load()

    checkAnswer(result1, data)

    // Append data in second time, the table must exist
    sparkSession
      .createDataFrame(sc.makeRDD(data), schema)
      .write
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_tmp)
      .mode(SaveMode.Append)
      .save()

    val result2 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_tmp)
      .load()

    // Both old data and new data are in the table
    checkAnswer(result2, data ++ data)

    conn.createStatement.executeUpdate(s"drop table if exists $test_table_tmp")
  }

  // TODO Enable more tests
  ignore("roundtrip save and load with uppercase column names") {
    testRoundtripSaveAndLoad(
      s"roundtrip_write_and_read_with_uppercase_column_names_$randomSuffix",
      sparkSession.createDataFrame(
        sc.parallelize(Seq(Row(1))),
        StructType(StructField("A", IntegerType) :: Nil)
      ),
      expectedSchemaAfterLoad =
        Some(StructType(StructField("a", IntegerType) :: Nil))
    )
  }

  ignore("save with column names that are reserved words") {
    testRoundtripSaveAndLoad(
      s"save_with_column_names_that_are_reserved_words_$randomSuffix",
      sparkSession.createDataFrame(
        sc.parallelize(Seq(Row(1))),
        StructType(StructField("table", IntegerType) :: Nil)
      )
    )
  }

  ignore("save with one empty partition (regression test for #96)") {
    val df = sparkSession.createDataFrame(
      sc.parallelize(Seq(Row(1)), 2),
      StructType(StructField("foo", IntegerType) :: Nil)
    )
    assert(df.rdd.glom.collect() === Array(Array.empty[Row], Array(Row(1))))
    testRoundtripSaveAndLoad(s"save_with_one_empty_partition_$randomSuffix", df)
  }

  ignore("save with all empty partitions (regression test for #96)") {
    val df = sparkSession.createDataFrame(
      sc.parallelize(Seq.empty[Row], 2),
      StructType(StructField("foo", IntegerType) :: Nil)
    )
    assert(df.rdd.glom.collect() === Array(Array.empty[Row], Array.empty[Row]))
    testRoundtripSaveAndLoad(
      s"save_with_all_empty_partitions_$randomSuffix",
      df
    )
    // Now try overwriting that table. Although the new table is empty, it should still overwrite
    // the existing table.
    val df2 = df.withColumnRenamed("foo", "bar")
    testRoundtripSaveAndLoad(
      s"save_with_all_empty_partitions_$randomSuffix",
      df2,
      saveMode = SaveMode.Overwrite
    )
  }

  ignore("multiple scans on same table") {
    // .rdd() forces the first query to be unloaded from Snowflake
    val rdd1 = sparkSession.sql("select testint from test_table").rdd
    // Similarly, this also forces an unload:
    sparkSession.sql("select testdouble from test_table").rdd
    // If the unloads were performed into the same directory then this call would fail: the
    // second unload from rdd2 would have overwritten the integers with doubles, so we'd get
    // a NumberFormatException.
    rdd1.count()
  }

  ignore("configuring maxlength on string columns") {
    val tableName = s"configuring_maxlength_on_string_column_$randomSuffix"
    try {
      val metadata = new MetadataBuilder().putLong("maxlength", 512).build()
      val schema =
        StructType(StructField("x", StringType, metadata = metadata) :: Nil)
      sparkSession
        .createDataFrame(sc.parallelize(Seq(Row("a" * 512))), schema)
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(params, tableName))
      val loadedDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .load()
      checkAnswer(loadedDf, Seq(Row("a" * 512)))
      // This append should fail due to the string being longer than the maxlength
      intercept[SQLException] {
        sparkSession
          .createDataFrame(sc.parallelize(Seq(Row("a" * 513))), schema)
          .write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(connectorOptions)
          .option("dbtable", tableName)
          .mode(SaveMode.Append)
          .save()
      }
    } finally {
      conn.createStatement.executeUpdate(s"drop table if exists $tableName")
      conn.commit()
    }
  }

  ignore(
    "informative error message when saving a table with string that is longer than max length"
  ) {
    val tableName = s"error_message_when_string_too_long_$randomSuffix"
    try {
      val df = sparkSession.createDataFrame(
        sc.parallelize(Seq(Row("a" * 512))),
        StructType(StructField("A", StringType) :: Nil)
      )
      val e = intercept[SQLException] {
        df.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(connectorOptions)
          .option("dbtable", tableName)
          .mode(SaveMode.ErrorIfExists)
          .save()
      }
      assert(e.getMessage.contains("while loading data into Snowflake"))
    } finally {
      conn.createStatement.executeUpdate(s"drop table if exists $tableName")
      conn.commit()
    }
  }

  ignore("SaveMode.Overwrite with schema-qualified table name (#97)") {
    val tableName = s"overwrite_schema_qualified_table_name$randomSuffix"
    val df = sparkSession.createDataFrame(
      sc.parallelize(Seq(Row(1))),
      StructType(StructField("a", IntegerType) :: Nil)
    )
    try {
      // Ensure that the table exists:
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(params, s"PUBLIC.$tableName"))
      // Try overwriting that table while using the schema-qualified table name:
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", s"PUBLIC.$tableName")
        .mode(SaveMode.Overwrite)
        .save()
    } finally {
      conn.createStatement.executeUpdate(s"drop table if exists $tableName")
      conn.commit()
    }
  }

  ignore("SaveMode.Overwrite with non-existent table") {
    testRoundtripSaveAndLoad(
      s"overwrite_non_existent_table$randomSuffix",
      sparkSession.createDataFrame(
        sc.parallelize(Seq(Row(1))),
        StructType(StructField("a", IntegerType) :: Nil)
      ),
      saveMode = SaveMode.Overwrite
    )
  }

  ignore("SaveMode.Overwrite with existing table") {
    val tableName = s"overwrite_existing_table$randomSuffix"
    try {
      // Create a table to overwrite
      sparkSession
        .createDataFrame(
          sc.parallelize(Seq(Row(1))),
          StructType(StructField("a", IntegerType) :: Nil)
        )
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(SaveMode.ErrorIfExists)
        .save()
      assert(DefaultJDBCWrapper.tableExists(params, tableName))

      sparkSession
        .createDataFrame(
          sc.parallelize(TestUtils.expectedData),
          TestUtils.testSchema
        )
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(SaveMode.Overwrite)
        .save()

      assert(DefaultJDBCWrapper.tableExists(params, tableName))
      val loadedDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .load()
      checkAnswer(loadedDf, TestUtils.expectedData)
    } finally {
      conn.createStatement.executeUpdate(s"drop table if exists $tableName")
      conn.commit()
    }
  }

  // TODO:test overwrite that fails.

  ignore("Append SaveMode doesn't destroy existing data") {
    val extraData = Seq(
      Row(
        2.toByte,
        false,
        null,
        -1234152.12312498,
        100000.0f,
        null,
        1239012341823719L,
        24.toShort,
        "___|_123",
        null
      )
    )

    sparkSession
      .createDataFrame(sc.parallelize(extraData), TestUtils.testSchema)
      .write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptions)
      .option("dbtable", test_table2)
      .mode(SaveMode.Append)
      .saveAsTable(test_table2)

    checkAnswer(
      sparkSession.sql("select * from test_table2"),
      TestUtils.expectedData ++ extraData
    )
  }

  ignore("Respect SaveMode.ErrorIfExists when table exists") {
    val rdd = sc.parallelize(TestUtils.expectedData)
    val df = sparkSession.createDataFrame(rdd, TestUtils.testSchema)
    df.createOrReplaceTempView(test_table) // to ensure that the table already exists

    // Check that SaveMode.ErrorIfExists throws an exception
    intercept[AnalysisException] {
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", test_table)
        .mode(SaveMode.ErrorIfExists)
        .saveAsTable(test_table)
    }
  }

  ignore("Do nothing when table exists if SaveMode = Ignore") {
    val rdd = sc.parallelize(TestUtils.expectedData.drop(1))
    val df = sparkSession.createDataFrame(rdd, TestUtils.testSchema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptions)
      .option("dbtable", test_table)
      .mode(SaveMode.Ignore)
      .saveAsTable(test_table)

    // Check that SaveMode.Ignore does nothing
    checkAnswer(
      sparkSession.sql("select * from test_table"),
      TestUtils.expectedData
    )
  }
}
