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

import java.sql.{Date, Timestamp}
import java.util.TimeZone

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.{TestHook, TestHookFlag}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.Random

// scalastyle:off println
class SnowflakeResultSetRDDSuite extends IntegrationSuiteBase {
  // Add some options for default for testing.
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()

  private val test_table_number: String = s"test_table_number_$randomSuffix"
  private val test_table_string_binary: String =
    s"test_table_string_binary_$randomSuffix"
  private val test_table_date_time: String =
    s"test_table_date_time_$randomSuffix"
  private val test_table_timestamp: String =
    s"test_table_timestamp_$randomSuffix"
  private val test_table_large_result: String =
    s"test_table_large_result_$randomSuffix"
  private val test_table_inf: String = s"test_table_inf_$randomSuffix"
  private val test_table_write: String = s"test_table_write_$randomSuffix"
  private val test_table_like: String = s"test_table_like_$randomSuffix"

  private lazy val test_table_number_rows = Seq(
    Row(
      BigDecimal(1),
      true,
      BigDecimal(100),
      BigDecimal(1.2),
      BigDecimal(10.2),
      BigDecimal(300),
      BigDecimal(30.2),
      BigDecimal(1.002),
      BigDecimal(1234567890),
      BigDecimal(1.01234),
      BigDecimal(1.01234567),
      BigDecimal("123456789012345"),
      BigDecimal("123456789.01234"),
      BigDecimal("0.012345678901234"),
      BigDecimal("1234567890123456789012345"), // pragma: allowlist secret
      BigDecimal("12345678901234567890.01234"),
      BigDecimal("1234567890.012345678901234"),
      1.1,
      1234567890.0123,
      2.2,
      0.123456789
    ),
    Row(
      2,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    ),
    Row(
      BigDecimal(3),
      false,
      BigDecimal(-100),
      BigDecimal(-1.2),
      BigDecimal(-10.2),
      BigDecimal(-300),
      BigDecimal(-30.2),
      BigDecimal(-1.002),
      BigDecimal(-1234567890),
      BigDecimal(-1.01234),
      BigDecimal(-1.01234567),
      BigDecimal("-123456789012345"),
      BigDecimal("-123456789.01234"),
      BigDecimal("-0.012345678901234"),
      BigDecimal("-1234567890123456789012345"),
      BigDecimal("-12345678901234567890.01234"),
      BigDecimal("-1234567890.012345678901234"),
      -1.1,
      -1234567890.0123,
      -2.2,
      -0.123456789
    )
  )

  lazy val setupNumberTable: Boolean = {
    jdbcUpdate(s"""create or replace table $test_table_number (
                     | int_c int, boolean_c boolean
                     | , num_1b1 number(3), num_1b2 number(2,1), num_1b3 number(3,1)
                     | , num_2b1 number(10), num_2b2 number(10,1), num_2b3 number(9,3)
                     | , num_4b1 number(20), num_4b2 number(20,5), num_4b3 number(15,8)
                     | , num_8b1 number(30), num_8b2 number(30,5), num_8b3 number(30,15)
                     | , num_16b1 number(37), num_16b2 number(38,5), num_16b3 number(36,15)
                     | , float4_c1 float4, float4_c2 float4, float8_c1 float8, float8_c2 float8
                     | )
                     | """.stripMargin)

    jdbcUpdate(s"""insert into $test_table_number values (
      1, true
      , 100, 1.2, 10.2
      , 300, 30.2, 1.002
      , 1234567890, 1.01234, 1.01234567
      , 123456789012345, 123456789.01234, 0.012345678901234
      , 1234567890123456789012345, 12345678901234567890.01234, 1234567890.012345678901234
      , 1.1, 1234567890.0123, 2.2,  0.123456789
    ), (
      2, null
      , null , null, null
      , null , null, null
      , null , null, null
      , null , null, null
      , null , null, null
      , null , null, null, null
    ), (
      3, false
      , -100, -1.2, -10.2
      , -300, -30.2, -1.002
      , -1234567890, -1.01234, -1.01234567
      , -123456789012345, -123456789.01234, -0.012345678901234
      , -1234567890123456789012345, -12345678901234567890.01234, -1234567890.012345678901234
      , -1.1, -1234567890.0123, -2.2,  -0.123456789
    )""".stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_number")
      .load()

    tmpdf.createOrReplaceTempView("test_table_number")
    true
  }

  private lazy val test_table_string_binary_rows = Seq(
    Row(
      BigDecimal(1),
      "varchar",
      "v50",
      "c",
      "c10",
      "s",
      "s20",
      "t",
      "t30",
      "binary".getBytes(),
      "binary100".getBytes(),
      "varbinary".getBytes()
    ),
    Row(
      BigDecimal(2),
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null,
      null
    ),
    Row(
      BigDecimal(3),
      "varchar",
      "v50",
      "c",
      "c10",
      "s",
      "s20",
      "t",
      "t30",
      "binary".getBytes(),
      "binary100".getBytes(),
      "varbinary".getBytes()
    )
  )

  lazy val setupStringBinaryTable: Boolean = {
    jdbcUpdate(s"""create or replace table $test_table_string_binary (
                  | int_c int,
                  | v varchar, v50 varchar(50), c char, c10 char(10),
                  | s string, s20 string(20), t text, t30 text(30),
                  | b binary, b100 binary(100), vb varbinary )
                  | """.stripMargin)

    jdbcUpdate(s"""insert into $test_table_string_binary select
        | 1, 'varchar', 'v50', 'c', 'c10', 's', 's20', 't', 't30',
        | hex_encode('binary'), hex_encode('binary100'), hex_encode('varbinary')
        |""".stripMargin)

    jdbcUpdate(s"""insert into $test_table_string_binary select
                  | 2, null , null, null , null, null , null, null , null,
                  | null , null, null
                  |""".stripMargin)

    jdbcUpdate(s"""insert into $test_table_string_binary select
                  | 3, 'varchar', 'v50', 'c', 'c10', 's', 's20', 't', 't30',
                  | hex_encode('binary'), hex_encode('binary100'), hex_encode('varbinary')
                  |""".stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_string_binary")
      .load()

    tmpdf.createOrReplaceTempView("test_table_string_binary")
    true
  }

  // Snowflake may output "23:59:59" as "23:59:59.",
  // If that is changed in the future, the expected result needs to be updated.
  private lazy val test_table_date_time_rows = Seq(
    Row(
      BigDecimal(0),
      Date.valueOf("9999-12-31"),
      "23:59:59.",
      "23:59:59.9",
      "23:59:59.99",
      "23:59:59.999",
      "23:59:59.9999",
      "23:59:59.99999",
      "23:59:59.999999",
      "23:59:59.9999999",
      "23:59:59.99999999",
      "23:59:59.999999999"
    ),
    Row(
      BigDecimal(1),
      Date.valueOf("1970-01-01"),
      "00:00:00.",
      "00:00:00.0",
      "00:00:00.00",
      "00:00:00.000",
      "00:00:00.0000",
      "00:00:00.00000",
      "00:00:00.000000",
      "00:00:00.0000000",
      "00:00:00.00000000",
      "00:00:00.000000000"
    ),
    Row(
      BigDecimal(2),
      Date.valueOf("0001-01-01"),
      "00:00:01.",
      "00:00:00.1",
      "00:00:00.01",
      "00:00:00.001",
      "00:00:00.0001",
      "00:00:00.00001",
      "00:00:00.000001",
      "00:00:00.0000001",
      "00:00:00.00000001",
      "00:00:00.000000001"
    )
  )

  lazy val setupDateTimeTable: Boolean = {
    jdbcUpdate(s"""create or replace table $test_table_date_time (
          | int_c int, date_c date, time_c0 time(0), time_c1 time(1), time_c2 time(2),
          | time_c3 time(3), time_c4 time(4), time_c5 time(5), time_c6 time(6),
          | time_c7 time(7), time_c8 time(8), time_c9 time(9)
          )""".stripMargin)

    jdbcUpdate(s"""insert into $test_table_date_time values (
              | 0, '9999-12-31', '23:59:59', '23:59:59.9', '23:59:59.99'
              | , '23:59:59.999', '23:59:59.9999', '23:59:59.99999', '23:59:59.999999'
              | , '23:59:59.9999999', '23:59:59.99999999', '23:59:59.999999999'
           ), (
              | 1, '1970-01-01', '00:00:00', '00:00:00.00', '00:00:00.00',
              | '00:00:00.000', '00:00:00.0000', '00:00:00.00000', '00:00:00.000000'
              | , '00:00:00.0000000', '00:00:00.00000000', '00:00:00.000000000'
           ), (
              | 2, '0001-01-01', '00:00:01', '00:00:00.1', '00:00:00.01', '00:00:00.001'
              | , '00:00:00.0001', '00:00:00.00001', '00:00:00.000001'
              | , '00:00:00.0000001', '00:00:00.00000001', '00:00:00.000000001'
           )""".stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_date_time")
      .load()

    tmpdf.createOrReplaceTempView("test_table_date_time")
    true
  }

  private lazy val test_table_timestamp_rows = Seq(
    Row(
      BigDecimal(1),
      Timestamp.valueOf("2014-01-11 06:12:13.123456"),
      Timestamp.valueOf("2014-01-11 06:12:13"),
      Timestamp.valueOf("2014-01-11 06:12:13.123"),
      Timestamp.valueOf("2014-01-11 06:12:13.123456"),
      Timestamp.valueOf("2014-01-11 06:12:13.123456"),
      Timestamp.valueOf("2014-01-11 06:12:13"),
      Timestamp.valueOf("2014-01-11 06:12:13.123"),
      Timestamp.valueOf("2014-01-11 06:12:13.123456"),
      Timestamp.valueOf("2014-01-11 06:12:13.123456"),
      Timestamp.valueOf("2014-01-11 06:12:13"),
      Timestamp.valueOf("2014-01-11 06:12:13.123"),
      Timestamp.valueOf("2014-01-11 06:12:13.123456")
    ),
    Row(
      BigDecimal(2),
      Timestamp.valueOf("2014-01-11 06:12:13.123456"),
      Timestamp.valueOf("2014-01-11 06:12:13"),
      Timestamp.valueOf("2014-01-11 06:12:13.123"),
      Timestamp.valueOf("2014-01-11 06:12:13.123456"),
      Timestamp.valueOf("2014-01-11 06:12:13.123456"),
      Timestamp.valueOf("2014-01-11 06:12:13"),
      Timestamp.valueOf("2014-01-11 06:12:13.123"),
      Timestamp.valueOf("2014-01-11 06:12:13.123456"),
      Timestamp.valueOf("2014-01-11 06:12:13.123456"),
      Timestamp.valueOf("2014-01-11 06:12:13"),
      Timestamp.valueOf("2014-01-11 06:12:13.123"),
      Timestamp.valueOf("2014-01-11 06:12:13.123456")
    )
  )

  lazy val setupTimestampTable: Boolean = {
    jdbcUpdate(s"""create or replace table $test_table_timestamp (
                  | int_c int,
                  | ts_ltz_c timestamp_ltz(9), ts_ltz_c0 timestamp_ltz(0),
                  | ts_ltz_c3 timestamp_ltz(3), ts_ltz_c6 timestamp_ltz(6),
                  |
                  | ts_ntz_c timestamp_ntz(9), ts_ntz_c0 timestamp_ntz(0),
                  | ts_ntz_c3 timestamp_ntz(3), ts_ntz_c6 timestamp_ntz(6),
                  |
                  | ts_tz_c timestamp_tz(9), ts_tz_c0 timestamp_tz(0),
                  | ts_tz_c3 timestamp_tz(3), ts_tz_c6 timestamp_tz(6)
                  | )""".stripMargin)

    jdbcUpdate(s"""insert into $test_table_timestamp values (
                  | 1,
                  | '2014-01-11 06:12:13.123456', '2014-01-11 06:12:13',
                  | '2014-01-11 06:12:13.123', '2014-01-11 06:12:13.123456',
                  |
                  | '2014-01-11 06:12:13.123456', '2014-01-11 06:12:13',
                  | '2014-01-11 06:12:13.123', '2014-01-11 06:12:13.123456',
                  |
                  | '2014-01-11 06:12:13.123456', '2014-01-11 06:12:13',
                  | '2014-01-11 06:12:13.123', '2014-01-11 06:12:13.123456'
                  | ),(
                  | 2,
                  | '2014-01-11 06:12:13.123456', '2014-01-11 06:12:13',
                  | '2014-01-11 06:12:13.123', '2014-01-11 06:12:13.123456',
                  |
                  | '2014-01-11 06:12:13.123456', '2014-01-11 06:12:13',
                  | '2014-01-11 06:12:13.123', '2014-01-11 06:12:13.123456',
                  |
                  | '2014-01-11 06:12:13.123456', '2014-01-11 06:12:13',
                  | '2014-01-11 06:12:13.123', '2014-01-11 06:12:13.123456'
                  | )""".stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_timestamp")
      .load()

    tmpdf.createOrReplaceTempView("test_table_timestamp")
    true
  }

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
                  | row_number() over (order by seq4()) - 1, '$largeStringValue'
                  | from table(generator(rowcount => $LARGE_TABLE_ROW_COUNT))""".stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_large_result")
      .load()

    tmpdf.createOrReplaceTempView("test_table_large_result")
    true
  }

  private lazy val test_table_inf_rows = Seq(
    Row(
      Double.PositiveInfinity,
      Double.NegativeInfinity,
      Double.PositiveInfinity,
      Double.NegativeInfinity,
      Double.NaN
    )
  )

  lazy val setupINFTable: Boolean = {
    jdbcUpdate(
      s"""create or replace table $test_table_inf (
         |c1 double, c2 double, c3 float, c4 float, c5 double)""".stripMargin)
    jdbcUpdate(
      s"""insert into $test_table_inf values (
         |'inf', '-INF', 'inf', '-inf', 'NaN')""".stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_inf")
      .load()

    tmpdf.createOrReplaceTempView("test_table_inf")
    true
  }

  lazy val setupTableForLike: Boolean = {
    // Below test table is from Snowflake user Doc.
    // https://docs.snowflake.net/manuals/sql-reference/functions/like.html#examples
    jdbcUpdate(
      s"""create or replace table $test_table_like (
         |subject string)""".stripMargin)
    jdbcUpdate(
      s"""insert into $test_table_like values
         | ('John  Dddoe'), ('Joe   Doe'), ('John_down'),
         | ('Joe down'), ('Elaine'), (''), (null),
         | ('100 times'), ('1000 times'), ('100%')
         |""".stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_like")
      .load()

    tmpdf.createOrReplaceTempView("test_table_like")
    true
  }

  def writeAndCheckForOneTable(sparkSession: SparkSession,
                               sfOptions: Map[String, String],
                               sourceTable: String,
                               extraSelectClause: String,
                               targetTable: String,
                               createTargetTable: Option[String],
                               tracePrint: Boolean): Boolean = {
    // Read data from source table
    val sourceQuery = s"select * from $sourceTable $extraSelectClause"
    val sourceDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query", sourceQuery)
      .load()

    if (tracePrint) {
      println(s"Source Query is: $sourceQuery")
      sourceDF.printSchema()
      sourceDF.show(100, false)
    }

    // Write data to snowflake
    jdbcUpdate(s"drop table if exists $targetTable")
    if (createTargetTable.isDefined) {
      jdbcUpdate(createTargetTable.get)
      sourceDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptions)
        .option("dbtable", targetTable)
        // .option("truncate_table", "off")
        // .option("usestagingtable", "on")
        .mode(SaveMode.Append)
        .save()
    } else {
      sourceDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptions)
        .option("dbtable", targetTable)
        // .option("truncate_table", "off")
        // .option("usestagingtable", "on")
        .mode(SaveMode.Overwrite)
        .save()
    }

    // Show the written table data.
    if (tracePrint) {
      val targetDF = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(sfOptions)
        .option("dbtable", s"$targetTable")
        .load()

      targetDF.printSchema()
      targetDF.show(100, false)
    }

    // Verify the result set to be identical by HASH_AGG()
    val sourceHashAgg = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query", s"select HASH_AGG(*) from $sourceTable $extraSelectClause")
      .load()
      .collect()(0)
    val targetHashAgg = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query", s"select HASH_AGG(*) from $targetTable")
      .load()
      .collect()(0)

    if (tracePrint) {
      println(s"Source: $sourceTable Target: $targetTable")
      println(s"The content of source and target table are:" +
        s"\n$sourceHashAgg\n$targetHashAgg")
    }

    assert(sourceHashAgg.equals(targetHashAgg),
      s"The content of source and target table are not identical:" +
        s"\n$sourceHashAgg\n$targetHashAgg")

    true
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
    thisConnectorOptionsNoTable += ("jdbc_query_result_format" -> "arrow")
  }

  test("testNumber") {
    setupNumberTable
    val result = sparkSession.sql("select * from test_table_number")

    checkAnswer(
      result,
      test_table_number_rows
    )
  }

  test("testStringBinary") {
    setupStringBinaryTable
    // COPY UNLOAD can't be run because it doesn't support binary
    if (!params.useCopyUnload) {
      val result = sparkSession.sql("select * from test_table_string_binary")

      checkAnswer(
        result,
        test_table_string_binary_rows
      )
    }
  }

  // Negative test for read fail.
  // The query tries to convert a String to number, but the value is not number
  // So the reading data frame hits runtime error.
  test("testFailToRead") {
    setupStringBinaryTable

    assertThrows[Exception]({
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", s"select to_number(v) from $test_table_string_binary")
        .load()
        .collect()
    })
  }

  test("test Write table with AUTOINCREMENT column") {
    // Create a table with AUTOINCREMENT column
    jdbcUpdate(
      s"""create or replace table $test_table_write (
         | int_c int, c_string string(1024), id_c int AUTOINCREMENT (100, 10))
         | """.stripMargin)

    // Write 5 rows without data for AUTOINCREMENT column
    val df1 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("query", "select seq4() as INT_C, 'test123' as C_STRING from" +
        " table(generator(rowcount => 5))")
      .load()
    df1.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .option("columnmap", "Map(INT_C -> INT_C, C_STRING -> C_STRING)")
      .mode(SaveMode.Append)
      .save()

    // Write 5 rows with data for AUTOINCREMENT column
    val df2 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("query", "select seq4(), 'test456', 1234 from table(generator(rowcount => 5))")
      .load()
    df2.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()

    // Verify result for AUTOINCREMENT column
    val expectedAnswer = Array(
      Row(100), Row(110), Row(120), Row(130), Row(140),
      Row(1234), Row(1234), Row(1234), Row(1234), Row(1234)
    )
    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("query", s"select ID_C from $test_table_write order by ID_C")
      .load()

    checkAnswer(result, expectedAnswer)
  }

  test("test read write StringBinary") {
    setupStringBinaryTable
    // COPY UNLOAD can't be run because it doesn't support binary
    if (!params.useCopyUnload) {
      writeAndCheckForOneTable(sparkSession, thisConnectorOptionsNoTable,
        test_table_string_binary, "", test_table_write, None, true)
    }
  }

  test("testDateTime") {
    setupDateTimeTable
    val result = sparkSession.sql("select * from test_table_date_time")

    checkAnswer(
      result,
      test_table_date_time_rows
    )
  }

  test("test read write Date Time") {
    setupDateTimeTable
    val createTableSql =
      s"""create or replace table $test_table_write (
         | int_c int, date_c date, time_c0 time(0), time_c1 time(1), time_c2 time(2),
         | time_c3 time(3), time_c4 time(4), time_c5 time(5), time_c6 time(6),
         | time_c7 time(7), time_c8 time(8), time_c9 time(9)
          )""".stripMargin
    writeAndCheckForOneTable(sparkSession, thisConnectorOptionsNoTable,
      test_table_date_time, "", test_table_write, Some(createTableSql), true)
  }

  test("testTimestamp") {
    setupTimestampTable
    // COPY UNLOAD can't be run because it only supports millisecond(0.001s).
    if (!params.useCopyUnload) {
      val result = sparkSession.sql("select * from test_table_timestamp")

      checkAnswer(
        result,
        test_table_timestamp_rows
      )
    }
  }

  // Most simple case for timestamp write
  test("testTimestamp write") {
    setupTimestampTable
    // COPY UNLOAD can't be run because it only supports millisecond(0.001s).
    if (!params.useCopyUnload) {
      val createTableSql =
        s"""create or replace table $test_table_write (
           | int_c int,
           | ts_ltz_c timestamp_ltz(9), ts_ltz_c0 timestamp_ltz(0),
           | ts_ltz_c3 timestamp_ltz(3), ts_ltz_c6 timestamp_ltz(6),
           |
           | ts_ntz_c timestamp_ntz(9), ts_ntz_c0 timestamp_ntz(0),
           | ts_ntz_c3 timestamp_ntz(3), ts_ntz_c6 timestamp_ntz(6),
           |
           | ts_tz_c timestamp_tz(9), ts_tz_c0 timestamp_tz(0),
           | ts_tz_c3 timestamp_tz(3), ts_tz_c6 timestamp_tz(6)
           | )""".stripMargin
      writeAndCheckForOneTable(sparkSession, thisConnectorOptionsNoTable,
        test_table_timestamp, "", test_table_write, Some(createTableSql), true)
    }
  }

  // test timestamp write with timezone
  test("testTimestamp write with timezone") {
    setupTimestampTable
    // COPY UNLOAD can't be run because it only supports millisecond(0.001s).
    if (!params.useCopyUnload) {
      var oldValue: Option[String] = None
      if (thisConnectorOptionsNoTable.contains("sftimezone")) {
        oldValue = Some(thisConnectorOptionsNoTable("sftimezone"))
        thisConnectorOptionsNoTable -= "sftimezone"
      }
      val oldTimezone = TimeZone.getDefault

      val createTableSql =
        s"""create or replace table $test_table_write (
           | int_c int,
           | ts_ltz_c timestamp_ltz(9), ts_ltz_c0 timestamp_ltz(0),
           | ts_ltz_c3 timestamp_ltz(3), ts_ltz_c6 timestamp_ltz(6),
           |
           | ts_ntz_c timestamp_ntz(9), ts_ntz_c0 timestamp_ntz(0),
           | ts_ntz_c3 timestamp_ntz(3), ts_ntz_c6 timestamp_ntz(6),
           |
           | ts_tz_c timestamp_tz(9), ts_tz_c0 timestamp_tz(0),
           | ts_tz_c3 timestamp_tz(3), ts_tz_c6 timestamp_tz(6)
           | )""".stripMargin

      // Test conditions with (sfTimezone, sparkTimezone)
      val testConditions: List[(String, String)] = List(
          (null, "GMT")
        , (null, "America/Los_Angeles")
        , ("America/New_York", "America/Los_Angeles")
      )

      for ((sfTimezone, sparkTimezone) <- testConditions) {
        // set spark timezone
        val thisSparkSession = if (sparkTimezone != null) {
          TimeZone.setDefault(TimeZone.getTimeZone(sparkTimezone))
          SparkSession.builder
            .master("local")
            .appName("SnowflakeSourceSuite")
            .config("spark.sql.shuffle.partitions", "6")
            .config("spark.driver.extraJavaOptions", s"-Duser.timezone=$sparkTimezone")
            .config("spark.executor.extraJavaOptions", s"-Duser.timezone=$sparkTimezone")
            .config("spark.sql.session.timeZone", sparkTimezone)
            .getOrCreate()
        } else {
          sparkSession
        }

        // Set timezone option
        if (sfTimezone != null) {
          if (thisConnectorOptionsNoTable.contains("sftimezone")) {
            thisConnectorOptionsNoTable -= "sftimezone"
          }
          thisConnectorOptionsNoTable += ("sftimezone" -> sfTimezone)
        } else {
          if (thisConnectorOptionsNoTable.contains("sftimezone")) {
            thisConnectorOptionsNoTable -= "sftimezone"
          }
        }

        writeAndCheckForOneTable(thisSparkSession, thisConnectorOptionsNoTable,
          test_table_timestamp, "", test_table_write, Some(createTableSql), true)
      }

      // restore options for further test
      thisConnectorOptionsNoTable -= "sftimezone"
      if (oldValue.isDefined) {
        thisConnectorOptionsNoTable += ("sftimezone" -> oldValue.get)
      }
      TimeZone.setDefault(oldTimezone)
    }
  }

  test("testLargeResult") {
    setupLargeResultTable
    if (!skipBigDataTest) {
      val tmpDF = sparkSession
        .sql("select * from test_table_large_result order by int_c")
        // The call of cache() is to confirm a data loss issue is fixed.
        // SNOW-123999
        .cache()

      val resultSet: Array[Row] = tmpDF.collect()

      var i: Int = 0
      while (i < resultSet.length) {
        val row = resultSet(i)
        assert(largeStringValue.equals(row(1)))
        // For Arrow format, the result is ordered.
        // For COPY UNLOAD, the order can't be guaranteed
        if (!params.useCopyUnload) {
          assert(Math.abs(
            BigDecimal(i).doubleValue -
              row(0).asInstanceOf[java.math.BigDecimal].doubleValue()
          ) < 0.00000000001)
        }
        i += 1
      }
    }
  }

  test("test COPY missing files and succeed") {
    setupLargeResultTable
    if (!skipBigDataTest) {
      val tmpDF = sparkSession
        .sql("select * from test_table_large_result order by int_c")

      // Enable the test flag
      TestHook.enableTestFlagOnly(
        TestHookFlag.TH_COPY_INTO_TABLE_MISS_FILES_SUCCESS)

      jdbcUpdate(s"drop table if exists $test_table_write")

      // Write the data to snowflake
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_write)
        .mode(SaveMode.Overwrite)
        .save()

      // Disable the test flag
      TestHook.disableTestHook()

      // Verify row count is correct.
      val rowCount = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", s"$test_table_write")
        .load()
        .count()
      assert(rowCount == LARGE_TABLE_ROW_COUNT)
    }
  }

  // Negative Test
  // disabled since Spark 3.5, since the test DF has only one partation
  ignore("test COPY missing files and fails") {
    setupLargeResultTable
    // Don't run this test for use_copy_unload=true
    // because there are only 3 files (2 partitions) for this data size.
    if (!skipBigDataTest && !params.useCopyUnload) {
      val tmpDF = sparkSession
        .sql("select * from test_table_large_result order by int_c")

      // Enable 2 test flags.
      TestHook.enableTestFlagOnly(
        TestHookFlag.TH_COPY_INTO_TABLE_MISS_FILES_SUCCESS)
      TestHook.enableTestFlag(
        TestHookFlag.TH_COPY_INTO_TABLE_MISS_FILES_FAIL)

      jdbcUpdate(s"drop table if exists $test_table_write")

      try {
        // Some files are missed, so the spark job fails.
        assertThrows[SnowflakeConnectorException] {
          tmpDF.write
            .format(SNOWFLAKE_SOURCE_NAME)
            .options(thisConnectorOptionsNoTable)
            .option("dbtable", test_table_write)
            .mode(SaveMode.Overwrite)
            .save()
        }
      } finally {
        // Disable the test flags
        TestHook.disableTestHook()
      }
    }
  }

  test("testSparkScalaUDF") {
    setupLargeResultTable
    if (!skipBigDataTest) {
      val squareUDF = (s: Int) => {
        s * s
      }
      val funcName = s"UDFSquare$randomSuffix"
      sparkSession.udf.register(funcName, squareUDF)

      val resultSet: Array[Row] = sparkSession
        .sql(s"select int_c, $funcName(int_c), c_string" +
          s" from test_table_large_result where int_c < 100 order by int_c")
        .collect()

      var i: Int = 0
      while (i < resultSet.length) {
        val row = resultSet(i)
        assert(largeStringValue.equals(row(2)))
        // For Arrow format, the result is ordered.
        // For COPY UNLOAD, the order can't be guaranteed
        if (!params.useCopyUnload) {
          assert(Math.abs(
            BigDecimal(i).doubleValue -
              row(0).asInstanceOf[java.math.BigDecimal].doubleValue()
          ) < 0.00000000001)
        }
        i += 1
      }
    }
  }

  test("testDoubleINF") {
    setupINFTable
    val tmpDF = sparkSession
      .sql(s"select * from test_table_inf")

    assert(tmpDF.schema.fields(0).dataType.equals(DoubleType))

    var resultSet: Array[Row] = tmpDF.collect()
    assert(resultSet.length == 1)
    assert(resultSet(0).equals(test_table_inf_rows.head))

    // Write the INF back to snowflake
    jdbcUpdate(s"drop table if exists $test_table_write")
    tmpDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .option("truncate_table", "off")
      .option("usestagingtable", "on")
      .mode(SaveMode.Overwrite)
      .save()

    // Read back the written data.
    val readBackDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_write")
      .load()

    assert(readBackDF.schema.fields(0).dataType.equals(DoubleType))

    resultSet = readBackDF.collect()
    assert(resultSet.length == 1)
    assert(resultSet(0).equals(test_table_inf_rows(0)))
  }

  // For USE_COPY_UNLOAD=TRUE, write an empty result doesn't really create the table
  // use separate Jira to fix it.
  test("testReadWriteEmptyResult") {
    setupLargeResultTable
    val tmpDF = sparkSession
      .sql(s"select * from test_table_large_result where int_c < -1")

    var resultSet: Array[Row] = tmpDF.collect()
    val sourceLength = resultSet.length
    assert(sourceLength == 0)

    // Write the Data back to snowflake
    jdbcUpdate(s"drop table if exists $test_table_write")
    tmpDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .option("truncate_table", "off")
      .option("usestagingtable", "on")
      .mode(SaveMode.Overwrite)
      .save()

    // Read back the written data.
    val readBackDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_write")
      .load()

    resultSet = readBackDF.collect()
    assert(resultSet.length == sourceLength)
  }

  // Some partitions are empty, but some are not
  test("testReadWriteSomePartitionsEmpty") {
    setupLargeResultTable
    if (!skipBigDataTest) {
      val originalDF = sparkSession
        .sql(s"select * from test_table_large_result")

      // Use UDF to avoid FILTER to be push-down.
      import org.apache.spark.sql.functions._
      val betweenUdf = udf((x: Integer, min: Integer, max: Integer) => {
        if (x >= min && x < max) true else false
      })
      val tmpDF = originalDF.filter(
        betweenUdf(col("int_c"), lit(400000), lit(500000)))

      var resultSet: Array[Row] = tmpDF.collect()
      val sourceLength = resultSet.length
      assert(sourceLength == 100000)

      // Write the Data back to snowflake
      jdbcUpdate(s"drop table if exists $test_table_write")
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_write)
        .option("truncate_table", "off")
        .option("usestagingtable", "on")
        .mode(SaveMode.Overwrite)
        .save()

      // Read back the written data.
      val readBackDF = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", s"$test_table_write")
        .load()

      resultSet = readBackDF.collect()
      assert(resultSet.length == sourceLength)
    }
  }

  // large table read and write.
  test("testReadWriteLargeTable") {
    setupLargeResultTable
    if (!skipBigDataTest) {
      val tmpDF = sparkSession
        .sql(s"select * from test_table_large_result order by int_c")

      var resultSet: Array[Row] = tmpDF.collect()
      val sourceLength = resultSet.length
      assert(sourceLength == LARGE_TABLE_ROW_COUNT)

      // Write the Data back to snowflake
      jdbcUpdate(s"drop table if exists $test_table_write")
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_write)
        .option("truncate_table", "off")
        .option("usestagingtable", "on")
        .mode(SaveMode.Overwrite)
        .save()

      // Read back the written data.
      val readBackDF = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", s"$test_table_write")
        .load()

      resultSet = readBackDF.collect()
      assert(resultSet.length == sourceLength)
    }
  }

  // Negative test for GCP doesn't support use_copy_unload = true
  test("GCP only supports use_copy_unload = true") {
    setupLargeResultTable
    if ("gcp".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"))) {
      var oldValue: Option[String] = None
      if (thisConnectorOptionsNoTable.contains("use_copy_unload")) {
        oldValue = Some(thisConnectorOptionsNoTable("use_copy_unload"))
        thisConnectorOptionsNoTable -= "use_copy_unload"
      }
      thisConnectorOptionsNoTable += ("use_copy_unload" -> "true")

      val tmpDF = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", s"$test_table_large_result")
        .load()

      assertThrows[SnowflakeConnectorFeatureNotSupportException] {
        tmpDF.collect()
      }
      thisConnectorOptionsNoTable -= "use_copy_unload"
      if (oldValue.isDefined) {
        thisConnectorOptionsNoTable += ("use_copy_unload" -> oldValue.get)
      }
    } else {
      println("Skip the test on non-GCP platform")
    }
  }

  // Test sfURL to support the sfURL to begin with http:// or https://
  test("Test sfURL begin with http:// or https://") {
    setupLargeResultTable
    val origSfURL = thisConnectorOptionsNoTable("sfurl")
    val testURLs = List(s"http://$origSfURL", s"https://$origSfURL")
    testURLs.foreach(url => {
      thisConnectorOptionsNoTable -= "sfurl"
      thisConnectorOptionsNoTable += ("sfurl" -> url)

      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", s"$test_table_large_result")
        .load()
        .count()
    })
    // reset sfRUL
    thisConnectorOptionsNoTable -= "sfurl"
    thisConnectorOptionsNoTable += ("sfurl" -> origSfURL)
  }

  // Negative test for hitting exception when uploading data to cloud
  test("Test GCP uploading retry works") {
    setupLargeResultTable
    // Enable test hook to simulate upload error
    TestHook.enableTestFlagOnly(TestHookFlag.TH_GCS_UPLOAD_RAISE_EXCEPTION)

    // Set max_retry_count to small value to avoid bock testcase too long.
    thisConnectorOptionsNoTable += ("max_retry_count" -> "2")

    val tmpDF = sparkSession
      .sql(s"select * from test_table_large_result where int_c < 1000")

    // Write the Data back to snowflake
    jdbcUpdate(s"drop table if exists $test_table_write")
    // Test use_exponential_backoff is 'on' or 'off'
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

  test("Test Utils.runQuery") {
    setupLargeResultTable
    val rs = Utils.runQuery(thisConnectorOptionsNoTable, s"select * from $test_table_large_result")
    var count = 0
    while(rs.next()) {
      count += 1
    }
    assert(count == LARGE_TABLE_ROW_COUNT)
  }

  test("count super large table") {
    if (!skipBigDataTest) {
      val expectedCount = Int.MaxValue + 1L
      val actualCount = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("query", s"select 1 from table(generator(rowcount => $expectedCount))")
        .load()
        .count()
      assert(actualCount == expectedCount)
    }
  }

  // Copy one table from AWS account to GCP account
  ignore("copy data from AWS to GCP") {
    val moveTableName = "LINEITEM_FROM_PARQUET"
    val awsSchema = "TPCH_SF100"

    var awsOptionsNoTable: Map[String, String] = Map()
    connectorOptionsNoTable.foreach(tup => {
      awsOptionsNoTable += tup
    })
    awsOptionsNoTable -= "sfURL"
    awsOptionsNoTable += ("sfURL" -> "sfctest0.snowflakecomputing.com")

    var gcpOptionsNoTable: Map[String, String] = Map()
    connectorOptionsNoTable.foreach(tup => {
      gcpOptionsNoTable += tup
    })
    gcpOptionsNoTable -= "sfURL"
    gcpOptionsNoTable += ("sfURL" -> "sfctest0.us-central1.gcp.snowflakecomputing.com")

    // Read data from AWS.
    val awsReadDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(awsOptionsNoTable)
      .option("query", s"select * from $awsSchema.$moveTableName where 1 = 0")
      .load()

    // Write the Data to GCP
    awsReadDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(gcpOptionsNoTable)
      .option("dbtable", moveTableName)
      .option("truncate_table", "off")
      .option("usestagingtable", "on")
      .mode(SaveMode.Overwrite)
      .save()

    // clean up download dir
    import sys.process._
    "rm -fr /tmp/test_move_data" !

    "mkdir /tmp/test_move_data" !

    val awsConn = Utils.getJDBCConnection(awsOptionsNoTable)
    awsConn.createStatement().execute("create or replace stage test_move_data")
    awsConn.createStatement().execute(
      s"""COPY INTO '@test_move_data/' FROM ( SELECT * FROM $awsSchema.$moveTableName )
      FILE_FORMAT = (
      TYPE=CSV
      COMPRESSION='gzip'
      FIELD_DELIMITER='|'
      FIELD_OPTIONALLY_ENCLOSED_BY='"'
      ESCAPE_UNENCLOSED_FIELD = none
      NULL_IF= ()
      )""".stripMargin
    )
    awsConn.createStatement.execute("GET @test_move_data/ file:///tmp/test_move_data")
    awsConn.close()

    val gcpConn = Utils.getJDBCConnection(gcpOptionsNoTable)
    gcpConn.createStatement().execute("create or replace stage test_move_data")
    gcpConn.createStatement.execute("PUT file:///tmp/test_move_data/*.gz @test_move_data/")

    gcpConn.createStatement().execute(
      s"""copy into $moveTableName FROM @test_move_data/
         |FILE_FORMAT = (
         |    TYPE=CSV
         |    FIELD_DELIMITER='|'
         |    NULL_IF=()
         |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
         |    TIMESTAMP_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF3'
         |    DATE_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF3'
         |  )""".stripMargin
    )
    gcpConn.close()
  }

  test("test normal LIKE pushdown 1") {
    setupTableForLike
    // Table values in test_table_like
    // ('John  Dddoe'), ('Joe   Doe'), ('John_down'),
    // ('Joe down'), ('Elaine'), (''), (null),
    // ('100 times'), ('1000 times'), ('100%')

    // Normal LIKE: subject like '%Jo%oe%'
    val result1 = sparkSession.sql(
      s"select * from test_table_like where subject like '%Jo%oe%' order by 1")

    val expectedResult1 = Seq(
      Row("Joe   Doe"), Row("John  Dddoe")
    )

    checkAnswer(
      result1,
      expectedResult1
    )
  }

  test("test normal LIKE pushdown 2") {
    setupTableForLike
    // Table values in test_table_like
    // ('John  Dddoe'), ('Joe   Doe'), ('John_down'),
    // ('Joe down'), ('Elaine'), (''), (null),
    // ('100 times'), ('1000 times'), ('100%')

    // Normal LIKE: subject like '%Jo%oe%'
    val result1 = sparkSession.sql(
      s"select * from test_table_like where subject like '100%' order by 1")

    val expectedResult1 = Seq(
      Row("100 times"), Row("100%"), Row("1000 times")
    )

    checkAnswer(
      result1,
      expectedResult1
    )
  }

  test("test normal NOT LIKE pushdown") {
    setupTableForLike
    // Only run the test with Arrow format,
    // there is empty string in the result, it is read as NULL for COPY UNLOAD.
    if (!params.useCopyUnload) {
      // Table values in test_table_like
      // ('John  Dddoe'), ('Joe   Doe'), ('John_down'),
      // ('Joe down'), ('Elaine'), (''), (null),
      // ('100 times'), ('1000 times'), ('100%')

      // Normal NOT LIKE: subject not like '%Jo%oe%'
      val result2 = sparkSession.sql(
        s"select * from test_table_like where subject not like '%Jo%oe%' order by 1")

      val expectedResult2 = Seq(
        Row(""), Row("100 times"), Row("100%"), Row("1000 times"),
        Row("Elaine"), Row("Joe down"), Row("John_down")
      )

      checkAnswer(
        result2,
        expectedResult2
      )
    }
  }

  // This is supported from Spark 3.0
  test("test LIKE with ESCAPE pushdown 1") {
    setupTableForLike
    // Table values in test_table_like
    // ('John  Dddoe'), ('Joe   Doe'), ('John_down'),
    // ('Joe down'), ('Elaine'), (''), (null),
    // ('100 times'), ('1000 times'), ('100%')

    // Normal NOT LIKE: subject not like '%Jo%oe%'
    val result2 = sparkSession.sql(
      s"select * from test_table_like where subject like '%J%h%^_do%' escape '^' order by 1")

    val expectedResult2 = Seq(
      Row("John_down")
    )

    checkAnswer(
      result2,
      expectedResult2
    )
  }

  // There is a Spark issue, so below test case is ignored temporarily.
  // Spark issue is filed: https://issues.apache.org/jira/browse/SPARK-31210
  // With this SQL, the Spark transforms the LIKE as
  // "StartsWith(subject#130, 100^)" so LIKE Pushdown is not triggered.
  // This issue is duplicated to https://issues.apache.org/jira/browse/SPARK-30254
  // Apache Spark engineers have fixed it. This test can be enabled in formal
  // Apache Spark release.
  // This is supported from Spark 3.0
  test ("test LIKE with ESCAPE pushdown 2") {
    setupTableForLike
    // Table values in test_table_like
    // ('John  Dddoe'), ('Joe   Doe'), ('John_down'),
    // ('Joe down'), ('Elaine'), (''), (null),
    // ('100 times'), ('1000 times'), ('100%')

    // Normal NOT LIKE: like '100^%' escape '^'
    val result2 = sparkSession.sql(
      s"select * from test_table_like where subject like '100^%' escape '^' order by 1")

    val expectedResult2 = Seq(
      Row("100%")
    )

    checkAnswer(
      result2,
      expectedResult2
    )
  }

  // This is supported from Spark 3.0
  test("test NOT LIKE with ESCAPE pushdown") {
    setupTableForLike
    // Only run the test with Arrow format,
    // there is empty string in the result, it is read as NULL for COPY UNLOAD.
    if (!params.useCopyUnload) {
      // Table values in test_table_like
      // ('John  Dddoe'), ('Joe   Doe'), ('John_down'),
      // ('Joe down'), ('Elaine'), (''), (null),
      // ('100 times'), ('1000 times'), ('100%')

      // Normal NOT LIKE: subject not like '%Jo%oe%'
      val result2 = sparkSession.sql(
        s"select * from test_table_like where subject not like '%J%h%^_do%' escape '^' order by 1")

      val expectedResult2 = Seq(
        Row(""),
        Row("100 times"),
        Row("100%"),
        Row("1000 times"),
        Row("Elaine"),
        Row("Joe   Doe"),
        Row("Joe down"),
        Row("John  Dddoe")
      )

      checkAnswer(
        result2,
        expectedResult2
      )
    }
  }

  // Write large partition (uncompressed size > 2 GB)
  test("test large partition") {
    // Only run this test on AWS/Azure because
    // uncompressed data is not cached on them.
    if (!"gcp".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"))) {
      val thisSparkSession = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .config("spark.master", "local")
        .getOrCreate()

      def getRandomString(len: Int): String = {
        Random.alphanumeric take len mkString ""
      }

      val partitionCount = 1
      val rowCountPerPartition = 1024 * 1024
      val strValue = getRandomString(512)
      // Create RDD which generates 1 large partition
      val testRDD: RDD[Row] = thisSparkSession.sparkContext
        .parallelize(Seq[Int](), partitionCount)
        .mapPartitions { _ => {
          (1 to rowCountPerPartition).map { _ => {
            Row(strValue, strValue, strValue, strValue)
          }
          }.iterator
        }
        }
      val schema = StructType(
        List(
          StructField("str1", StringType),
          StructField("str2", StringType),
          StructField("str3", StringType),
          StructField("str4", StringType)
        )
      )

      // Convert RDD to DataFrame
      val df = thisSparkSession.createDataFrame(testRDD, schema)

      // Write to snowflake
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_write)
        .mode(SaveMode.Overwrite)
        .save()
      assert(getRowCount(test_table_write) == partitionCount * rowCountPerPartition)
    }
  }

  test("test upload with AWS multiple parts upload API") {
    val partitionCount = 1
    val rowCountPerPartition = 1024 * 50
    // It is enough to run this test on AWS for Arrow
    if (Option(System.getenv("SNOWFLAKE_TEST_ACCOUNT")).getOrElse("aws").equals("aws") &&
      !params.useCopyUnload) {
      val thisSparkSession = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .config("spark.master", "local")
        .getOrCreate()

      def getRandomString(len: Int): String = {
        Random.alphanumeric take len mkString ""
      }

      // Create RDD which generates 1 large partition
      val testRDD: RDD[Row] = thisSparkSession.sparkContext
        .parallelize(Seq[Int](), partitionCount)
        .mapPartitions { _ => {
          (1 to rowCountPerPartition).map { _ => {
            // generate value for each row for large compressed size
            val strValue = getRandomString(512)
            Row(strValue, strValue, strValue, strValue)
          }
          }.iterator
        }
        }
      val schema = StructType(
        List(
          StructField("str1", StringType),
          StructField("str2", StringType),
          StructField("str3", StringType),
          StructField("str4", StringType)
        )
      )

      // Convert RDD to DataFrame
      val df = thisSparkSession.createDataFrame(testRDD, schema)

      // Write to snowflake with upload multiple part API
      jdbcUpdate(s"drop table if exists $test_table_write")
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_UPLOAD_CHUNK_SIZE_IN_MB, "5")
        .option("dbtable", test_table_write)
        .mode(SaveMode.Overwrite)
        .save()
      assert(getRowCount(test_table_write) == partitionCount * rowCountPerPartition)

      // Write to snowflake without upload multiple part API
      jdbcUpdate(s"drop table if exists $test_table_write")
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_USE_AWS_MULTIPLE_PARTS_UPLOAD, "off")
        .option("dbtable", test_table_write)
        .mode(SaveMode.Overwrite)
        .save()
      assert(getRowCount(test_table_write) == partitionCount * rowCountPerPartition)

      // Negative test to inject exception after upload 2nd block
      TestHook.enableTestFlagOnly(TestHookFlag.TH_FAIL_UPLOAD_AWS_2ND_BLOCK)
      assertThrows[Exception]({
        df.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option(Parameters.PARAM_UPLOAD_CHUNK_SIZE_IN_MB, "5")
          .option("dbtable", test_table_write)
          .mode(SaveMode.Overwrite)
          .save()
      })
      TestHook.disableTestHook()
    }
  }

  test("treat decimal(X, 0) as long") {
    val df = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option(Parameters.PARAM_TREAT_DECIMAL_AS_LONG, "true")
      .option("query", s"select 1 as a, 2::Decimal(10, 0) as b, 3::Decimal(10, 1) as c")
      .load()

    val schema = df.schema
    assert(schema.fields(0).dataType == LongType)
    assert(schema.fields(1).dataType == LongType)
    // Column c is Decimal(10, 1) which is not affected by this option
    assert(schema.fields(2).dataType == DecimalType(10, 1))

    val row = df.collect()(0)
    assert(row.schema.fields(0).dataType == LongType)
    assert(row.schema.fields(1).dataType == LongType)
    assert(row.schema.fields(2).dataType == DecimalType(10, 1))
    assert(row.getLong(0) == 1)
    assert(row.getLong(1) == 2)
    assert(row.getDecimal(2) == java.math.BigDecimal.valueOf(3.0))
  }

  test("negative test: treat decimal(X, 0) as long") {
    val maxLong10 = s"${Long.MaxValue}0"
    // Conversion will happen if the decimal value is overflow for Long
    val ex = intercept[Exception] {
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_TREAT_DECIMAL_AS_LONG, "true")
        .option("query", s"select $maxLong10 :: Decimal(38, 0)")
        .load()
        .collect()
    }
    assert(ex.getMessage.contains(maxLong10))

    // It will work with decimal (default)
    val row = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("query", s"select $maxLong10 :: Decimal(38, 0)")
      .load()
      .collect()(0)
    val decimalMaxLong10 = java.math.BigDecimal.valueOf(Long.MaxValue)
      .multiply(java.math.BigDecimal.valueOf(10))
    assert(row.getDecimal(0) == decimalMaxLong10)
  }

  ignore("perf test for AWS multiple parts upload API") {
    val maxRunTime = 3
    val partitionCount = 1
    val rowCountPerPartition = 1024 * 300
    // It is enough to run this test on AWS for Arrow
    if (Option(System.getenv("SNOWFLAKE_TEST_ACCOUNT")).getOrElse("aws").equals("aws") &&
      !params.useCopyUnload) {
      val thisSparkSession = SparkSession
        .builder()
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .config("spark.master", "local")
        .getOrCreate()

      def getRandomString(len: Int): String = {
        Random.alphanumeric take len mkString ""
      }

      // Create RDD which generates 1 large partition
      val testRDD: RDD[Row] = thisSparkSession.sparkContext
        .parallelize(Seq[Int](), partitionCount)
        .mapPartitions { _ => {
          (1 to rowCountPerPartition).map { _ => {
            // generate value for each row for large compressed size
            val strValue = getRandomString(512)
            Row(strValue, strValue, strValue, strValue)
          }
          }.iterator
        }
        }
      val schema = StructType(
        List(
          StructField("str1", StringType),
          StructField("str2", StringType),
          StructField("str3", StringType),
          StructField("str4", StringType)
        )
      )

      // Convert RDD to DataFrame
      val df = thisSparkSession.createDataFrame(testRDD, schema)

      var perfResults = new ListBuffer[Long]()
      var runId = 0
      // Write to snowflake without upload multiple part API
      // This is the baseline
      while (runId < maxRunTime) {
        jdbcUpdate(s"drop table if exists $test_table_write")

        val start = System.currentTimeMillis()
        df.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option(Parameters.PARAM_USE_AWS_MULTIPLE_PARTS_UPLOAD, "off")
          .option("dbtable", test_table_write)
          .mode(SaveMode.Overwrite)
          .save()
        perfResults += (System.currentTimeMillis() - start)
        runId += 1

        assert(getRowCount(test_table_write) == partitionCount * rowCountPerPartition)
      }
      val result_no_buf = perfResults.toArray

      runId = 0
      perfResults = new ListBuffer[Long]()
      // Write to snowflake with upload multiple part API: block size 8m
      while (runId < maxRunTime) {
        jdbcUpdate(s"drop table if exists $test_table_write")

        val start = System.currentTimeMillis()
        df.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option(Parameters.PARAM_UPLOAD_CHUNK_SIZE_IN_MB, "8") // default
          .option("dbtable", test_table_write)
          .mode(SaveMode.Overwrite)
          .save()
        perfResults += (System.currentTimeMillis() - start)
        runId += 1

        assert(getRowCount(test_table_write) == partitionCount * rowCountPerPartition)
      }
      val result_8m_buf = perfResults.toArray

      runId = 0
      perfResults = new ListBuffer[Long]()
      // Write to snowflake with upload multiple part API: block size 16M
      while (runId < maxRunTime) {
        jdbcUpdate(s"drop table if exists $test_table_write")

        val start = System.currentTimeMillis()
        df.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option(Parameters.PARAM_UPLOAD_CHUNK_SIZE_IN_MB, "16")
          .option("dbtable", test_table_write)
          .mode(SaveMode.Overwrite)
          .save()
        perfResults += (System.currentTimeMillis() - start)
        runId += 1

        assert(getRowCount(test_table_write) == partitionCount * rowCountPerPartition)
      }
      val result_16m_buf = perfResults.toArray

      runId = 0
      perfResults = new ListBuffer[Long]()
      // Write to snowflake with upload multiple part API: block size 32M
      while (runId < maxRunTime) {
        jdbcUpdate(s"drop table if exists $test_table_write")

        val start = System.currentTimeMillis()
        df.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option(Parameters.PARAM_UPLOAD_CHUNK_SIZE_IN_MB, "32")
          .option("dbtable", test_table_write)
          .mode(SaveMode.Overwrite)
          .save()
        perfResults += (System.currentTimeMillis() - start)
        runId += 1

        assert(getRowCount(test_table_write) == partitionCount * rowCountPerPartition)
      }
      val result_32m_buf = perfResults.toArray

      runId = 0
      perfResults = new ListBuffer[Long]()
      // Write to snowflake with upload multiple part API: block size 64M
      while (runId < maxRunTime) {
        jdbcUpdate(s"drop table if exists $test_table_write")

        val start = System.currentTimeMillis()
        df.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option(Parameters.PARAM_UPLOAD_CHUNK_SIZE_IN_MB, "64")
          .option("dbtable", test_table_write)
          .mode(SaveMode.Overwrite)
          .save()
        perfResults += (System.currentTimeMillis() - start)
        runId += 1

        assert(getRowCount(test_table_write) == partitionCount * rowCountPerPartition)
      }
      val result_64m_buf = perfResults.toArray

      // output perf result
      if (maxRunTime > 0) {
        def getPerfReport(results: Array[Long]): String = {
          val buf = new StringBuilder(
            s"Average ${Utils.getTimeString(results.sum / results.size)}: ")
          results.foreach(x => buf.append(Utils.getTimeString(x)).append(", "))
          buf.toString()
        }

        println(s"AWS_PERF: Run $maxRunTime times")
        println(s"AWS_PERF: without new API : ${getPerfReport(result_no_buf)}")
        println(s"AWS_PERF: with new API 8M : ${getPerfReport(result_8m_buf)}")
        println(s"AWS_PERF: with new API 16M: ${getPerfReport(result_16m_buf)}")
        println(s"AWS_PERF: with new API 32M: ${getPerfReport(result_32m_buf)}")
        println(s"AWS_PERF: with new API 64M: ${getPerfReport(result_64m_buf)}")
      }
    }
  }

  // This function is a sample code
  ignore("Sample code to generate random data and write to snowflake") {
    val thisSparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .config("spark.master", "local")
      .getOrCreate()

    def getRandomString(len: Int): String = {
      Random.alphanumeric take len mkString ""
    }

    val partitionCount = 2
    val rowCountPerPartition = 1300000
    // Create RDD which generates data with multiple partitions
    val testRDD: RDD[Row] = thisSparkSession.sparkContext
      .parallelize(Seq[Int](), partitionCount)
      .mapPartitions { _ => {
        (1 to rowCountPerPartition).map { _ => {
          Row(Random.nextInt, Random.nextDouble(), getRandomString(50),
            Random.nextInt, Random.nextDouble(), getRandomString(50),
            Random.nextInt, Random.nextDouble(), getRandomString(50))
        }
        }.iterator
      }
      }
    val schema = StructType(
      List(
        StructField("int1", IntegerType),
        StructField("double1", DoubleType),
        StructField("str1", StringType),
        StructField("int2", IntegerType),
        StructField("double2", DoubleType),
        StructField("str2", StringType),
        StructField("int3", IntegerType),
        StructField("double3", DoubleType),
        StructField("str3", StringType)
      )
    )

    // Convert RDD to DataFrame
    val df = thisSparkSession.createDataFrame(testRDD, schema)

    // Write to snowflake
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .mode(SaveMode.Overwrite)
      .save()
  }

  test("write with JSON and special characters in field name") {
    val partitionCount = 1
    val rowCountPerPartition = 2
    // Create RDD which generates data with multiple partitions
    val testRDD: RDD[Row] = sparkSession.sparkContext
      .parallelize(Seq[Int](), partitionCount)
      .mapPartitions { _ => {
        (1 to rowCountPerPartition).map { i => {
          Row(s"name_$i",
            Map("a1" -> s"value_a$i", "a2" -> s"value_aa$i"),
            Map("b1" -> s"value_b$i", "b2" -> s"value_bb$i"),
            Map("c1" -> s"value_c$i", "c2" -> s"value_cc$i"),
            Map("d1" -> s"value_d$i", "d2" -> s"value_dd$i"))
        }
        }.iterator
      }
      }

    // Field names have special characters
    val (colName0, colName1, colName2, colName3, colName4)
    : (String, String, String, String, String) =
      ("first_name", "some:for", "some;for", "some,for", "some for")
    val schema = StructType(List(
      StructField(colName0, StringType),
      StructField(colName1, MapType(StringType, StringType)),
      StructField(colName2, MapType(StringType, StringType)),
      StructField(colName3, MapType(StringType, StringType)),
      StructField(colName4, MapType(StringType, StringType))
    ))

    // Convert RDD to DataFrame
    val df = sparkSession.createDataFrame(testRDD, schema)

    // Write to snowflake
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .mode(SaveMode.Overwrite)
      .save()

    // read data back to check correctness
    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("query", s"select * from $test_table_write")
      .load()

    val expectedResultArrow = Seq(
      Row("name_1",
        s"""{\n  "a1": "value_a1",\n  "a2": "value_aa1"\n}""",
        s"""{\n  "b1": "value_b1",\n  "b2": "value_bb1"\n}""",
        s"""{\n  "c1": "value_c1",\n  "c2": "value_cc1"\n}""",
        s"""{\n  "d1": "value_d1",\n  "d2": "value_dd1"\n}"""),
      Row("name_2",
        s"""{\n  "a1": "value_a2",\n  "a2": "value_aa2"\n}""",
        s"""{\n  "b1": "value_b2",\n  "b2": "value_bb2"\n}""",
        s"""{\n  "c1": "value_c2",\n  "c2": "value_cc2"\n}""",
        s"""{\n  "d1": "value_d2",\n  "d2": "value_dd2"\n}""")
    )
    val expectedResult = if (params.useCopyUnload) {
      // The returned format is different for USE_COPY_UNLOAD = true
      def replaceSpace(data: String) = data.replaceAll("\n", "").replaceAll(" ", "")
      expectedResultArrow.map(r => Row(
        r.getString(0),
        replaceSpace(r.getString(1)),
        replaceSpace(r.getString(2)),
        replaceSpace(r.getString(3)),
        replaceSpace(r.getString(4))))
    } else {
      expectedResultArrow
    }

    // Check the result is expected.
    checkAnswer(
      result,
      expectedResult
    )
  }

  test("write with JSON and without special characters in field name") {
    val df = getTestJsonDF("FIRST_name", "some_for1", "some_for2", "some_for3", "some_for4")

    val quotesFieldName = Seq("true", "false")
    // If there is no special characters in field names,
    // it works no matter PARAM_INTERNAL_QUOTE_JSON_FIELD_NAME is true or false.
    quotesFieldName.foreach( quote => {
      val localSFOption = replaceOption(thisConnectorOptionsNoTable,
        Parameters.PARAM_INTERNAL_QUOTE_JSON_FIELD_NAME, quote)
      // Write to snowflake
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(localSFOption)
        .option("dbtable", test_table_write)
        .mode(SaveMode.Overwrite)
        .save()

      // read data back to check correctness
      val result = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(localSFOption)
        .option("query", s"select * from $test_table_write")
        .load()

      val expectedResultArrow = Seq(
        Row("name_1",
          s"""{\n  "a1": "value_a1",\n  "a2": "value_aa1"\n}""",
          s"""{\n  "b1": "value_b1",\n  "b2": "value_bb1"\n}""",
          s"""{\n  "c1": "value_c1",\n  "c2": "value_cc1"\n}""",
          s"""{\n  "d1": "value_d1",\n  "d2": "value_dd1"\n}"""),
        Row("name_2",
          s"""{\n  "a1": "value_a2",\n  "a2": "value_aa2"\n}""",
          s"""{\n  "b1": "value_b2",\n  "b2": "value_bb2"\n}""",
          s"""{\n  "c1": "value_c2",\n  "c2": "value_cc2"\n}""",
          s"""{\n  "d1": "value_d2",\n  "d2": "value_dd2"\n}""")
      )
      val expectedResult = if (params.useCopyUnload) {
        // The returned format is different for USE_COPY_UNLOAD = true
        def replaceSpace(data: String) = data.replaceAll("\n", "").replaceAll(" ", "")
        expectedResultArrow.map(r => Row(
          r.getString(0),
          replaceSpace(r.getString(1)),
          replaceSpace(r.getString(2)),
          replaceSpace(r.getString(3)),
          replaceSpace(r.getString(4))))
      } else {
        expectedResultArrow
      }

      // Check the result is expected.
      checkAnswer(
        result,
        expectedResult
      )
    })
  }

  private def getTestJsonDF(colName0: String,
                            colName1: String,
                            colName2: String,
                            colName3: String,
                            colName4: String): DataFrame = {
    val partitionCount = 1
    val rowCountPerPartition = 2
    val testRDD: RDD[Row] = sparkSession.sparkContext
      .parallelize(Seq[Int](), partitionCount)
      .mapPartitions { _ => {
        (1 to rowCountPerPartition).map { i => {
          Row(s"name_$i",
            Map("a1" -> s"value_a$i", "a2" -> s"value_aa$i"),
            Map("b1" -> s"value_b$i", "b2" -> s"value_bb$i"),
            Map("c1" -> s"value_c$i", "c2" -> s"value_cc$i"),
            Map("d1" -> s"value_d$i", "d2" -> s"value_dd$i")
          )
        }
        }.iterator
      }
      }

    val schema = StructType(List(
      StructField(colName0, StringType),
      StructField(colName1, MapType(StringType, StringType)),
      StructField(colName2, MapType(StringType, StringType)),
      StructField(colName3, MapType(StringType, StringType)),
      StructField(colName4, MapType(StringType, StringType))
    ))

    sparkSession.createDataFrame(testRDD, schema)
  }

  test("column name is snowflake keyword: SNOW-293194") {
    val df = getTestJsonDF("create", "table", "View", "INSERT", "test")
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .mode(SaveMode.Overwrite)
      .save()

    assert(sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("query", s"select * from $test_table_write")
      .load().count == 2)

    // It fails if disable the column name quotation
    assertThrows[Exception] {
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_write)
        .option(Parameters.PARAM_INTERNAL_QUOTE_JSON_FIELD_NAME, "false")
        .mode(SaveMode.Overwrite)
        .save()
    }

    // If the column name is not keyword, it works even if the internal disable is disabled
    val df2 = getTestJsonDF("create1", "table1", "View1", "INSERT1", "test1")
    df2.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .option(Parameters.PARAM_INTERNAL_QUOTE_JSON_FIELD_NAME, "false")
      .mode(SaveMode.Overwrite)
      .save()

    assert(sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("query", s"select * from $test_table_write")
      .load().count == 2)
  }

  test("repro & test SNOW-262080") {
    setupLargeResultTable
    val tmpDF = sparkSession
      .sql("select * from test_table_large_result where int_c < 10")

    // To test check_table_existence_in_current_schema,
    // internal_check_table_existence_with_fully_qualified_name need to be false
    thisConnectorOptionsNoTable +=
      (Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_WITH_FULLY_QUALIFIED_NAME -> "false")

    try {
      // create one same name table in schema:public
      jdbcUpdate(s"create table public.$test_table_write(c1 int)")
      // drop table in this schema
      jdbcUpdate(s"drop table if exists $test_table_write")

      // Staging table with check_table_existence_in_current_schema = "false"
      assertThrows[Exception]({
        tmpDF.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_IN_CURRENT_SCHEMA_ONLY, "false")
          .option("dbtable", test_table_write)
          .mode(SaveMode.Overwrite)
          .save()
      })

      // Staging table with check_table_existence_in_current_schema = "true", table doesn't exist
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_IN_CURRENT_SCHEMA_ONLY, "true")
        .option("dbtable", test_table_write)
        .mode(SaveMode.Overwrite)
        .save()

      // Staging table with check_table_existence_in_current_schema = "true", table exists
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_IN_CURRENT_SCHEMA_ONLY, "true")
        .option("dbtable", test_table_write)
        .mode(SaveMode.Overwrite)
        .save()

      jdbcUpdate(s"drop table if exists $test_table_write")
      // Without staging table with check_table_existence_in_current_schema = "false"
      assertThrows[Exception]({
        tmpDF.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_IN_CURRENT_SCHEMA_ONLY, "false")
          .option("dbtable", test_table_write)
          .option("usestagingtable", "false")
          .option("truncate_table", "true")
          .mode(SaveMode.Overwrite)
          .save()
      })

      // Without staging table with check_table_existence_in_current_schema = "true",
      // table doesn't exist
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_IN_CURRENT_SCHEMA_ONLY, "true")
        .option("dbtable", test_table_write)
        .option("usestagingtable", "false")
        .option("truncate_table", "true")
        .mode(SaveMode.Overwrite)
        .save()

      // Without staging table with check_table_existence_in_current_schema = "true", table exists
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_IN_CURRENT_SCHEMA_ONLY, "true")
        .option("dbtable", test_table_write)
        .option("usestagingtable", "false")
        .option("truncate_table", "true")
        .mode(SaveMode.Overwrite)
        .save()
    } finally {
      jdbcUpdate(s"drop table if exists $test_table_write")
      jdbcUpdate(s"drop table if exists public.$test_table_write")
      thisConnectorOptionsNoTable -=
        Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_WITH_FULLY_QUALIFIED_NAME
    }
  }

  test("The fix of SNOW-262080 with SaveMode.ErrorIfExists") {
    setupLargeResultTable
    val tmpDF = sparkSession
      .sql("select * from test_table_large_result where int_c < 10")

    try {
      // create one same name table in schema:public
      jdbcUpdate(s"create table public.$test_table_write(c1 int)")
      // drop table in this schema
      jdbcUpdate(s"drop table if exists $test_table_write")

      // Staging table with check_table_existence_in_current_schema = "false"
      assertThrows[Exception]({
        // Reproduce issue. The table doesn't exists, but table existence checking returns true.
        tmpDF.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_IN_CURRENT_SCHEMA_ONLY, "false")
          .option("dbtable", test_table_write)
          .mode(SaveMode.ErrorIfExists)
          .save()
      })

      // Staging table with check_table_existence_in_current_schema = "true"
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_IN_CURRENT_SCHEMA_ONLY, "true")
        .option("dbtable", test_table_write)
        .mode(SaveMode.ErrorIfExists)
        .save()
    } finally {
      jdbcUpdate(s"drop table if exists $test_table_write")
      jdbcUpdate(s"drop table if exists public.$test_table_write")
    }
  }

  test("The fix of SNOW-262080 with SaveMode.Ignore") {
    setupLargeResultTable
    val tmpDF = sparkSession
      .sql("select * from test_table_large_result where int_c < 10")

    try {
      // create one same name table in schema:public
      jdbcUpdate(s"create table public.$test_table_write(c1 int)")
      // drop table in this schema
      jdbcUpdate(s"drop table if exists $test_table_write")

      // Staging table with check_table_existence_in_current_schema = "false"
      // Reproduce issue:
      // The table doesn't exist, but table existence checking returns true.
      // So the write is ignored. Later read fails.
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_IN_CURRENT_SCHEMA_ONLY, "false")
        .option("dbtable", test_table_write)
        .mode(SaveMode.Ignore)
        .save()
      assertThrows[Exception]({
        sparkSession.read
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option("dbtable", s"${params.sfDatabase}.${params.sfSchema}.$test_table_write")
          .load()
      })

      // Staging table with check_table_existence_in_current_schema = "true"
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_IN_CURRENT_SCHEMA_ONLY, "true")
        .option("dbtable", test_table_write)
        .mode(SaveMode.Ignore)
        .save()
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", s"${params.sfDatabase}.${params.sfSchema}.$test_table_write")
        .load()
    } finally {
      jdbcUpdate(s"drop table if exists $test_table_write")
      jdbcUpdate(s"drop table if exists public.$test_table_write")
    }
  }

  // Copy from test("repro & test SNOW-262080") and modify it
  test("test SNOW-521177") {
    setupLargeResultTable
    val tmpDF = sparkSession
      .sql("select * from test_table_large_result where int_c < 10")

    // To test internal_check_table_existence_with_fully_qualified_name,
    // check_table_existence_in_current_schema need to be false
    thisConnectorOptionsNoTable +=
      (Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_IN_CURRENT_SCHEMA_ONLY -> "false")

    try {
      // create one same name table in schema:public
      jdbcUpdate(s"create table public.$test_table_write(c1 int)")
      // drop table in this schema
      jdbcUpdate(s"drop table if exists $test_table_write")

      // Staging table with check_table_existence_in_current_schema = "false"
      assertThrows[Exception]({
        tmpDF.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option(
            Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_WITH_FULLY_QUALIFIED_NAME, "false")
          .option("dbtable", test_table_write)
          .mode(SaveMode.Overwrite)
          .save()
      })

      // Staging table with check_table_existence_in_current_schema = "true", table doesn't exist
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_WITH_FULLY_QUALIFIED_NAME, "true")
        .option("dbtable", test_table_write)
        .mode(SaveMode.Overwrite)
        .save()

      // Staging table with check_table_existence_in_current_schema = "true", table exists
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_WITH_FULLY_QUALIFIED_NAME, "true")
        .option("dbtable", test_table_write)
        .mode(SaveMode.Overwrite)
        .save()

      jdbcUpdate(s"drop table if exists $test_table_write")
      // Without staging table with check_table_existence_in_current_schema = "false"
      assertThrows[Exception]({
        tmpDF.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option(
            Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_WITH_FULLY_QUALIFIED_NAME, "false")
          .option("dbtable", test_table_write)
          .option("usestagingtable", "false")
          .option("truncate_table", "true")
          .mode(SaveMode.Overwrite)
          .save()
      })

      // Without staging table with check_table_existence_in_current_schema = "true",
      // table doesn't exist
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_WITH_FULLY_QUALIFIED_NAME, "true")
        .option("dbtable", test_table_write)
        .option("usestagingtable", "false")
        .option("truncate_table", "true")
        .mode(SaveMode.Overwrite)
        .save()

      // Without staging table with check_table_existence_in_current_schema = "true", table exists
      tmpDF.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option(Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_WITH_FULLY_QUALIFIED_NAME, "true")
        .option("dbtable", test_table_write)
        .option("usestagingtable", "false")
        .option("truncate_table", "true")
        .mode(SaveMode.Overwrite)
        .save()
    } finally {
      jdbcUpdate(s"drop table if exists $test_table_write")
      jdbcUpdate(s"drop table if exists public.$test_table_write")
      thisConnectorOptionsNoTable -=
        Parameters.PARAM_INTERNAL_CHECK_TABLE_EXISTENCE_IN_CURRENT_SCHEMA_ONLY
    }
  }

  test("test write table name with schema/database") {
    val rowCount = 100
    val df = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("query", s"select seq4() from table(generator(rowcount => $rowCount))")
      .load()

    val testTableNames = Seq(
      s"${params.sfDatabase}.${params.sfSchema}.$test_table_write",
      s"${params.sfSchema}.$test_table_write",
      s"${params.sfDatabase}..$test_table_write")

    testTableNames.foreach{ name =>
      try {
        // write with stage table
        jdbcUpdate(s"drop table if exists $name")
        df.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option("dbtable", name)
          .mode(SaveMode.Overwrite)
          .save()
        assert(sparkSession.read
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option("dbtable", name)
          .load()
          .count() == rowCount)

        // write without stage table
        df.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option("dbtable", name)
          .option("usestagingtable", "false")
          .option("truncate_table", "true")
          .mode(SaveMode.Overwrite)
          .save()
        assert(sparkSession.read
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(thisConnectorOptionsNoTable)
          .option("dbtable", name)
          .load()
          .count() == rowCount)
      } finally {
        jdbcUpdate(s"drop table if exists $name")
      }
    }
  }

  // For the connection from spark connector, spark.snowflakedb.version must have been set.
  test("Test client_info is set correctly") {
    val clientInfo = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"select current_session_client_info()")
      .load()
      .collect()(0).getString(0)

    val objectMapper = new ObjectMapper()
    val jsonNode = objectMapper.readTree(clientInfo)
    assert(jsonNode.get(Utils.PROPERTY_NAME_OF_CONNECTOR_VERSION).asText().equals(Utils.VERSION))
  }

  // Refer to SNOW-346101 for details
  test("If client_info is not set, this case will fail") {
    val dateStringDF = sparkSession.sql(s"select '2014-04-25'")
    jdbcUpdate(s"create or replace table $test_table_write(d date)")

    // If client_inf is not set, this case fails. The error message is:
    // Date '2014-04-25' is not recognized
    dateStringDF.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .mode(SaveMode.Append)
      .save()
  }

  test("if timezone and timestamp output formats are sf_current, they are not set") {
    var sfOptions = thisConnectorOptionsNoTable
    sfOptions += (Parameters.PARAM_SF_TIMEZONE -> "sf_current")
    sfOptions += (Parameters.PARAM_TIMESTAMP_NTZ_OUTPUT_FORMAT -> "sf_current")
    sfOptions += (Parameters.PARAM_TIMESTAMP_LTZ_OUTPUT_FORMAT -> "sf_current")
    sfOptions += (Parameters.PARAM_TIMESTAMP_TZ_OUTPUT_FORMAT -> "sf_current")

    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptions)
      .option("query", s"select current_timestamp()")
      .load()
      .collect()
  }

  ignore("manual test for read with AWS VPCE stage") {
    val sfOptionsNoTable: Map[String, String] =
      replaceOption(
        thisConnectorOptionsNoTable,
        "S3_STAGE_VPCE_DNS_NAME",
        // This is not a real value. Need to change to use a valid one for manual test
        "*.vpce-XXXXXXXXXXX-YYYYYYYYYY.s3.us-west-2.vpce.amazonaws.com"
      )

    val query = "select seq4(), uniform(1, 10, random(12)) from" +
      " table(generator(rowcount => 100000)) order by 1"

    val rows = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsNoTable)
      .option("query", query)
      .load()
      .collect()
    assert(rows.length == 100000)
  }

  ignore("manual test for write with AWS VPCE stage") {
    val sfOptionsNoTable: Map[String, String] =
      replaceOption(
        thisConnectorOptionsNoTable,
        "S3_STAGE_VPCE_DNS_NAME",
        // This is not a real value. Need to change to use a valid one for manual test
        "*.vpce-XXXXXXXXXXX-YYYYYYYYYY.s3.us-west-2.vpce.amazonaws.com")

    def getRandomString(len: Int): String = {
      Random.alphanumeric take len mkString ""
    }

    val partitionCount = 1
    val rowCountPerPartition = 100
    val strValue = getRandomString(10)
    // Create RDD which generates 1 large partition
    val testRDD: RDD[Row] = sparkSession.sparkContext
      .parallelize(Seq[Int](), partitionCount)
      .mapPartitions { _ => {
        (1 to rowCountPerPartition).map { _ => {
          Row(strValue, strValue, strValue, strValue)
        }
        }.iterator
      }
      }
    val schema = StructType(
      List(
        StructField("str1", StringType),
        StructField("str2", StringType),
        StructField("str3", StringType),
        StructField("str4", StringType)
      )
    )

    // Convert RDD to DataFrame
    val df = sparkSession.createDataFrame(testRDD, schema)

    // Write to snowflake
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsNoTable)
      .option("dbtable", test_table_write)
      .mode(SaveMode.Overwrite)
      .save()
    assert(getRowCount(test_table_write) == partitionCount * rowCountPerPartition)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_number")
      jdbcUpdate(s"drop table if exists $test_table_string_binary")
      jdbcUpdate(s"drop table if exists $test_table_date_time")
      jdbcUpdate(s"drop table if exists $test_table_timestamp")
      jdbcUpdate(s"drop table if exists $test_table_large_result")
      jdbcUpdate(s"drop table if exists $test_table_inf")
      jdbcUpdate(s"drop table if exists $test_table_write")
      jdbcUpdate(s"drop table if exists $test_table_like")
    } finally {
      TestHook.disableTestHook()
      super.afterAll()
    }
  }
}
// scalastyle:on println

