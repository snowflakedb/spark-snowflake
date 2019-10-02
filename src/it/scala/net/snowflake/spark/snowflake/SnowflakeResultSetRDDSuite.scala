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

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.Row

class SnowflakeResultSetRDDSuite extends IntegrationSuiteBase {
  // Add some options for default for testing.
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()

  private val test_table_number: String = s"test_table_number_$randomSuffix"
  private val test_table_string_binary: String = s"test_table_string_binary_$randomSuffix"
  private val test_table_date_time: String = s"test_table_date_time_$randomSuffix"
  private val test_table_timestamp: String = s"test_table_timestamp_$randomSuffix"
  private val test_table_large_result: String = s"test_table_large_result_$randomSuffix"

  private lazy val test_table_number_rows = Seq(
    Row(
      BigDecimal(1), true
    , BigDecimal(100), BigDecimal(1.2), BigDecimal(10.2)
    , BigDecimal(300), BigDecimal(30.2), BigDecimal(1.002)
    , BigDecimal(1234567890), BigDecimal(1.01234), BigDecimal(1.01234567)
    , BigDecimal("123456789012345"), BigDecimal("123456789.01234"), BigDecimal("0.012345678901234")
    , BigDecimal("1234567890123456789012345"), BigDecimal("12345678901234567890.01234"), BigDecimal("1234567890.012345678901234")
    , 1.1, 1234567890.0123, 2.2,  0.123456789
  ),
    Row(
    2, null
    , null , null, null
    , null , null, null
    , null , null, null
    , null , null, null
    , null , null, null
    , null , null, null, null
  ),
    Row (
      BigDecimal(3), false
    , BigDecimal(-100), BigDecimal(-1.2), BigDecimal(-10.2)
    , BigDecimal(-300), BigDecimal(-30.2), BigDecimal(-1.002)
    , BigDecimal(-1234567890), BigDecimal(-1.01234), BigDecimal(-1.01234567)
    , BigDecimal("-123456789012345"), BigDecimal("-123456789.01234"), BigDecimal("-0.012345678901234")
    , BigDecimal("-1234567890123456789012345"), BigDecimal("-12345678901234567890.01234"), BigDecimal("-1234567890.012345678901234")
    , -1.1, -1234567890.0123, -2.2,  -0.123456789
  ))

  private def setupNumberTable() : Unit = {
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
  }

  private lazy val test_table_string_binary_rows = Seq(
    Row(BigDecimal(1), "varchar", "v50", "c", "c10", "s", "s20", "t", "t30",
      "binary".getBytes(), "binary100".getBytes(), "varbinary".getBytes()),
    Row(BigDecimal(2), null , null, null , null, null , null, null , null, null , null, null),
    Row(BigDecimal(3), "varchar", "v50", "c", "c10", "s", "s20", "t", "t30",
      "binary".getBytes(), "binary100".getBytes(), "varbinary".getBytes())
  )

  private def setupStringBinaryTable() : Unit = {
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
  }


  // Snowflake may output "23:59:59" as "23:59:59.",
  // If that is changed in the future, the expected result needs to be updated.
  private lazy val test_table_date_time_rows = Seq(
    Row(
      BigDecimal(0), Date.valueOf("9999-12-31")
      , "23:59:59.", "23:59:59.9", "23:59:59.99"
      , "23:59:59.999", "23:59:59.9999", "23:59:59.99999", "23:59:59.999999"
      , "23:59:59.9999999", "23:59:59.99999999", "23:59:59.999999999"),
    Row (BigDecimal(1), Date.valueOf("1970-01-01")
      , "00:00:00.", "00:00:00.0", "00:00:00.00",
      "00:00:00.000", "00:00:00.0000", "00:00:00.00000", "00:00:00.000000"
      , "00:00:00.0000000", "00:00:00.00000000", "00:00:00.000000000"),
    Row(BigDecimal(2), Date.valueOf("0000-01-01")
      , "00:00:01.", "00:00:00.1", "00:00:00.01"
      , "00:00:00.001", "00:00:00.0001", "00:00:00.00001", "00:00:00.000001"
      , "00:00:00.0000001", "00:00:00.00000001", "00:00:00.000000001")
  )

  private def setupDateTimeTable() : Unit = {
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
              | 2, '0000-01-01', '00:00:01', '00:00:00.1', '00:00:00.01', '00:00:00.001'
              | , '00:00:00.0001', '00:00:00.00001', '00:00:00.000001'
              | , '00:00:00.0000001', '00:00:00.00000001', '00:00:00.000000001'
           )""".stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_date_time")
      .load()

    tmpdf.createOrReplaceTempView("test_table_date_time")
  }

  private lazy val test_table_timestamp_rows = Seq(
    Row(BigDecimal(1),
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
  ), Row(BigDecimal(2),
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
    ))

  private def setupTimestampTable() : Unit = {
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
  }

  private val largeStringValue =
    s"""spark_connecor_test_large_result_1234567890
       |spark_connecor_test_large_result_1234567890
       |spark_connecor_test_large_result_1234567890
       |spark_connecor_test_large_result_1234567890
       |spark_connecor_test_large_result_1234567890
       |spark_connecor_test_large_result_1234567890
       |spark_connecor_test_large_result_1234567890
       |spark_connecor_test_large_result_1234567890
       |""".stripMargin.filter(_ >= ' ')

  private def setupLargeResultTable() : Unit = {
    jdbcUpdate(s"""create or replace table $test_table_large_result (
                  | int_c int, c_string string(1024) )""".stripMargin)

    jdbcUpdate(s"""insert into $test_table_large_result select
                  | seq4(), '$largeStringValue'
                  | from table(generator(rowcount => 100000))""".stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
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

    connectorOptionsNoTable.foreach(tup => { thisConnectorOptionsNoTable += tup })

    // Setup special options for this test
    thisConnectorOptionsNoTable += ("use_copy_unload" -> "false")
    thisConnectorOptionsNoTable += ("partition_size_in_mb" -> "50")
    thisConnectorOptionsNoTable += ("query_result_format" -> "arrow")
    thisConnectorOptionsNoTable += ("time_output_format" -> "HH24:MI:SS.FF")

    setupNumberTable()
    setupStringBinaryTable()
    setupDateTimeTable()
    setupTimestampTable()
    setupLargeResultTable()

    SnowflakeConnectorUtils.enablePushdownSession(sparkSession)
  }

  test("testNumber") {
    val result = sparkSession.sql("select * from test_table_number")

    testPushdown(
      s""" SELECT * FROM ( $test_table_number ) AS "SF_CONNECTOR_QUERY_ALIAS" """.stripMargin,
      result,
      test_table_number_rows)
  }

  test("testStringBinary") {
    // COPY UNLOAD can't be run because it doesn't support binary
    if (!params.useCopyUnload) {
      val result = sparkSession.sql("select * from test_table_string_binary")

      testPushdown(
        s""" SELECT * FROM ( $test_table_string_binary ) AS "SF_CONNECTOR_QUERY_ALIAS""""
          .stripMargin,
        result,
        test_table_string_binary_rows)
    }
  }

  test("testDateTime") {
    val result = sparkSession.sql("select * from test_table_date_time")

    testPushdown(
      s""" SELECT * FROM ( $test_table_date_time ) AS "SF_CONNECTOR_QUERY_ALIAS" """.stripMargin,
      result,
      test_table_date_time_rows)
  }

  test("testTimestamp") {
    // COPY UNLOAD can't be run because it only supports millisecond(0.001s).
    if (!params.useCopyUnload) {
      val result = sparkSession.sql("select * from test_table_timestamp")

      testPushdown(
        s""" SELECT * FROM ( $test_table_timestamp ) AS "SF_CONNECTOR_QUERY_ALIAS" """.stripMargin,
        result,
        test_table_timestamp_rows)
    }
  }

  test("testLargeResult") {
    val resultSet: Array[Row] = sparkSession.sql(
      "select * from test_table_large_result order by int_c").collect()

    var i : Int = 0
    while ( i < resultSet.length ) {
      val row = resultSet(i)
      assert(largeStringValue.equals(row(1))
        && (BigDecimal(i).compareTo(row(0).asInstanceOf[java.math.BigDecimal]) == 0))
      i += 1
    }
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
    } finally {
      super.afterAll()
      SnowflakeConnectorUtils.disablePushdownSession(sqlContext.sparkSession)
    }
  }
}
