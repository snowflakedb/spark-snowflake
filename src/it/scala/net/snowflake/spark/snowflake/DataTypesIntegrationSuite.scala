/*
 * Copyright 2015-2016 Snowflake Computing
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

import java.sql.{SQLException, Timestamp}
import java.text.{DateFormat, SimpleDateFormat}
import org.apache.spark.sql.{Row, SaveMode}
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_SHORT_NAME
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType, TimestampType}

/**
  * Created by mzukowski on 8/12/16.
  */
class DataTypesIntegrationSuite extends IntegrationSuiteBase {

  private val test_table: String = s"test_table_$randomSuffix"

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table")
    } finally {
      super.afterAll()
    }
  }

  def checkTestTable(expectedAnswer: Seq[Row]): Unit = {
    val loadedDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"select * from $test_table order by i")
      .load()
    checkAnswer(loadedDf, expectedAnswer)
  }

  test("Test BOOLEAN") {
    jdbcUpdate(s"create or replace table $test_table(i int, v boolean)")
    jdbcUpdate(s"insert into $test_table values(1, false),(2, true),(3, null)")
    checkTestTable(Seq(Row(1, false), Row(2, true), Row(3, null)))
  }

  def timestampHelper(typeName: String,
                      expectedHour1: Int,
                      expectedHour2: Int): Unit = {
    jdbcUpdate(s"create or replace table $test_table(i int, v $typeName)")
    jdbcUpdate(
      s"""insert into $test_table values(1, '2013-04-05 12:01:02 -02:00'),
         |(2, '2013-04-05 18:01:02.123 +02:00'),(3, null)""".stripMargin.filter(_ >= ' ')
    )
    // scalastyle:off
    checkTestTable(
      Seq(
        Row(1, TestUtils.toTimestamp(2013, 3, 5, expectedHour1, 1, 2, 0)),
        Row(2, TestUtils.toTimestamp(2013, 3, 5, expectedHour2, 1, 2, 123)),
        Row(3, null)
      )
    )
    // scalastyle:on
  }

  test("Test TIMESTAMP_NTZ") {
    timestampHelper("timestamp_ntz", 12, 18)
  }
  test("Test TIMESTAMP_LTZ") {
    timestampHelper("timestamp_ltz", 14, 16)
  }
  test("Test TIMESTAMP_TZ") {
    timestampHelper("timestamp_tz", 14, 16)
  }

  test("Test DATE") {
    jdbcUpdate(s"create or replace table $test_table(i int, v date)")
    jdbcUpdate(
      s"insert into $test_table values(1, '1900-01-01'),(2, '2013-04-05'),(3, null)"
    )
    checkTestTable(
      Seq(
        Row(1, TestUtils.toDate(1900, 0, 1)),
        Row(2, TestUtils.toDate(2013, 3, 5)),
        Row(3, null)
      )
    )
  }

  test("change timestamp format") {
    jdbcUpdate(s"CREATE OR REPLACE TABLE $test_table (START_TIME TIMESTAMP)")

    val spark = sparkSession
    import spark.implicits._

    val df1 = Seq("2023-11-28 10:23:59.123456").toDF()
    df1.write.format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable).option("dbtable", test_table)
      .option(Parameters.PARAM_STRING_TIMESTAMP_FORMAT, "YYYY-MM-DD HH24:MI:SS.FF9")
      .mode(SaveMode.Append).save()

    val df = sparkSession.read.format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable).option("dbtable", test_table).load()
    assert(df.collect().head.getTimestamp(0).toString.startsWith("2023-11-28 10:23:59.123"))

    // it doesn't work if source dataframe has timestamp column
    val dateFormat: DateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS")
    val time: Timestamp =
      new Timestamp(dateFormat.parse("28/10/1996 00:00:00.000").getTime)

    val data = sc.parallelize(Seq(
      Row(time, "2023-11-28 10:23:59.123456")
    ))

    val schema = new StructType(
      Array(
        StructField("time", TimestampType),
        StructField("str", StringType)
      )
    )

    jdbcUpdate(s"CREATE OR REPLACE TABLE $test_table (START_TIME TIMESTAMP, END_TIME TIMESTAMP)")

    val df2 = sparkSession.createDataFrame(data, schema)

    assertThrows[SQLException]{
      df2.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
        .options(connectorOptionsNoTable).option("dbtable", test_table)
        .option(Parameters.PARAM_STRING_TIMESTAMP_FORMAT, "YYYY-MM-DD HH24:MI:SS.FF9")
        .mode(SaveMode.Append).save()
    }
  }

  test("insert timestamp into date") {
    jdbcUpdate(s"create or replace table $test_table(i date)")

    val spark = sparkSession
    import spark.implicits._

    val dateFormat: DateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS")

    val time: Timestamp =
      new Timestamp(dateFormat.parse("28/10/1996 00:00:00.000").getTime)

    val list = List(time)

    val df = list.toDF()

    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table)
      .mode(SaveMode.Append)
      .save()

    checkTestTable(
      Seq(Row(new SimpleDateFormat("yyyy-MM-dd").parse("1996-10-28")))
    )

  }

  test("filter on date column") {
    jdbcUpdate(
      s"""create or replace table $test_table ("id" int, "time" date)"""
    )
    jdbcUpdate(
      s"""insert into $test_table values(1, '2019-10-01'),(1, '2019-09-23'),
         |(1,'2019-01-01'),(1,'2020-01-01')""".stripMargin.filter(_ >= ' ')
    )

    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table)
      .option("keep_column_case", "on")
      .load()
      .filter(col("time") >= lit("2019-09-01").cast(DateType))
      .filter(col("time") <= lit("2019-11-01").cast(DateType))
      .select("id")
      .groupBy("id")
      .agg(count("*").alias("abc"))
      .collect()

    assert(result.length == 1)
    assert(result(0)(1) == 2)

    sparkSession.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table)
      .option("keep_column_case", "off")
      .load()
      .filter(col("\"time\"") <= lit("2019-09-01").cast(DateType))
      .filter(col("\"time\"") >= lit("2019-11-01").cast(DateType))
      .select("\"id\"")
      .groupBy("\"id\"")
      .agg(count("*").alias("abc"))
      .show()

    jdbcUpdate(s"drop table $test_table")
  }

  test("filter on timestamp column") {
    jdbcUpdate(
      s"""create or replace table $test_table ("id" int, "time" timestamp)"""
    )
    jdbcUpdate(
      s"insert into $test_table values(1, to_timestamp_ntz(1567036800000000, 6))"
    )
    jdbcUpdate(
      s"insert into $test_table values(1, to_timestamp_ntz(1567036700000000, 6))"
    )
    jdbcUpdate(
      s"insert into $test_table values(1, to_timestamp_ntz(1567036900000000, 6))"
    )
    jdbcUpdate(
      s"insert into $test_table values(1, to_timestamp_ntz(1467036800000000, 6))"
    )

    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table)
      .option("keep_column_case", "on")
      .load()
      .filter(col("time") >= lit("2019-07-29 00:00:00.000").cast(TimestampType))
      .filter(col("time") <= lit("2019-08-29 00:00:00.000").cast(TimestampType))
      .select("id")
      .groupBy("id")
      .agg(count("*").alias("abc"))
      .collect()

    assert(result.length == 1)
    assert(result(0)(1) == 2)

    sparkSession.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table)
      .option("keep_column_case", "off")
      .load()
      .filter(
        col("\"time\"") <= lit("2019-07-29 00:00:00.000").cast(TimestampType)
      )
      .filter(
        col("\"time\"") >= lit("2019-08-29 00:00:00.000").cast(TimestampType)
      )
      .select("\"id\"")
      .groupBy("\"id\"")
      .agg(count("*").alias("abc"))
      .show()

    jdbcUpdate(s"drop table $test_table")

  }

}
