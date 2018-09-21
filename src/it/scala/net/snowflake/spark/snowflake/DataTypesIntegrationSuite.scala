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

import java.sql.Timestamp
import java.text.{DateFormat, SimpleDateFormat}
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}

import org.apache.spark.sql.{Row, SaveMode}
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

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
    val loadedDf = sqlContext.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"select * from $test_table order by i")
      .load()
    checkAnswer(loadedDf, expectedAnswer)
  }

  test("Test BOOLEAN") {
    jdbcUpdate(s"create or replace table $test_table(i int, v boolean)")
    jdbcUpdate(s"insert into $test_table values(1, false),(2, true),(3, null)")
    checkTestTable(
      Seq(Row(1, false), Row(2, true), Row(3, null)))
  }

  def timestampHelper(typeName: String, expectedHour1: Int, expectedHour2: Int): Unit = {
    jdbcUpdate(s"create or replace table $test_table(i int, v $typeName)")
    jdbcUpdate(s"insert into $test_table values(1, '2013-04-05 12:01:02 -02:00'),(2, '2013-04-05 18:01:02.123 +02:00'),(3, null)")
    checkTestTable(
      Seq(Row(1, TestUtils.toTimestamp(2013, 3, 5, expectedHour1, 1, 2, 0)),
        Row(2, TestUtils.toTimestamp(2013, 3, 5, expectedHour2, 1, 2, 123)),
        Row(3, null)))
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
    jdbcUpdate(s"insert into $test_table values(1, '1900-01-01'),(2, '2013-04-05'),(3, null)")
    checkTestTable(
      Seq(Row(1, TestUtils.toDate(1900,0,1)),
        Row(2, TestUtils.toDate(2013,3,5)),
        Row(3, null)))
  }

  test("insert timestamp into date") {
    jdbcUpdate(s"create or replace table $test_table(i date)")

    val dateFormat: DateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss.SSS")

    val time: Timestamp = new Timestamp(dateFormat.parse("28/10/1996 00:00:00.000").getTime)

    val df = sparkSession.createDataFrame(
      sc.parallelize(
        Seq(
          Row(time)
        )
      ),
      new StructType(
        Array(
          StructField("i", TimestampType, false)
        )
      )
    )


    df.write.format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table)
      .mode(SaveMode.Append)
      .save()

    checkTestTable(Seq(Row(new SimpleDateFormat("yyyy-MM-dd").parse("1996-10-28"))))


  }



}
