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

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.Row

/**
  * Created by mzukowski on 8/12/16.
  */
class ParquetDataTypesIntegrationSuite extends IntegrationSuiteBase {

  private val test_table: String = s"test_table_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()
    java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"))
  }

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
      .option("sffiletype","parquet")
      .option("query", s"select * from $test_table order by i")
      .load()
    loadedDf.show()
    checkAnswer(loadedDf, expectedAnswer)
  }

  test("Test number"){
    jdbcUpdate(s"create or replace table $test_table(i int, v2 integer, v3 bigint, v4 smallint, v5 tinyint, v6 byteint, v7 number(10,2))")
    jdbcUpdate(s"insert into $test_table values(1,2,1234567890,4,5,6,12345678.99),(7,8,9,10,11,12,22345678.99)")
    checkTestTable(
      Seq(Row(1,2,1234567890,4,5,6,BigDecimal(12345678.99)),Row(7,8,9,10,11,12,BigDecimal(22345678.99)))
    )
  }
  test("Test floating number"){
    jdbcUpdate(s"create or replace table $test_table(i double, f float, dp double precision, r real)")
    jdbcUpdate(s"insert into $test_table values(1.1,2.2,3.3,4.4),(6.6,7.7,8.8,9.9)")
    checkTestTable(
      Seq(Row(1.1,2.2,3.3,4.4),Row(6.6,7.7,8.8,9.9))
    )
  }
  test("Test String"){
    jdbcUpdate(s"create or replace table $test_table(i varchar, v50 varchar(2), c char, c10 char(2), s string, s20 string(2), t text, t30 text(2))")
    jdbcUpdate(s"insert into $test_table values('a','aa','b','bb','c','cc','d','dd'), ('1','11','2','22','3','33','4','44')")
    checkTestTable(
      Seq(Row("a","aa","b","bb","c","cc","d","dd"),Row("1","11","2","22","3","33","4","44"))
    )
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

  test("Test DATE") {
    jdbcUpdate(s"create or replace table $test_table(i int, v date)")
    jdbcUpdate(s"insert into $test_table values(1, '1900-01-01'),(2, '2013-04-05'),(3, null)")
    checkTestTable(
      Seq(Row(1, TestUtils.toDate(1900,0,1)),
        Row(2, TestUtils.toDate(2013,3,5)),
        Row(3, null)))
  }



}