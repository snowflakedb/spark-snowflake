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

import java.sql.{Date, Timestamp}

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.FunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
  * Unit test for data type conversions
  */
class ConversionsSuite extends FunSuite {

  val mapper = new ObjectMapper()

  test("Data should be correctly converted") {
    val convertRow = Conversions.createRowConverter[Row](TestUtils.testSchema)
    val doubleMin = Double.MinValue.toString
    val longMax = Long.MaxValue.toString
    // scalastyle:off
    val unicodeString = "Unicode是樂趣"
    // scalastyle:on

    val timestampString = "2014-03-01 00:00:01.123 -0800"
    val expectedTimestampMillis = TestUtils.toMillis(2014, 2, 1, 0, 0, 1, 123)

    val timestamp2String = "2014-03-01 00:00:01.123"
    val expectedTimestamp2Millis = TestUtils.toMillis(2014, 2, 1, 0, 0, 1, 123)

    val dateString = "2015-07-01"
    val expectedDateMillis = TestUtils.toMillis(2015, 6, 1, 0, 0, 0)

    var convertedRow = convertRow(
      Array(
        "1",
        dateString,
        "123.45",
        doubleMin,
        "1.0",
        "42",
        longMax,
        "23",
        unicodeString,
        timestampString
      )
    )

    var expectedRow = Row(
      1.asInstanceOf[Byte],
      new Date(expectedDateMillis),
      new java.math.BigDecimal("123.45"),
      Double.MinValue,
      1.0f,
      42,
      Long.MaxValue,
      23.toShort,
      unicodeString,
      new Timestamp(expectedTimestampMillis)
    )

    assert(convertedRow == expectedRow)

    convertedRow = convertRow(
      Array(
        "1",
        dateString,
        "123.45",
        doubleMin,
        "1.0",
        "42",
        longMax,
        "23",
        unicodeString,
        timestamp2String
      )
    )

    expectedRow = Row(
      1.asInstanceOf[Byte],
      new Date(expectedDateMillis),
      new java.math.BigDecimal("123.45"),
      Double.MinValue,
      1.0f,
      42,
      Long.MaxValue,
      23.toShort,
      unicodeString,
      new Timestamp(expectedTimestamp2Millis)
    )

    assert(convertedRow == expectedRow)
  }

  test("Row conversion handles null values") {
    val convertRow = Conversions.createRowConverter[Row](TestUtils.testSchema)
    val emptyRow = List.fill(TestUtils.testSchema.length)(null).toArray[String]
    val nullsRow = List.fill(TestUtils.testSchema.length)(null).toArray[String]
    assert(convertRow(emptyRow) === Row(nullsRow: _*))
  }

  test("Dates are correctly converted") {
    val convertRow = Conversions.createRowConverter[Row](
      StructType(Seq(StructField("a", DateType)))
    )
    assert(
      convertRow(Array("2015-07-09")) === Row(TestUtils.toDate(2015, 6, 9))
    )
    assert(convertRow(Array(null)) === Row(null))
    intercept[java.text.ParseException] {
      convertRow(Array("not-a-date"))
    }
  }

  test("json string to row conversion") {
    val str =
      s"""
         |{
         |"byte": 1,
         |"boolean": true,
         |"date": "2015-07-09",
         |"double": 1234.56,
         |"float": 678.98,
         |"decimal": 9999999999999999,
         |"integer": 1234,
         |"long": 123123123,
         |"short":123,
         |"string": "test string",
         |"timestamp": "2015-07-01 00:00:00.001",
         |"array":[1,2,3,4,5],
         |"map":{"a":1,"b":2,"c":3},
         |"structure":{"num":123,"str":"str1"}
         |}
       """.stripMargin

    val schema: StructType = new StructType(
      Array(
        StructField("byte", ByteType, nullable = false),
        StructField("boolean", BooleanType, nullable = false),
        StructField("date", DateType, nullable = false),
        StructField("double", DoubleType, nullable = false),
        StructField("float", FloatType, nullable = false),
        StructField("decimal", DecimalType(38, 0), nullable = false),
        StructField("integer", IntegerType, nullable = false),
        StructField("long", LongType, nullable = false),
        StructField("short", ShortType, nullable = false),
        StructField("string", StringType, nullable = false),
        StructField("timestamp", TimestampType, nullable = false),
        StructField("array", ArrayType(IntegerType), nullable = false),
        StructField("map", MapType(StringType, IntegerType), nullable = false),
        StructField(
          "structure",
          StructType(
            Array(
              StructField("num", IntegerType, nullable = false),
              StructField("str", StringType, nullable = false)
            )
          ),
          nullable = false
        )
      )
    )

    val result: Row =
      Conversions
        .jsonStringToRow[Row](mapper.readTree(str), schema)
        .asInstanceOf[Row]

    // scalastyle:off println
    println(result)
    // scalastyle:on println

    val expect =
      "[1,true,2015-07-09,1234.56,678.98,9999999999999999,1234,123123123,123,test string," +
        "2015-07-01 00:00:00.001,[1,2,3,4,5],keys: [a,b,c], values: [1,2,3],[123,str1]]"

    assert(expect == result.toString())
  }

  test("Data with micro-seconds and nano-seconds precision should be correctly converted"){
    val convertRow = Conversions.createRowConverter[Row](TestUtils.testSchema)
    val doubleMin = Double.MinValue.toString
    val longMax = Long.MaxValue.toString
    // scalastyle:off
    val unicodeString = "instacart是独角兽"
    // scalastyle:on

    val timestampString = "2014-03-01 00:00:01.123456"

    val expectedTimestampMicro: Timestamp = java.sql.Timestamp.valueOf(timestampString)

    val dateString = "2015-07-01"
    val expectedDate = TestUtils.toMillis(2015, 6, 1, 0, 0, 0)



    val timestampString2 = "2014-03-01 00:00:01.123456789"

    val expectedTimestampMicro2: Timestamp = java.sql.Timestamp.valueOf(timestampString2)

    val dateString2 = "2015-07-01"
    val expectedDate2 = TestUtils.toMillis(2015, 6, 1, 0, 0, 0)

    val convertedRow = convertRow(
      Array(
        "1",
        dateString,
        "123.45",
        doubleMin,
        "1.0",
        "42",
        longMax,
        "23",
        unicodeString,
        timestampString
      )
    )

    val expectedRow = Row(
      1.asInstanceOf[Byte],
      new Date(expectedDate),
      new java.math.BigDecimal("123.45"),
      Double.MinValue,
      1.0f,
      42,
      Long.MaxValue,
      23.toShort,
      unicodeString,
      expectedTimestampMicro
    )

    val convertedRow2 = convertRow(
      Array(
        "1",
        dateString2,
        "123.45",
        doubleMin,
        "1.0",
        "42",
        longMax,
        "23",
        unicodeString,
        timestampString2
      )
    )

    val expectedRow2 = Row(
      1.asInstanceOf[Byte],
      new Date(expectedDate2),
      new java.math.BigDecimal("123.45"),
      Double.MinValue,
      1.0f,
      42,
      Long.MaxValue,
      23.toShort,
      unicodeString,
      expectedTimestampMicro2
    )

    assert(convertedRow == expectedRow)
    assert(convertedRow2 == expectedRow2)
  }
}
