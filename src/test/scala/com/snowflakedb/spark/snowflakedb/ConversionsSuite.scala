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

import java.sql.{Date, Timestamp}

import org.scalatest.FunSuite

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, StructField, BooleanType, StructType}

/**
 * Unit test for data type conversions
 */
class ConversionsSuite extends FunSuite {

  test("Data should be correctly converted") {
    val convertRow = Conversions.createRowConverter(TestUtils.testSchema)
    val doubleMin = Double.MinValue.toString
    val longMax = Long.MaxValue.toString
    // scalastyle:off
    val unicodeString = "Unicode是樂趣"
    // scalastyle:on

    val timestampString = "2014-03-01 00:00:01.123 -0800"
    val expectedTimestampMillis = TestUtils.toMillis(2014, 2, 1, 0, 0, 1, 123)

    val dateString = "2015-07-01"
    val expectedDateMillis = TestUtils.toMillis(2015, 6, 1, 0, 0, 0)

    def quote(str: String) = "\"" + str + "\""

    val convertedRow = convertRow(
      Array("1", quote(dateString), "123.45", doubleMin, "1.0", "42",
        longMax, "23", quote(unicodeString), quote(timestampString)))

    val expectedRow = Row(1.asInstanceOf[Byte], new Date(expectedDateMillis),
      new java.math.BigDecimal("123.45"), Double.MinValue, 1.0f, 42, Long.MaxValue, 23.toShort, unicodeString,
      new Timestamp(expectedTimestampMillis))

    assert(convertedRow.get(0) == expectedRow.get(0))
    assert(convertedRow.get(1) == expectedRow.get(1))
    assert(convertedRow.get(2) == expectedRow.get(2))
    assert(convertedRow.get(3) == expectedRow.get(3))
    assert(convertedRow.get(4) == expectedRow.get(4))
    assert(convertedRow.get(5) == expectedRow.get(5))
    assert(convertedRow.get(6) == expectedRow.get(6))
    assert(convertedRow.get(7) == expectedRow.get(7))
    assert(convertedRow.get(8) == expectedRow.get(8))
    assert(convertedRow.get(9) == expectedRow.get(9))

    assert(convertedRow == expectedRow)
  }

  test("Row conversion handles null values") {
    val convertRow = Conversions.createRowConverter(TestUtils.testSchema)
    val emptyRow = List.fill(TestUtils.testSchema.length)("").toArray[String]
    val nullsRow = List.fill(TestUtils.testSchema.length)(null).toArray[String]
    assert(convertRow(emptyRow) === Row(nullsRow: _*))
  }

  test("Dates are correctly converted") {
    val convertRow = Conversions.createRowConverter(StructType(Seq(StructField("a", DateType))))
    assert(convertRow(Array("\"2015-07-09\"")) === Row(TestUtils.toDate(2015, 6, 9)))
    assert(convertRow(Array("")) === Row(null))
    intercept[java.text.ParseException] {
      convertRow(Array("not-a-date"))
    }
  }
}
