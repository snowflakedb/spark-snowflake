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
import java.util.Calendar

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.util.Random

/**
  * Helpers for Snowflake tests that require common mocking
  */
object TestUtils {

  /**
    * Simple schema that includes all data types we support
    */
  val testSchema: StructType = {
    // These column names need to be lowercase; see #51
    StructType(
      Seq(
        StructField("testbyte", ByteType),
        StructField("testdate", DateType),
        StructField("testdec152", DecimalType(15, 2)),
        StructField("testdouble", DoubleType),
        StructField("testfloat", FloatType),
        StructField("testint", IntegerType),
        StructField("testlong", LongType),
        StructField("testshort", ShortType),
        StructField("teststring", StringType),
        StructField("testtimestamp", TimestampType)
      )
    )
  }

  // scalastyle:off
  /**
    * Expected parsed output corresponding to snowflake_unload_data.txt
    */
  val expectedData: Seq[Row] = Seq(
    Row(
      1.toByte,
      TestUtils.toDate(2015, 6, 1),
      BigDecimal(1234567890123.45),
      1234152.12312498,
      1.0f,
      42,
      1239012341823719L,
      23.toShort,
      "Unicode's樂趣",
      TestUtils.toTimestamp(2015, 6, 1, 0, 0, 0, 1)
    ),
    Row(
      2.toByte,
      TestUtils.toDate(1960, 0, 2),
      BigDecimal(1.01),
      2.0,
      3.0f,
      4,
      5L,
      6.toShort,
      "\"",
      TestUtils.toTimestamp(2015, 6, 2, 12, 34, 56, 789)
    ),
    Row(
      3.toByte,
      TestUtils.toDate(2999, 11, 31),
      BigDecimal(-1.01),
      -2.0,
      -3.0f,
      -4,
      -5L,
      -6.toShort,
      "\\'\"|",
      TestUtils.toTimestamp(1950, 11, 31, 17, 0, 0, 1)
    ),
    Row(List.fill(10)(null): _*)
  )
  // scalastyle:on

  /**
    * The same as `expectedData`, but with dates and timestamps converted into string format.
    * See #39 for context.
    */
  val expectedDataWithConvertedTimesAndDates: Seq[Row] = expectedData.map {
    row =>
      Row.fromSeq(row.toSeq.map {
        case t: Timestamp => Conversions.formatTimestamp(t)
        case d: Date => Conversions.formatDate(d)
        case other => other
      })
  }

  /**
    * Convert date components to a millisecond timestamp
    */
  def toMillis(year: Int,
               zeroBasedMonth: Int,
               date: Int,
               hour: Int,
               minutes: Int,
               seconds: Int,
               millis: Int = 0): Long = {
    val calendar = Calendar.getInstance()
    calendar.set(year, zeroBasedMonth, date, hour, minutes, seconds)
    calendar.set(Calendar.MILLISECOND, millis)
    calendar.getTime.getTime
  }

  /**
    * Convert date components to a SQL Timestamp
    */
  def toTimestamp(year: Int,
                  zeroBasedMonth: Int,
                  date: Int,
                  hour: Int,
                  minutes: Int,
                  seconds: Int,
                  millis: Int): Timestamp = {
    new Timestamp(
      toMillis(year, zeroBasedMonth, date, hour, minutes, seconds, millis)
    )
  }

  /**
    * Convert date components to a SQL [[Date]].
    */
  def toDate(year: Int, zeroBasedMonth: Int, date: Int): Date = {
    new Date(toTimestamp(year, zeroBasedMonth, date, 0, 0, 0, 0).getTime)
  }

  /** This takes a query in the shape produced by QueryBuilder and
    * performs the necessary indentation for pretty printing.
    *
    * @note Warning: This is a hacky implementation that isn't very 'functional' at all.
    * In fact, it's quite imperative.
    *
    * This is useful for logging and debugging.
    */
  def prettyPrint(query: String): String = {
    val opener = "\\(SELECT"
    val closer = "\\) AS \\\"SUBQUERY_[0-9]{1,10}\\\""

    val breakPoints = "(" + "(?=" + opener + ")" + "|" + "(?=" + closer + ")" +
      "|" + "(?<=" + closer + ")" + ")"

    var remainder = query
    var indent = 0

    val str = new StringBuilder
    var inSuffix: Boolean = false

    while (remainder.length > 0) {
      val prefix = "\n" + "\t" * indent
      val parts = remainder.split(breakPoints, 2)
      str.append(prefix + parts.head)

      if (parts.length >= 2 && parts.last.length > 0) {
        val n: Char = parts.last.head

        if (n == '(') {
          indent += 1
        } else {

          if (!inSuffix) {
            indent -= 1
            inSuffix = true
          }

          if (n == ')') {
            inSuffix = false
          }
        }
        remainder = parts.last
      } else remainder = ""
    }

    str.toString()
  }

  /**
   * Generate random suffixes; useful for use
   * in generating collision-safe identifiers in tests.
   */
  def randomSuffix: String = Math.abs(Random.nextLong()).toString
}
