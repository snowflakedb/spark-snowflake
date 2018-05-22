/*
 * Copyright 2015-2018 Snowflake Computing
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

import java.sql.Timestamp
import java.text._
import java.util.Date

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.reflect.ClassTag

/**
  * Data type conversions for Snowflake unloaded data
  */
private[snowflake] object Conversions {

  // For TZ and LTZ, Snowflake serializes with timezone
  // Note - we use a pattern with timezone in the beginning, to make sure
  // parsing with PATTERN_NTZ fails for PATTERN_TZLTZ strings.
  // Note - for JDK 1.6, we use Z ipo XX for SimpleDateFormat
  private val PATTERN_TZLTZ =
    if (System.getProperty("java.version").startsWith("1.6."))
      "Z yyyy-MM-dd HH:mm:ss.SSS"
    else
      "XX yyyy-MM-dd HH:mm:ss.SSS"

  // For NTZ, Snowflake serializes w/o timezone
  private val PATTERN_NTZ = "yyyy-MM-dd HH:mm:ss.SSS"

  // For DATE, simple ISO format
  private val PATTERN_DATE = "yyyy-MM-dd"

  private val snowflakeTimestampFormat: DateFormat = new DateFormat() {

    // Thread local SimpleDateFormat for parsing/formatting
    private var formatTzLtz = new ThreadLocal[SimpleDateFormat] {
      override protected def initialValue: SimpleDateFormat =
        new SimpleDateFormat(PATTERN_TZLTZ)
    }
    private var formatNtz = new ThreadLocal[SimpleDateFormat] {
      override protected def initialValue: SimpleDateFormat =
        new SimpleDateFormat(PATTERN_NTZ)
    }

    override def format(date: Date,
                        toAppendTo: StringBuffer,
                        fieldPosition: FieldPosition): StringBuffer = {
      // Always export w/ timezone
      formatTzLtz.get().format(date, toAppendTo, fieldPosition)
    }

    override def parse(source: String, pos: ParsePosition): Date = {
      val idx    = pos.getIndex
      val errIdx = pos.getErrorIndex
      // First try with the NTZ format, as that's our default TIMESTAMP type
      var res = formatNtz.get().parse(source, pos)
      if (res == null) {
        // Restore pos to parse from the same place
        pos.setIndex(idx)
        pos.setErrorIndex(errIdx)
        // Try again, using the format with a timezone
        formatTzLtz.get().parse(source, pos)
      } else {
        res
      }

    }
  }

  // Thread local SimpleDateFormat for parsing/formatting
  private var snowflakeDateFormat = new ThreadLocal[SimpleDateFormat] {
    override protected def initialValue: SimpleDateFormat =
      new SimpleDateFormat(PATTERN_DATE)
  }
  // Thread local DecimalFormat for parsing
  private var snowflakeDecimalFormat = new ThreadLocal[DecimalFormat] {
    override protected def initialValue: DecimalFormat = {
      var df = new DecimalFormat()
      df.setParseBigDecimal(true)
      df
    }
  }

  def formatDate(d: Date): String = {
    snowflakeDateFormat.get().format(d)
  }

  def formatTimestamp(t: Timestamp): String = {
    snowflakeTimestampFormat.format(t)
  }

  // All strings are converted into double-quoted strings, with
  // quote inside converted to double quotes
  def formatString(s: String): String = {
    "\"" + s.replace("\"", "\"\"") + "\""
  }

  def formatAny(v: Any): String = {
    if (v == null) ""
    else v.toString
  }

  /**
    * Return a function that will convert arrays of strings conforming to
    * the given schema to Row instances
    */
  def createRowConverter[T: ClassTag](
      schema: StructType): (Array[String]) => T = {
    convertRow[T](schema, _: Array[String])
  }

  /**
    * Construct a Row from the given array of strings, retrieved from Snowflake's UNLOAD.
    * The schema will be used for type mappings.
    */
  private def convertRow[T: ClassTag](schema: StructType,
                                      fields: Array[String]): T = {

    val row         = implicitly[ClassTag[Row]]
    val internalRow = implicitly[ClassTag[InternalRow]]

    val isInternalRow: Boolean = implicitly[ClassTag[T]] match {
      case `row`         => false
      case `internalRow` => true
      case _ =>
        throw new SnowflakeConnectorException("Wrong type for convertRow.")
    }

    val converted = fields.zip(schema).map {
      case (input, field) =>
        // If the column is not nullable, use an empty string instead.
        val data = if (input == null && !field.nullable) "" else input
        // Input values that are null produce nulls
        if (data == null) {
          null
        } else
          field.dataType match {
            case ByteType        => data.toByte
            case BooleanType     => parseBoolean(data)
            case DateType        => parseDate(data, isInternalRow)
            case DoubleType      => data.toDouble
            case FloatType       => data.toFloat
            case dt: DecimalType => parseDecimal(data, isInternalRow)
            case IntegerType     => data.toInt
            case LongType        => data.toLong
            case ShortType       => data.toShort
            case StringType =>
              if (isInternalRow) UTF8String.fromString(data) else data
            case TimestampType => parseTimestamp(data, isInternalRow)
            case _             => data
          }
    }

    if (isInternalRow) {
      InternalRow.fromSeq(converted).asInstanceOf[T]
    } else
      Row.fromSeq(converted).asInstanceOf[T]
  }

  /**
    * Parse a string exported from a Snowflake TIMESTAMP column
    */
  private def parseTimestamp(s: String, isInternalRow: Boolean): Any = {
    val res = new Timestamp(snowflakeTimestampFormat.parse(s).getTime)
    if (isInternalRow) DateTimeUtils.fromJavaTimestamp(res)
    else res
  }

  /**
    * Parse a string exported from a Snowflake DATE column
    */
  private def parseDate(s: String, isInternalRow: Boolean): Any = {
    val d = new java.sql.Date(snowflakeDateFormat.get().parse(s).getTime)
    if (isInternalRow) DateTimeUtils.fromJavaDate(d)
    else d
  }

  private def parseBoolean(s: String): Boolean = {
    if (s == "true") true
    else if (s == "false") false
    else
      throw new IllegalArgumentException(
        s"Expected 'true' or 'false' but got '$s'")
  }

  /**
    * Parse a decimal using Snowflake's UNLOAD decimal syntax.
    */
  def parseDecimal(s: String, isInternalRow: Boolean): Any = {
    val res =
      snowflakeDecimalFormat.get().parse(s).asInstanceOf[java.math.BigDecimal]
    if (isInternalRow) Decimal(res)
    else res
  }
}
