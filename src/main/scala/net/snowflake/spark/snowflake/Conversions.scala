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
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.{Date, TimeZone}

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, DateTimeUtils, GenericArrayData}
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
  // Because simpleDateFormat only support milliseconds,
  // we need to refactor this and handle nano seconds field separately
  private val PATTERN_TZLTZ =
    if (System.getProperty("java.version").startsWith("1.6.")) {
      "Z yyyy-MM-dd HH:mm:ss."
    } else {
      "XX yyyy-MM-dd HH:mm:ss."
    }

  // For NTZ, Snowflake serializes w/o timezone
  // and handle nano seconds field separately during parsing
  private val PATTERN_NTZ = "yyyy-MM-dd HH:mm:ss."

  // For DATE, simple ISO format
  private val PATTERN_DATE = "yyyy-MM-dd"

  private val snowflakeTimestampFormat: DateFormat = new DateFormat() {

    // Thread local SimpleDateFormat for parsing/formatting
    private val formatTzLtz = new ThreadLocal[SimpleDateFormat] {
      override protected def initialValue: SimpleDateFormat =
        new SimpleDateFormat(PATTERN_TZLTZ)
    }
    private val formatNtz = new ThreadLocal[SimpleDateFormat] {
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
      val idx = pos.getIndex
      val errIdx = pos.getErrorIndex
      // First try with the NTZ format, as that's our default TIMESTAMP type
      val res = formatNtz.get().parse(source, pos)
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

  // For TZ and LTZ, Snowflake serializes with timezone
  // Note - we use a pattern with timezone in the beginning, to make sure
  // parsing with Timestamp standard fails for PATTERN_TZLTZ strings.
  // For details of the format, check below URL
  // https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
  private val PATTERN_WRITE_TZLTZ = "XX yyyy-MM-dd HH:mm:ss.SSSSSSSSS"
  private val timestampWriteFormatter = DateTimeFormatter.ofPattern(PATTERN_WRITE_TZLTZ)

  // Thread local SimpleDateFormat for parsing/formatting
  private val snowflakeDateFormat = new ThreadLocal[SimpleDateFormat] {
    override protected def initialValue: SimpleDateFormat =
      new SimpleDateFormat(PATTERN_DATE)
  }
  // Thread local DecimalFormat for parsing
  private val snowflakeDecimalFormat = new ThreadLocal[DecimalFormat] {
    override protected def initialValue: DecimalFormat = {
      val df = new DecimalFormat()
      df.setParseBigDecimal(true)
      df
    }
  }

  def formatDate(d: Date): String = {
    snowflakeDateFormat.get().format(d)
  }

  def formatTimestamp(t: Timestamp): String = {
    // For writing to snowflake, time zone needs to be included
    // in the timestamp string. The spark default timezone is used.
    timestampWriteFormatter.format(
      ZonedDateTime.of(
        t.toLocalDateTime(),
        TimeZone.getDefault.toZoneId))
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
    schema: StructType
  ): Array[String] => T = {
    convertRow[T](schema, _: Array[String])
  }

  /**
    * Construct a Row from the given array of strings, retrieved from Snowflake's UNLOAD.
    * The schema will be used for type mappings.
    */
  private def convertRow[T: ClassTag](schema: StructType,
                                      fields: Array[String]): T = {

    val isIR: Boolean = isInternalRow[T]()

    val converted = fields.zip(schema).map {
      case (input, field) =>
        // If the column is not nullable, use an empty string instead.
        val data = if (input == null && !field.nullable) "" else input
        // Input values that are null produce nulls
        if (data == null) {
          null
        } else {
          field.dataType match {
            case ByteType => data.toByte
            case BooleanType => parseBoolean(data)
            case DateType => parseDate(data, isIR)
            case DoubleType => parseDouble(data)
            case FloatType => parseFloat(data)
            case _: DecimalType => parseDecimal(data, isIR)
            case IntegerType => data.toInt
            case LongType => data.toLong
            case ShortType => data.toShort
            case StringType =>
              if (isIR) UTF8String.fromString(data) else data
            case TimestampType => parseTimestamp(data, isIR)
            case _ => data
          }
        }
    }

    if (isIR) {
      InternalRow.fromSeq(converted).asInstanceOf[T]
    } else {
      Row.fromSeq(converted).asInstanceOf[T]
    }
  }

  /**
    * Parse a string exported from a Snowflake TIMESTAMP column
    */
  private def parseTimestamp(s: String, isInternalRow: Boolean): Any = {
    // Need to handle the nano seconds filed separately
    // valueOf only works with yyyy-[m]m-[d]d hh:mm:ss[.f...]
    // so we need to do a little parsing
    val timestampRegex = """\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3,9}""".r

    val parsedTS = timestampRegex.findFirstMatchIn(s) match {
      case Some(ts) => ts.toString()
      case None => throw new IllegalArgumentException(s"Malformed timestamp $s")
    }

    val ts = java.sql.Timestamp.valueOf(parsedTS)
    val nanoFraction = ts.getNanos

    val res = new Timestamp(snowflakeTimestampFormat.parse(s).getTime)

    res.setNanos(nanoFraction)
    // Since fromJavaTimestamp and spark only support microsecond
    // level precision so have to divide the nano field by 1000
    if (isInternalRow) (DateTimeUtils.fromJavaTimestamp(res) + nanoFraction/1000)
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
    else {
      throw new IllegalArgumentException(
        s"Expected 'true' or 'false' but got '$s'"
      )
    }
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

  /**
    * Parse a DOUBLE string as Double type
    */
  private def parseDouble(s: String): Double = {
    if (s.equalsIgnoreCase("inf")) {
      Double.PositiveInfinity
    } else if (s.equalsIgnoreCase("-inf")) {
      Double.NegativeInfinity
    } else if (s.equalsIgnoreCase("NaN")) {
      Double.NaN
    } else {
      s.toDouble
    }
  }

  /**
    * Parse a Float string as Float type
    */
  private def parseFloat(s: String): Float = {
    if (s.equalsIgnoreCase("inf")) {
      Float.PositiveInfinity
    } else if (s.equalsIgnoreCase("-inf")) {
      Float.NegativeInfinity
    } else if (s.equalsIgnoreCase("NaN")) {
      Float.NaN
    } else {
      s.toFloat
    }
  }
  /**
    * Convert a json String to Row
    */
  private[snowflake] def jsonStringToRow[T: ClassTag](
    data: JsonNode,
    dataType: DataType
  ): Any = {
    val isIR: Boolean = isInternalRow[T]()
    dataType match {
      case ByteType => data.asInt().toByte
      case BooleanType => data.asBoolean()
      case DateType => parseDate(data.asText(), isIR)
      case DoubleType => data.asDouble()
      case FloatType => data.asDouble().toFloat
      case DecimalType() => Decimal(data.decimalValue())
      case IntegerType => data.asInt()
      case LongType => data.asLong()
      case ShortType => data.shortValue()
      case StringType =>
        if (isIR) UTF8String.fromString(data.asText()) else data.asText()
      case TimestampType => parseTimestamp(data.asText(), isIR)
      case ArrayType(dt, _) =>
        val result = new Array[Any](data.size())
        (0 until data.size())
          .foreach(i => result(i) = jsonStringToRow[T](data.get(i), dt))
        new GenericArrayData(result)
      case StructType(fields) =>
        val converted = fields.map(field => {
          val value = data.findValue(field.name)
          if (value == null) {
            if (field.nullable) null
            else throw new IllegalArgumentException("data is not nullable")
          } else jsonStringToRow[T](value, field.dataType)
        })
        if (isIR) InternalRow.fromSeq(converted).asInstanceOf[T]
        else Row.fromSeq(converted).asInstanceOf[T]
      // String key only
      case MapType(_, dt, _) =>
        val keys = data.fieldNames()
        var keyList: List[UTF8String] = Nil
        var valueList: List[Any] = Nil
        while (keys.hasNext) {
          val key = keys.next()
          keyList = UTF8String.fromString(key) :: keyList
          valueList = jsonStringToRow[T](data.get(key), dt) :: valueList
        }
        new ArrayBasedMapData(
          new GenericArrayData(keyList.reverse.toArray),
          new GenericArrayData(valueList.reverse.toArray)
        )
      case _ =>
        if (isIR) UTF8String.fromString(data.toString) else data.toString
    }
  }

  private[snowflake] def isInternalRow[U: ClassTag](): Boolean = {
    val row = implicitly[ClassTag[Row]]
    val internalRow = implicitly[ClassTag[InternalRow]]

    implicitly[ClassTag[U]] match {
      case `row` => false
      case `internalRow` => true
      case _ => throw new SnowflakeConnectorException("Wrong type for convertRow.")
    }
  }
}
