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

package com.databricks.spark.redshift

import java.sql.Timestamp
import java.text.{DecimalFormat, DateFormat, FieldPosition, ParsePosition, SimpleDateFormat}
import java.util.Date

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

/**
 * Data type conversions for Redshift unloaded data
 */
private[redshift] object Conversions {

  private val redshiftTimestampFormat: DateFormat = new DateFormat() {

    // Snowflake needs to use either of these formats
    private val PATTERN_BASE = "yyyy-MM-dd HH:mm:ss.SSS"
    private val PATTERN_TZ= "yyyy-MM-dd HH:mm:ss.SSS Z"

    private val formatBase= new SimpleDateFormat(PATTERN_BASE)
    private val formatTz = new SimpleDateFormat(PATTERN_TZ)

    override def format(
        date: Date,
        toAppendTo: StringBuffer,
        fieldPosition: FieldPosition): StringBuffer = {
      // Always export w/ timezone
      formatTz.format(date, toAppendTo, fieldPosition)
    }

    override def parse(source: String, pos: ParsePosition): Date = {
      val idx = pos.getIndex
      val errIdx = pos.getErrorIndex
      var res = formatBase.parse(source, pos)
      if (res == null) {
        // Restore pos
        pos.setIndex(idx)
        pos.setErrorIndex(errIdx)
        // Try again
        res = formatTz.parse(source, pos)
      }
      res
    }
  }

  private val redshiftDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * Parse a string exported from a Redshift TIMESTAMP column
   */
  private def parseTimestamp(s: String): Timestamp = {
    new Timestamp(redshiftTimestampFormat.parse(s).getTime)
  }

  /**
   * Parse a string exported from a Redshift DATE column
   */
  private def parseDate(s: String): java.sql.Date = {
    new java.sql.Date(redshiftDateFormat.parse(s).getTime)
  }

  def formatDate(d: Date): String = {
    redshiftTimestampFormat.format(d)
  }

  def formatTimestamp(t: Timestamp): String = {
    redshiftTimestampFormat.format(t)
  }

  /**
   * Parse a boolean using Redshift's UNLOAD bool syntax
   */
  private def parseBoolean(s: String): Boolean = {
    if (s == "t") true
    else if (s == "f") false
    else throw new IllegalArgumentException(s"Expected 't' or 'f' but got '$s'")
  }

  private[this] val redshiftDecimalFormat: DecimalFormat = new DecimalFormat()
  redshiftDecimalFormat.setParseBigDecimal(true)

  /**
   * Parse a decimal using Redshift's UNLOAD decimal syntax
   */
  def parseDecimal(s: String): java.math.BigDecimal = {
    redshiftDecimalFormat.parse(s).asInstanceOf[java.math.BigDecimal]
  }
  /**
   * Construct a Row from the given array of strings, retrieved from Redshift UNLOAD.
   * The schema will be used for type mappings.
   */
  private def convertRow(schema: StructType, fields: Array[String]): Row = {
    val converted = fields.zip(schema).map {
      case (data, field) =>
        if (data == null || data.isEmpty) null
        else field.dataType match {
          case ByteType => data.toByte
          case BooleanType => parseBoolean(data)
          case DateType => parseDate(data)
          case DoubleType => data.toDouble
          case FloatType => data.toFloat
          case dt: DecimalType => parseDecimal(data)
          case IntegerType => data.toInt
          case LongType => data.toLong
          case ShortType => data.toShort
          case StringType => data
          case TimestampType => parseTimestamp(data)
          case _ => data
        }
    }

    Row.fromSeq(converted)
  }

  /**
   * Return a function that will convert arrays of strings conforming to
   * the given schema to Row instances
   */
  def createRowConverter(schema: StructType): (Array[String]) => Row = {
    convertRow(schema, _: Array[String])
  }
}
