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

import java.sql.Timestamp

import org.apache.parquet.example.data.Group
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import scala.reflect.ClassTag

/**
  * Parquet file conversions for Snowflake unloaded data
  */
private[snowflake] object ParquetConversions {


  /**
    * Return a function that will convert arrays of strings conforming to
    * the given schema to Row instances
    */
  def createRowConverter[T: ClassTag](
                                       schema: StructType): (Option[Group]) => T = {
    convertRow[T](schema, _: Option[Group])
  }

  /**
    * Construct a Row from the given array of strings, retrieved from Snowflake's UNLOAD.
    * The schema will be used for type mappings.
    */
  private def convertRow[T: ClassTag](schema: StructType,
                                      fields: Option[Group]): T = {

    val row = implicitly[ClassTag[Row]]
    val internalRow = implicitly[ClassTag[InternalRow]]
    val isInternalRow = implicitly[ClassTag[T]] match {
      case `row` => false
      case `internalRow` => true
      case _ =>
        throw new SnowflakeConnectorException("Wrong type for convertRow.")
    }

    var fieldCount = fields.get.getType.getFieldCount
    var types = schema.toArray

    val converted = (0 until fieldCount).map(index => {
      //multilayer data is not supported now, each field contains 0 (null) or 1 value
      if (fields.get.getFieldRepetitionCount(index) == 0) {
        if (types(index).nullable) null
        else throw new NullPointerException()
      }
      else {
        types(index).dataType match {
          case dt: DecimalType => {
            fields.get.getType.getType(index).asPrimitiveType().getPrimitiveTypeName match {
              case PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY => parseDecimal(BigDecimal(BigInt(fields.get.getBinary(index, 0).getBytes), dt.scale), isInternalRow)
              case PrimitiveTypeName.INT32 => parseDecimal(BigDecimal(fields.get.getInteger(index, 0), dt.scale), isInternalRow)
              case PrimitiveTypeName.INT64 => parseDecimal(BigDecimal(fields.get.getLong(index, 0), dt.scale), isInternalRow)
              case _ => {
                println(s"${fields.get.getType.getType(index).asPrimitiveType().getPrimitiveTypeName}")
                null
              }
            }
          }
          case DoubleType => fields.get.getDouble(index, 0)
          case StringType => {
            val str = fields.get.getString(index, 0)
            if (isInternalRow) UTF8String.fromString(str) else str
          }
          case BooleanType => fields.get.getBoolean(index, 0)
          case DateType => parseDate(fields.get.getInteger(index, 0), isInternalRow)
          case TimestampType => parseTimestamp(fields.get.getLong(index, 0), isInternalRow)
          case _ => {
            println(s"${fields.get.getType.getType(index).toString}===>${types(index).dataType}")
            null
          }
          //removed ByteType, FloatType, IntegerType, LongType, ShortType
        }
      }

    })
    if (isInternalRow) {
      InternalRow.fromSeq(converted).asInstanceOf[T]
    } else {
      Row.fromSeq(converted).asInstanceOf[T]
    }

  }

  /**
    * Parse a string exported from a Snowflake TIMESTAMP column
    */
  private def parseTimestamp(input: Long, isInternalRow: Boolean): Any = {
    val res = new Timestamp(input)
    if (isInternalRow) DateTimeUtils.fromJavaTimestamp(res)
    else res
  }

  /**
    * Parse a string exported from a Snowflake DATE column
    */
  private def parseDate(input: Int, isInternalRow: Boolean): Any = {
    val d = new java.sql.Date(1000l * 3600 * 24 * input)
    if (isInternalRow) DateTimeUtils.fromJavaDate(d)
    else d
  }

  /**
    * Parse a decimal using Snowflake's UNLOAD decimal syntax.
    */
  def parseDecimal(input: BigDecimal, isInternalRow: Boolean): Any = {
    if (isInternalRow) Decimal(input)
    else input
  }
}