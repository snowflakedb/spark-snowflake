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

import scala.collection.JavaConverters._
import java.sql.{Date, Timestamp}
import net.snowflake.client.jdbc.internal.apache.commons.codec.binary.Base64
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.snowflakeStyleSchema
import net.snowflake.spark.snowflake.Parameters.{MergedParameters, mergeParameters}
import net.snowflake.spark.snowflake.SparkConnectorContext.getClass
import net.snowflake.spark.snowflake.Utils.ensureUnquoted
import net.snowflake.spark.snowflake.io.SupportedFormat
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.RebaseDateTime
import org.slf4j.LoggerFactory

import java.nio.ByteBuffer
import java.time.{LocalDate, ZoneId, ZoneOffset}
import java.util.concurrent.TimeUnit
import scala.collection.mutable

/**
 * Functions to write data to Snowflake.
 *
 * At a high level, writing data back to Snowflake involves the following steps:
 *
 *   - Use an RDD to save DataFrame content as strings, using customized
 *     formatting functions from Conversions

 *   - Use JDBC to issue any CREATE TABLE commands, if required.
 *
 *   - If there is data to be written (i.e. not all partitions were empty),
 *     copy all the files sharing the prefix we exported to into Snowflake.
 *
 *     This is done by issuing a COPY command over JDBC that
 *     instructs Snowflake to load the CSV data into the appropriate table.
 *
 *     If the Overwrite SaveMode is being used, then by default the data
 *     will be loaded into a temporary staging table,
 *     which later will atomically replace the original table using SWAP.
 */
private[snowflake] class SnowflakeWriter(jdbcWrapper: JDBCWrapper) {

  def save(sqlContext: SQLContext,
           data: DataFrame,
           saveMode: SaveMode,
           params: MergedParameters): Unit = {
    val format: SupportedFormat = {
      if (params.useParquetInWrite()) {
        SupportedFormat.PARQUET
      } else if (Utils.containVariant(data.schema)){
        SupportedFormat.JSON
      }
      else {
        SupportedFormat.CSV
      }
    }

    if (params.columnMap.isEmpty && params.columnMapping == "name") {
      val conn = jdbcWrapper.getConnector(params)
      try {
        val toSchema: Option[StructType] = Some(
          Utils.removeQuote(
            jdbcWrapper.resolveTable(conn, params.table.get.name, params)
          )
        )
        params.setColumnMap(Option(data.schema), toSchema)
      } finally conn.close()
    } else if (params.columnMap.isDefined){
      val conn = jdbcWrapper.getConnector(params)
      try {
        val toSchema = Utils.removeQuote(
            jdbcWrapper.resolveTable(conn, params.table.get.name, params)
          )
        params.columnMap match {
          case Some(map) =>
            map.values.foreach{
              value =>
                if (!toSchema.fieldNames.contains(value)){
                  throw new IllegalArgumentException(
                    s"Column with name $value does not match any column in snowflake table")
                }
            }
        }
      } finally conn.close()
    }

    val output: DataFrame = removeUselessColumns(data, params)
    val strRDD = dataFrameToRDD(sqlContext, output, params, format)
    io.writeRDD(sqlContext, params, strRDD, output.schema, saveMode, format)
  }

  /**
   * function that map spark style column name to snowflake style column name
  */
  def mapColumn(schema: StructType,
                params: MergedParameters
               ): StructType = {
    params.columnMap match {
      case Some(map) =>
        StructType(schema.map {
          sparkField =>
            StructField(
              map.getOrElse(sparkField.name, sparkField.name),
              sparkField.dataType match {
                case datatype: StructType =>
                  mapColumn(datatype, params)
                case _ =>
                  sparkField.dataType
              },
              sparkField.nullable,
              sparkField.metadata
            )
        })
      case _ =>
        val newSchema = snowflakeStyleSchema(schema, params)
        StructType(newSchema.map {
          sparkField =>
            StructField(
              // unquote field name because avro does not allow quote in field name
              ensureUnquoted(sparkField.name),
              sparkField.dataType match {
                case datatype: StructType =>
                  mapColumn(datatype, params)
                case _ =>
                  sparkField.dataType
              },
              sparkField.nullable,
              sparkField.metadata
            )
        })
    }
  }



  def dataFrameToRDD(sqlContext: SQLContext,
                     data: DataFrame,
                     params: MergedParameters,
                     format: SupportedFormat): RDD[Any] = {
    val spark = sqlContext.sparkSession
    import spark.implicits._ // for toJson conversion

    format match {
      case SupportedFormat.PARQUET =>
        val snowflakeStyleSchema = mapColumn(data.schema, params)
        val schema = io.ParquetUtils.convertStructToAvro(snowflakeStyleSchema)
        data.rdd.map (row => {
            def rowToAvroRecord(row: Row,
                                schema: Schema,
                                snowflakeStyleSchema: StructType,
                                params: MergedParameters): GenericData.Record = {
              val record = new GenericData.Record(schema)
              row.toSeq.zip(snowflakeStyleSchema.names).foreach {
                case (row: Row, name) =>
                  record.put(name,
                    rowToAvroRecord(
                      row,
                      schema.getField(name).schema().getTypes.get(0),
                      snowflakeStyleSchema(name).dataType.asInstanceOf[StructType],
                      params
                    ))
                case (map: scala.collection.immutable.Map[Any, Any], name) =>
                  record.put(name, map.asJava)
                case (str: String, name) =>
                  record.put(name, if (params.trimSpace) str.trim else str)
                case (arr: mutable.WrappedArray[Any], name) =>
                  record.put(name, arr.toArray)
                case (decimal: java.math.BigDecimal, name) =>
                  record.put(name, ByteBuffer.wrap(decimal.unscaledValue().toByteArray))
                case (timestamp: java.sql.Timestamp, name) =>
                  record.put(name, TimeUnit.MICROSECONDS.toSeconds(
                    RebaseDateTime.rebaseJulianToGregorianMicros(
                      TimeUnit.MILLISECONDS.toMicros(timestamp.getTime))))
                case (date: java.sql.Date, name) =>
                  record.put(name, RebaseDateTime.
                    rebaseJulianToGregorianDays(TimeUnit.MILLISECONDS.toDays(date.getTime).toInt))
                case (date: java.time.LocalDateTime, name) =>
                  record.put(name, date.toEpochSecond(ZoneOffset.UTC))
                case (value, name) => record.put(name, value)
              }
              record
            }
          rowToAvroRecord(row, schema, snowflakeStyleSchema, params)
        })
      case SupportedFormat.CSV =>
        val conversionFunction = genConversionFunctions(data.schema, params)
        data.rdd.map(row => {
          row.toSeq
            .zip(conversionFunction)
            .map {
              case (element, func) => func(element)
            }
            .mkString("|")
        })
      case SupportedFormat.JSON =>
        // convert binary (Array of Byte) to encoded base64 String before COPY
        val newSchema: StructType = prepareSchemaForJson(data.schema)
        val conversionsFunction = genConversionFunctionsForJson(data.schema, params)
        val newData: RDD[Row] = data.rdd.map(row => {
          Row.fromSeq(
            row.toSeq
              .zip(conversionsFunction)
              .map {
                case (element, func) => func(element)
              }
          )
        })
        spark.createDataFrame(newData, newSchema).toJSON.map(_.toString).rdd.asInstanceOf[RDD[Any]]
    }
  }

  private def prepareSchemaForJson(schema: StructType): StructType =
    StructType.apply(schema.map{
      // Binary types will be converted to String type before COPY
      case field: StructField if field.dataType == BinaryType =>
        StructField(field.name, StringType, field.nullable, field.metadata)
      case other => other
    })


  private def genConversionFunctionsForJson(schema: StructType,
                                            params: MergedParameters): Array[Any => Any] =
    schema.fields.map(field =>
      field.dataType match {
        case StringType =>
          (v: Any) =>
            if (params.trimSpace) {
              v.toString.trim
            } else v
        case BinaryType =>
          (v: Any) =>
            v match {
              case null => ""
              case bytes: Array[Byte] => Base64.encodeBase64String(bytes)
            }
        case _ => (input: Any) => input
      }
    )

  /**
   * If column mapping is enable, remove all useless columns from the input DataFrame
   */
  private def removeUselessColumns(dataFrame: DataFrame,
                                   params: MergedParameters): DataFrame =
    params.columnMap match {
      case Some(map) =>
        // Enclose column name with backtick(`) if dot(.) exists in column name
        val names = map.keys.toSeq.map(name =>
          if (name.contains(".")) {
            s"`$name`"
          } else {
            name
          })
        try {
          dataFrame.select(names.head, names.tail: _*)
        } catch {
          case e: AnalysisException =>
            throw new IllegalArgumentException(
              "Incorrect column name when column mapping: " + e.toString
            )
        }
      case _ => dataFrame
    }

  // Prepare a set of conversion functions, based on the schema
  def genConversionFunctions(schema: StructType, params: MergedParameters): Array[Any => Any] =
    schema.fields.map { field =>
      field.dataType match {
        case DateType =>
          (v: Any) =>
            v match {
              case null => ""
              case t: Timestamp => Conversions.formatTimestamp(t)
              case d: Date => Conversions.formatDate(d)
            }
        case TimestampType =>
          (v: Any) =>
          {
            if (v == null) ""
            else Conversions.formatTimestamp(v.asInstanceOf[Timestamp])
          }
        case StringType =>
          (v: Any) =>
          {
            if (v == null) ""
            else {
              val trimmed = if (params.trimSpace) {
                v.toString.trim
              } else v
              Conversions.formatString(trimmed.asInstanceOf[String])
            }
          }
        case BinaryType =>
          (v: Any) =>
            v match {
              case null => ""
              case bytes: Array[Byte] => Base64.encodeBase64String(bytes)
            }
        case _ =>
          (v: Any) =>
            Conversions.formatAny(v)
      }
    }
}

object DefaultSnowflakeWriter extends SnowflakeWriter(DefaultJDBCWrapper)