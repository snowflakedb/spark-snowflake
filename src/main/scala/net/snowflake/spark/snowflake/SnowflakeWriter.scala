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

import java.sql.{Date, Timestamp}

import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.SupportedFormat
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._

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
private[snowflake] class SnowflakeWriter(
    jdbcWrapper: JDBCWrapper) {

  def save(
            sqlContext: SQLContext,
            data: DataFrame,
            saveMode: SaveMode,
            params: MergedParameters
          ): Unit = {

    def removeQuote(schema: StructType): StructType =
      new StructType(schema.map(field=>
        StructField(
          if(field.name.startsWith("\"") && field.name.endsWith("\""))
            field.name.substring(1, field.name.length - 1)
          else field.name,
          field.dataType,field.nullable)).toArray)

    val format: SupportedFormat =
      if(Utils.containVariant(data.schema)) SupportedFormat.JSON
      else SupportedFormat.CSV

    var toSchema: Option[StructType] = None
    if(params.columnMap.isEmpty && params.columnMapping == "name") {
      val conn = jdbcWrapper.getConnector(params)
      try {
        toSchema = Some(removeQuote(jdbcWrapper.resolveTable(conn, params.table.get.name, params)))
        params.setColumnMap(
          generateColumnMap(
            data.schema,
            toSchema.get,
            params.columnMismatchBehavior == "error")
        )

      } finally conn.close()
    }

    params.columnMap match {
      case Some(map) =>
        if (map.isEmpty) {
          throw new UnsupportedOperationException(
            s"""
               |No column name matched between Snowflake Table and Spark Dataframe.
               |Please check the column names or manually assign the ColumnMap
         """.stripMargin
          )
        }
      case _ => //do nothing
    }

    val output: DataFrame = removeUselessColumns(data, params)
    val strRDD = dataFrameToRDD(sqlContext, output, params, format)
    io.writeRDD(params, strRDD, output.schema, saveMode, format)
  }



  def dataFrameToRDD(
                      sqlContext: SQLContext,
                      data: DataFrame,
                      params: MergedParameters,
                      format: SupportedFormat
                    ): RDD[String] = {
    val spark = sqlContext.sparkSession
    import spark.implicits._ // for toJson conversion

    format match {
      case SupportedFormat.CSV =>
        val conversionFunction = genConversionFunctions(data.schema)
        data.rdd.map(row=>{
          row.toSeq
            .zip(conversionFunction)
            .map{
              case(element, func) => func(element)
            }
            .mkString("|")
        })
      case SupportedFormat.JSON =>
        data.toJSON.map(_.toString).rdd
    }
  }

  /**
    * If column mapping is enable, remove all useless columns from the input DataFrame
    */
  private def removeUselessColumns(dataFrame: DataFrame, params: MergedParameters): DataFrame =
    params.columnMap match {
      case Some(map) =>
        val names = map.keys.toSeq
        try{
          dataFrame.select(names.head, names.tail: _*)
        }catch{
          case e: AnalysisException =>
            throw new IllegalArgumentException("Incorrect column name when column mapping: " + e.toString)
        }
      case _ => dataFrame
    }

  private def generateColumnMap(
                                 from: StructType,
                                 to: StructType,
                                 reportError: Boolean): Map[String, String] = {

    def containsColumn(name: String, list: StructType): Boolean =
      list.exists(_.name.equalsIgnoreCase(name))

    val result = mutable.HashMap[String, String]()
    val fromNameMap = mutable.HashMap[String, String]()
    val toNameMap = mutable.HashMap[String, String]()

    //check duplicated name after to lower
    from.foreach(field =>
      fromNameMap.put(field.name.toLowerCase, field.name) match {
        case Some(_) =>
          // if Snowflake table doesn't contain this column, ignore it
          if (containsColumn(field.name, to)) throw new UnsupportedOperationException(
            s"""
               |Duplicated column names in Spark DataFrame: ${fromNameMap(field.name.toLowerCase)}, ${field.name}
             """.stripMargin
          )
        case _ => //nothing
      }
    )

    to.foreach(field =>
      toNameMap.put(field.name.toLowerCase, field.name) match {
        case Some(_) =>
          // if Spark DataFrame doesn't contain this column, ignore it
          if (containsColumn(field.name, from)) throw new UnsupportedOperationException(
            s"""
               |Duplicated column names in Snowflake table: ${toNameMap(field.name.toLowerCase)}, ${field.name}
             """.stripMargin
          )
        case _ => //nothing
      }
    )

    //check mismatch
    if(reportError)
      if(fromNameMap.size != toNameMap.size) throw new UnsupportedOperationException(
        s"""
           |column number of Spark Dataframe (${fromNameMap.size}) doesn't match column number of Snowflake Table (${toNameMap.size})
         """.stripMargin
      )


    fromNameMap.foreach{
      case(index, name) =>
        if(toNameMap.contains(index)) result.put(name, toNameMap(index))
        else if (reportError) throw new UnsupportedOperationException(
          s"""
             |can't find column $name in Snowflake Table
           """.stripMargin
        )
    }

    result.toMap
  }

  // Prepare a set of conversion functions, based on the schema
  def genConversionFunctions(schema: StructType): Array[Any => Any] =
    schema.fields.map { field =>
      field.dataType match {
        case DateType =>
          (v: Any) =>
            v match {
              case null         => ""
              case t: Timestamp => Conversions.formatTimestamp(t)
              case d: Date      => Conversions.formatDate(d)
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
              else Conversions.formatString(v.asInstanceOf[String])
            }
        case _ =>
          (v: Any) =>
            Conversions.formatAny(v)
      }
    }
}

object DefaultSnowflakeWriter
    extends SnowflakeWriter(
      DefaultJDBCWrapper)
