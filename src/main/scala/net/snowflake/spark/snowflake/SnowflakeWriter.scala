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
import org.apache.spark.sql.functions.lit

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
    val format: SupportedFormat =
      if(Utils.containVariant(data.schema)) SupportedFormat.JSON
      else SupportedFormat.CSV

    var schema: StructType = null
    if(params.columnMap.isEmpty && params.columnMapping == "name") {
      val conn = jdbcWrapper.getConnector(params)
      try {
        schema = jdbcWrapper.resolveTable(conn, params.table.get.name, params)
        params.setColumnMap(
          generateColumnMap(
            data.schema,
            schema,
            params.columnMismatchBehavior == "error")
        )

      } finally conn.close()
    }

    var output: DataFrame = removeUselessColumns(data, params)
    if(schema != null) output = addNulls(data, schema, params)
//    if(params.columnTypeMatching == "loose") output = castToString(output, schema, params)
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

  private def addNulls(dataFrame: DataFrame, toSchema: StructType, params: MergedParameters): DataFrame = {
    params.columnMap match {
      case Some(map) =>
        if(map.size == toSchema.size) dataFrame
        else {
          var list: List[StructField] = Nil
          val nameSet = mutable.HashSet[String]()
          var newMap:Map[String, String] = map
          var result = dataFrame

          map.foreach{
            case(_, to) => nameSet.add(to)
          }
          toSchema.foreach(field => {
            if(!nameSet.contains(field.name)) list = field :: list
          })

          if(list.isEmpty) dataFrame //impossible
          else {
            list.foreach(field => {
                val name = s"SF_SP_NULL_COLUMN_${field.name}"
                newMap += name -> field.name
                result = result.withColumn(name, lit(null).cast(field.dataType))
              }
            )
            params.setColumnMap(newMap)
            result
          }
        }
      case _ => dataFrame
    }
  }

//  private def castToString(dataFrame: DataFrame, toSchema: StructType, params: MergedParameters): DataFrame = {
//    params.columnMap match {
//      case Some(map)=>
//      case _ =>
//    }
//  }

  private def generateColumnMap(
                                 from: StructType,
                                 to: StructType,
                                 reportError: Boolean): Map[String, String] = {
    val result = mutable.HashMap[String, String]()
    val fromNameSet = mutable.HashMap[String, String]()
    val toNameSet = mutable.HashMap[String, String]()

    //check duplicated name after to lower
    from.foreach(field =>
      fromNameSet.put(field.name.toLowerCase, field.name) match {
        case Some(name) =>
          throw new UnsupportedOperationException(
            s"""
               |Duplicated column names in Spark DataFrame: $name, ${field.name}
             """.stripMargin
          )
        case _ => //nothing
      }
    )

    to.foreach(field =>
      toNameSet.put(field.name.toLowerCase, field.name) match {
        case Some(name) =>
          throw new UnsupportedOperationException(
            s"""
               |Duplicated column names in Snowflake table: $name, ${field.name}
             """.stripMargin
          )
        case _ => //nothing
      }
    )

    //check mismatch
    if(reportError)
      if(fromNameSet.size != toNameSet.size) throw new UnsupportedOperationException(
        s"""
           |column number of Spark Dataframe (${fromNameSet.size}) doesn't match column number of Snowflake Table (${toNameSet.size})
         """.stripMargin
      )


    fromNameSet.foreach{
      case(index, name) =>
        if(toNameSet.contains(index)) result.put(name, toNameSet(index))
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
