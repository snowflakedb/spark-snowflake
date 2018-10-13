/*
 * Copyright 2018 Snowflake Computing
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
package net.snowflake.spark.snowflake.io

import java.sql.{Connection, SQLException}
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake._
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

import scala.util.Random

private[io] object StageWriter {

  private val log = LoggerFactory.getLogger(getClass)

  def writeToStage(
                    rdd: RDD[String],
                    schema: StructType,
                    saveMode: SaveMode,
                    params: MergedParameters,
                    format: SupportedFormat
                  ): Unit = {
    if (params.table.isEmpty) {
      throw new IllegalArgumentException(
        "For save operations you must specify a Snowflake table name with the 'dbtable' parameter")
    }
    val prologueSql = Utils.genPrologueSql(params)
    log.debug(prologueSql.toString)

    val conn = DefaultJDBCWrapper.getConnector(params)

    try {
      prologueSql.execute(conn)

      val (storage, stage) =
        CloudStorageOperations.createStorageClient(params, conn, true, None)

      val filesToCopy = storage.upload(rdd, format, None, true)

      writeToTable(conn, schema, saveMode, params,
        filesToCopy.head.substring(0, filesToCopy.head.indexOf("/")), stage, format)

    } finally {
      conn.close()
    }

  }


  /**
    * load data from stage to table
    */
  private def writeToTable(
                            conn: Connection,
                            schema: StructType,
                            saveMode: SaveMode,
                            params: MergedParameters,
                            file: String,
                            tempStage: String,
                            format: SupportedFormat
                          ): Unit = {
    val table = params.table.get
    val tempTable =
      TableName(s"${table.name}_staging_${Math.abs(Random.nextInt()).toString}")
    val targetTable = if (saveMode == SaveMode.Overwrite
      && params.useStagingTable) tempTable else table

    try {
      //purge tables when overwriting
      if (saveMode == SaveMode.Overwrite &&
        DefaultJDBCWrapper.tableExists(conn, table.toString)) {
        if (params.useStagingTable) {
          if (params.truncateTable) {
            conn.createTableLike(tempTable.name, table.name)
          }
        }
        else if (params.truncateTable) conn.truncateTable(table.name)
        else conn.dropTable(table.name)
      }

      //create table
      conn.createTable(targetTable.name, schema)

      //pre actions
      Utils.executePreActions(DefaultJDBCWrapper, conn, params, Option(targetTable))

      // Load the temporary data into the new file
      val copyStatement =
        copySql(schema, saveMode, params, targetTable, file, tempStage, format, conn)
      //copy
      log.debug(Utils.sanitizeQueryText(copyStatement.toString))
      //todo: handle on_error parameter on spark side

      //report the number of skipped files.
      val resultSet = copyStatement.execute(conn) //todo: replace table name to Identifier(?) after bug fixed
      if (params.continueOnError) {
        var rowSkipped: Long = 0l
        while (resultSet.next()) {
          rowSkipped +=
            resultSet.getLong("rows_parsed") -
              resultSet.getLong("rows_loaded")
        }
        log.error(s"ON_ERROR: Continue -> Skipped $rowSkipped rows")
      }
      Utils.setLastCopyLoad(copyStatement.toString)
      //post actions
      Utils.executePostActions(DefaultJDBCWrapper, conn, params, Option(targetTable))

      if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
        if (DefaultJDBCWrapper.tableExists(conn, table.toString))
          conn.swapTable(table.name, tempTable.name)
        else
          conn.renameTable(table.name, tempTable.name)
      }
    } catch {
      case e: Exception =>
        // snowflake-todo: try to provide more error information,
        // possibly from actual SQL output
        if (targetTable == tempTable) conn.dropTable(tempTable.name)
        log.error("Error occurred while loading files to Snowflake: " + e)
        throw e
    }
  }

  /**
    * Generate the COPY SQL command
    */
  private def copySql(
                       schema: StructType,
                       saveMode: SaveMode,
                       params: MergedParameters,
                       table: TableName,
                       file: String,
                       tempStage: String,
                       format: SupportedFormat,
                       conn: Connection
                     ): SnowflakeSQLStatement = {

    if (saveMode != SaveMode.Append && params.columnMap.isDefined)
      throw new UnsupportedOperationException("The column mapping only works in append mode.")

    def getMappingToString(list: Option[List[(Int, String)]]): SnowflakeSQLStatement =
      format match {
        case SupportedFormat.JSON =>
          val tableSchema = DefaultJDBCWrapper.resolveTable(conn, table.name)
          if (list.isEmpty || list.get.isEmpty)
            ConstantString("(") + tableSchema.fields.map(_.name).mkString(",") + ")"
          else ConstantString("(") +
            list.get.map(x => Utils.ensureQuoted(x._2)).mkString(", ") + ")"
        case SupportedFormat.CSV =>
          if (list.isEmpty || list.get.isEmpty) EmptySnowflakeSQLStatement()
          else ConstantString("(") +
            list.get.map(x => Utils.ensureQuoted(x._2)).mkString(", ") + ")"
      }


    def getMappingFromString(list: Option[List[(Int, String)]], from: SnowflakeSQLStatement): SnowflakeSQLStatement =
      format match {
        case SupportedFormat.JSON =>
          if (list.isEmpty || list.get.isEmpty) {
            val names = schema.fields.map(x => "parse_json($1):".concat(x.name)).mkString(",")
            ConstantString("from (select") + names + from + "tmp)"
          }
          else
            ConstantString("from (select") +
              list.get.map(x => "parse_json($1):".concat(schema(x._1 - 1).name)).mkString(", ") +
              from + "tmp)"
        case SupportedFormat.CSV =>
          if (list.isEmpty || list.get.isEmpty) from
          else ConstantString("from (select") +
            list.get.map(x => "tmp.$".concat(x._1.toString)).mkString(", ") +
            from + "tmp)"
      }

    val fromString = ConstantString(s"FROM @$tempStage/$file") !

    val mappingList: Option[List[(Int, String)]] = params.columnMap match {
      case Some(map) =>
        Some(map.toList.map {
          case (key, value) =>
            try {
              (schema.fieldIndex(key) + 1, value)
            } catch {
              case e: Exception => {
                log.error("Error occurred while column mapping: " + e)
                throw e
              }
            }
        })

      case None => None
    }

    val mappingToString = getMappingToString(mappingList)

    val mappingFromString = getMappingFromString(mappingList, fromString)

    val formatString =
      format match {
        case SupportedFormat.CSV =>
          ConstantString(
            s"""
               |FILE_FORMAT = (
               |    TYPE=CSV
               |    FIELD_DELIMITER='|'
               |    NULL_IF=()
               |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
               |    TIMESTAMP_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF3'
               |    DATE_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF3'
               |  )
           """.stripMargin) !
        case SupportedFormat.JSON =>
          ConstantString(
            s"""
               |FILE_FORMAT = (
               |    TYPE = JSON
               |)
           """.stripMargin) !
      }

    val truncateCol =
      if (params.truncateColumns())
        ConstantString("TRUNCATECOLUMNS = TRUE") !
      else
        EmptySnowflakeSQLStatement()

    val purge = if (params.purge())
      ConstantString("PURGE = TRUE") ! else EmptySnowflakeSQLStatement()

    val onError = if (params.continueOnError)
      ConstantString("ON_ERROR = CONTINUE") ! else EmptySnowflakeSQLStatement()

    //todo: replace table name to Identifier(?) after bug fixed
    ConstantString("copy into") + table.name + mappingToString +
      mappingFromString + formatString + truncateCol + purge + onError
  }


}
