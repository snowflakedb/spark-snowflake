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

import java.sql.Connection
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake._
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import net.snowflake.spark.snowflake.io.StageWriter.log
import net.snowflake.spark.snowflake.test.{TestHook, TestHookFlag}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory

import scala.util.Random

// Snowflake doesn't support DDL in tran, so the State Machine is
// created to maintain the ATOMIC of write.
// When writing to snowflake, there are at most 5 operations.
// Operation 1: Drop table (Optional)
//              "Drop table" is implemented as "rename table".
//              In case of COMMIT, the renamed table is dropped.
//              In case of Rollback, it is rename back.
// Operation 2: Create table (Optional)
//              In case of Rollback, drop the created table.
// Operation 3: Truncate table (Optional)
//              Truncate table and COPY INTO are put in one user transaction
//              It is commit/rollback,
// Operation 4: Copy-into (MUST)
// Operation 5: commit or rollback (MUST)
//              The rollback is done in reverse order of the happened operations.
class WriteTableState(conn: Connection) {
  // In case a table need to drop, rename is first. It will be dropped when commit.
  private var tableNameToBeDropped: String = ""
  private var tableNameToBeDroppedRename: String = ""
  // The table name created by this write operation. It is dropped when rollback.
  private var tableNameToBeCreated: String = ""
  // The user transaction will cover TRUNCATE table and COPY-INTO
  private var transactionName: String = ""

  private def clearStatus(): Unit = {
    tableNameToBeDropped = ""
    tableNameToBeDroppedRename = ""
    tableNameToBeCreated = ""
    transactionName = ""
  }

  def dropTable(tableName: String): Unit = {
    tableNameToBeDropped = tableName
    tableNameToBeDroppedRename =
      s"${tableName}_rename_${Math.abs(Random.nextInt()).toString}"
    conn.renameTable(tableNameToBeDroppedRename, tableNameToBeDropped)

    TestHook.raiseExceptionIfTestFlagEnabled(
      TestHookFlag.TH_WRITE_ERROR_AFTER_DROP_OLD_TABLE,
      "Negative test to raise error after dropping existing table"
    )
  }

  def createTable(tableName: String, schema: StructType,
                  params: MergedParameters): Unit = {
    tableNameToBeCreated = tableName
    conn.createTable(tableNameToBeCreated, schema, params,
      overwrite = false, temporary = false)

    TestHook.raiseExceptionIfTestFlagEnabled(
      TestHookFlag.TH_WRITE_ERROR_AFTER_CREATE_NEW_TABLE,
      "Negative test to raise error after create new table"
    )
  }

  private def beginTranIfNotBeginYet(): Unit = {
    if (transactionName.isEmpty) {
      transactionName = s"spark_connector_tx_${Math.abs(Random.nextInt()).toString}"
      // Start a user transaction
      conn.createStatement().execute(s"START TRANSACTION NAME $transactionName")
    }
  }

  def truncateTable(tableName: String): Unit = {
    beginTranIfNotBeginYet()

    conn.truncateTable(tableName)

    TestHook.raiseExceptionIfTestFlagEnabled(
      TestHookFlag.TH_WRITE_ERROR_AFTER_TRUNCATE_TABLE,
      "Negative test to raise error after truncate table"
    )
  }

  def copyIntoTable(schema: StructType,
                    saveMode: SaveMode,
                    params: MergedParameters,
                    file: String,
                    tempStage: String,
                    format: SupportedFormat): Unit = {
    val targetTable = params.table.get

    beginTranIfNotBeginYet()

    // pre actions
    Utils.executePreActions(
      DefaultJDBCWrapper,
      conn,
      params,
      Option(targetTable)
    )

    // Load the temporary data into the new file
    val copyStatement =
      StageWriter.copySql(
        schema,
        saveMode,
        params,
        targetTable,
        file,
        tempStage,
        format,
        conn
      )
    // copy
    log.debug(Utils.sanitizeQueryText(copyStatement.toString))
    // todo: handle on_error parameter on spark side

    // report the number of skipped files.
    // todo: replace table name to Identifier(?) after bug fixed
    val resultSet = copyStatement.execute(params.bindVariableEnabled)(conn)
    if (params.continueOnError) {
      var rowSkipped: Long = 0L
      while (resultSet.next()) {
        rowSkipped +=
          resultSet.getLong("rows_parsed") -
            resultSet.getLong("rows_loaded")
      }
      log.error(s"ON_ERROR: Continue -> Skipped $rowSkipped rows")
    }
    Utils.setLastCopyLoad(copyStatement.toString)
    // post actions
    Utils.executePostActions(
      DefaultJDBCWrapper,
      conn,
      params,
      Option(targetTable)
    )

    TestHook.raiseExceptionIfTestFlagEnabled(
      TestHookFlag.TH_WRITE_ERROR_AFTER_COPY_INTO,
      "Negative test to raise error after copy-into is executed"
    )
  }

  def commit(): Unit = {
    // Commit transaction
    conn.commit()

    // Actually drop the table if "rename table" is used instead of "drop table"
    if (!tableNameToBeDroppedRename.isEmpty) {
      conn.dropTable(tableNameToBeDroppedRename)
    }

    clearStatus()
  }

  def rollback(): Unit = {
    // Rollback TRUNCATE_TABLE & COPY INTO
    if (!transactionName.isEmpty) {
      conn.rollback()
    }

    // Drop created table
    if (!tableNameToBeCreated.isEmpty) {
      conn.dropTable(tableNameToBeCreated)
    }

    // Rename back existing table
    if (!tableNameToBeDroppedRename.isEmpty) {
      conn.renameTable(tableNameToBeDropped, tableNameToBeDroppedRename)
    }

    clearStatus()
  }
}

private[io] object StageWriter {

  private[io] val log = LoggerFactory.getLogger(getClass)

  def writeToStage(rdd: RDD[String],
                   schema: StructType,
                   saveMode: SaveMode,
                   params: MergedParameters,
                   format: SupportedFormat): Unit = {
    if (params.table.isEmpty) {
      throw new IllegalArgumentException(
        "For save operations you must specify a Snowflake table name with the 'dbtable' parameter"
      )
    }
    val prologueSql = Utils.genPrologueSql(params)
    log.debug(prologueSql.toString)

    val conn = DefaultJDBCWrapper.getConnector(params)

    try {
      prologueSql.execute(params.bindVariableEnabled)(conn)

      val (storage, stage) = CloudStorageOperations.createStorageClient(
        params, conn, tempStage = true, None, "load")

      val filesToCopy = storage.upload(rdd, format, None)

      if (filesToCopy.nonEmpty) {
        writeToTable(
          conn,
          schema,
          saveMode,
          params,
          filesToCopy.head.substring(0, filesToCopy.head.indexOf("/")),
          stage,
          format
        )
      }
    } finally {
      conn.close()
    }

  }

  /**
    * load data from stage to table
    */
  private def writeToTable(conn: Connection,
                           schema: StructType,
                           saveMode: SaveMode,
                           params: MergedParameters,
                           file: String,
                           tempStage: String,
                           format: SupportedFormat): Unit = {
    if (params.useStagingTable || !params.truncateTable) {
      writeToTableWithStagingTable(conn, schema, saveMode, params, file, tempStage, format)
    } else {
      writeToTableWithoutStagingTable(conn, schema, saveMode, params, file, tempStage, format)
    }
  }

  /**
    * load data from stage to table without staging table
    */
  private def writeToTableWithoutStagingTable(conn: Connection,
                                           schema: StructType,
                                           saveMode: SaveMode,
                                           params: MergedParameters,
                                           file: String,
                                           tempStage: String,
                                           format: SupportedFormat): Unit = {
    val tableName: String = params.table.get.name
    val writeTableState = new WriteTableState(conn)

    try {
      // Drop table only if necessary.
      if (saveMode == SaveMode.Overwrite &&
        DefaultJDBCWrapper.tableExists(conn, tableName) &&
        !params.truncateTable )
      {
        writeTableState.dropTable(tableName)
      }

      // If create table if table doesn't exist
      if (!DefaultJDBCWrapper.tableExists(conn, tableName))
      {
        writeTableState.createTable(tableName, schema, params)
      } else if (params.truncateTable && saveMode == SaveMode.Overwrite) {
        writeTableState.truncateTable(tableName)
      }

      // Run COPY INTO and related commands
      writeTableState.copyIntoTable(schema, saveMode, params, file, tempStage, format)

      // Commit a user transaction
      writeTableState.commit()
    } catch {
      case e: Exception =>
        // Rollback all the changes
        writeTableState.rollback()

        log.error("Error occurred while loading files to Snowflake: " + e)
        throw e
    }
  }

  /**
    * load data from stage to table with staging table
    * This function is deprecated.
    */
  private def writeToTableWithStagingTable(conn: Connection,
                           schema: StructType,
                           saveMode: SaveMode,
                           params: MergedParameters,
                           file: String,
                           tempStage: String,
                           format: SupportedFormat): Unit = {
    val table = params.table.get
    val tempTable =
      TableName(
        s"${table.name.replace('"', '_')}_staging_${Math.abs(Random.nextInt()).toString}"
      )
    val targetTable =
      if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
        tempTable
      } else {
        table
      }

    try {
      // purge tables when overwriting
      if (saveMode == SaveMode.Overwrite &&
          DefaultJDBCWrapper.tableExists(conn, table.toString)) {
        if (params.useStagingTable) {
          if (params.truncateTable) {
            conn.createTableLike(tempTable.name, table.name)
          }
        } else if (params.truncateTable) conn.truncateTable(table.name)
        else conn.dropTable(table.name)
      }

      // If the SaveMode is 'Append' and the target exists, skip
      // CREATE TABLE IF NOT EXIST command. This command doesn't actually
      // create a table but it needs CREATE TABLE privilege.
      if (saveMode == SaveMode.Overwrite ||
        !DefaultJDBCWrapper.tableExists(conn, table.toString))
      {
        conn.createTable(targetTable.name, schema, params,
          overwrite = false, temporary = false)
      }

      // pre actions
      Utils.executePreActions(
        DefaultJDBCWrapper,
        conn,
        params,
        Option(targetTable)
      )

      // Load the temporary data into the new file
      val copyStatement =
        copySql(
          schema,
          saveMode,
          params,
          targetTable,
          file,
          tempStage,
          format,
          conn
        )
      // copy
      log.debug(Utils.sanitizeQueryText(copyStatement.toString))
      // todo: handle on_error parameter on spark side

      // report the number of skipped files.
      // todo: replace table name to Identifier(?) after bug fixed
      val resultSet = copyStatement.execute(params.bindVariableEnabled)(conn)
      if (params.continueOnError) {
        var rowSkipped: Long = 0L
        while (resultSet.next()) {
          rowSkipped +=
            resultSet.getLong("rows_parsed") -
              resultSet.getLong("rows_loaded")
        }
        log.error(s"ON_ERROR: Continue -> Skipped $rowSkipped rows")
      }
      Utils.setLastCopyLoad(copyStatement.toString)
      // post actions
      Utils.executePostActions(
        DefaultJDBCWrapper,
        conn,
        params,
        Option(targetTable)
      )

      if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
        if (conn.tableExists(table.toString)) {
          conn.swapTable(table.name, tempTable.name)
          conn.dropTable(tempTable.name)
        } else {
          conn.renameTable(table.name, tempTable.name)
        }
      } else {
        conn.commit()
      }
    } catch {
      case e: Exception =>
        // snowflake-todo: try to provide more error information,
        // possibly from actual SQL output
        if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
          if (targetTable == tempTable) conn.dropTable(tempTable.name)
        }
        log.error("Error occurred while loading files to Snowflake: " + e)
        throw e
    }
  }

  /**
    * Generate the COPY SQL command
    */
  private[io] def copySql(schema: StructType,
                      saveMode: SaveMode,
                      params: MergedParameters,
                      table: TableName,
                      file: String,
                      tempStage: String,
                      format: SupportedFormat,
                      conn: Connection): SnowflakeSQLStatement = {

    if (saveMode != SaveMode.Append && params.columnMap.isDefined) {
      throw new UnsupportedOperationException(
        "The column mapping only works in append mode."
      )
    }

    def getMappingToString(
      list: Option[List[(Int, String)]]
    ): SnowflakeSQLStatement =
      format match {
        case SupportedFormat.JSON =>
          val tableSchema =
            DefaultJDBCWrapper.resolveTable(conn, table.name, params)
          if (list.isEmpty || list.get.isEmpty) {
            ConstantString("(") + tableSchema.fields
              .map(_.name)
              .mkString(",") + ")"
          } else {
            ConstantString("(") +
              list.get
                .map(
                  x =>
                    if (params.keepOriginalColumnNameCase) {
                      Utils.quotedNameIgnoreCase(x._2)
                    } else {
                      Utils.ensureQuoted(x._2)
                    }
                )
                .mkString(", ") + ")"
          }
        case SupportedFormat.CSV =>
          if (list.isEmpty || list.get.isEmpty) {
            EmptySnowflakeSQLStatement()
          } else {
            ConstantString("(") +
              list.get
                .map(
                  x =>
                    if (params.keepOriginalColumnNameCase) {
                      Utils.quotedNameIgnoreCase(x._2)
                    } else {
                      Utils.ensureQuoted(x._2)
                    }
                )
                .mkString(", ") + ")"
          }
      }

    def getMappingFromString(
      list: Option[List[(Int, String)]],
      from: SnowflakeSQLStatement
    ): SnowflakeSQLStatement =
      format match {
        case SupportedFormat.JSON =>
          if (list.isEmpty || list.get.isEmpty) {
            val names = schema.fields
              .map(x => "parse_json($1):".concat(x.name))
              .mkString(",")
            ConstantString("from (select") + names + from + "tmp)"
          } else {
            ConstantString("from (select") +
              list.get
                .map(x => "parse_json($1):".concat(schema(x._1 - 1).name))
                .mkString(", ") +
              from + "tmp)"
          }
        case SupportedFormat.CSV =>
          if (list.isEmpty || list.get.isEmpty) {
            from
          } else {
            ConstantString("from (select") +
              list.get.map(x => "tmp.$".concat(x._1.toString)).mkString(", ") +
              from + "tmp)"
          }
      }

    val fromString = ConstantString(s"FROM @$tempStage/$file") !

    val mappingList: Option[List[(Int, String)]] = params.columnMap match {
      case Some(map) =>
        Some(map.toList.map {
          case (key, value) =>
            try {
              (schema.fieldIndex(key) + 1, value)
            } catch {
              case e: Exception =>
                log.error("Error occurred while column mapping: " + e)
                throw e
            }
        })

      case None => None
    }

    val mappingToString = getMappingToString(mappingList)

    val mappingFromString = getMappingFromString(mappingList, fromString)

    val formatString =
      format match {
        case SupportedFormat.CSV =>
          ConstantString(s"""
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
          ConstantString(s"""
               |FILE_FORMAT = (
               |    TYPE = JSON
               |)
           """.stripMargin) !
      }

    val truncateCol =
      if (params.truncateColumns()) {
        ConstantString("TRUNCATECOLUMNS = TRUE") !
      } else {
        EmptySnowflakeSQLStatement()
      }

    val purge =
      if (params.purge()) {
        ConstantString("PURGE = TRUE") !
      } else {
        EmptySnowflakeSQLStatement()
      }

    val onError =
      if (params.continueOnError) {
        ConstantString("ON_ERROR = CONTINUE") !
      } else {
        EmptySnowflakeSQLStatement()
      }

    // todo: replace table name to Identifier(?) after bug fixed
    ConstantString("copy into") + table.name + mappingToString +
      mappingFromString + formatString + truncateCol + purge + onError
  }

}
