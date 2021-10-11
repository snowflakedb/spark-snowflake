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

import java.sql.{Connection, ResultSet}
import java.time.LocalDateTime
import java.util.TimeZone

import net.snowflake.client.jdbc.SnowflakeResultSet
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake._
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import net.snowflake.spark.snowflake.test.{TestHook, TestHookFlag}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.slf4j.LoggerFactory

import scala.collection._
import scala.collection.mutable.ArrayBuffer
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

  def copyIntoTable(sqlContext: SQLContext,
                    schema: StructType,
                    saveMode: SaveMode,
                    params: MergedParameters,
                    file: String,
                    tempStage: String,
                    format: SupportedFormat,
                    fileUploadResults: List[FileUploadResult]): Unit = {
    val targetTable = params.table.get

    beginTranIfNotBeginYet()

    // pre actions
    Utils.executePreActions(
      DefaultJDBCWrapper,
      conn,
      params,
      Option(targetTable)
    )

    // Execute COPY INTO TABLE to load data
    StageWriter.executeCopyIntoTable(
      sqlContext,
      conn,
      schema,
      saveMode,
      params,
      targetTable,
      file,
      tempStage,
      format,
      fileUploadResults)

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

  def writeToStage(sqlContext: SQLContext,
                   rdd: RDD[String],
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
      prologueSql.foreach(x => x.execute(params.bindVariableEnabled)(conn))

      val (storage, stage) = CloudStorageOperations.createStorageClient(
        params, conn, tempStage = true, None, "load")

      val startTime = System.currentTimeMillis()
      val fileUploadResults = storage.upload(rdd, format, None)

      val startCopyInto = System.currentTimeMillis()
      if (fileUploadResults.nonEmpty) {
        val firstFileName = fileUploadResults.head.fileName
        writeToTable(
          sqlContext,
          conn,
          schema,
          saveMode,
          params,
          firstFileName.substring(0, firstFileName.indexOf("/")),
          stage,
          format,
          fileUploadResults
        )
      } else if (params.skipWriteWhenWritingEmptyDataFrame) {
        log.info(
          s"""${SnowflakeResultSetRDD.MASTER_LOG_PREFIX}:
             | Skip to execute COPY INTO TABLE command because
             | no file is uploaded.
             |""".stripMargin.filter(_ >= ' '))
      } else {
          conn.createTable(params.table.get.name, schema, params,
            overwrite = saveMode.equals(SaveMode.Overwrite), temporary = false)
      }
      val endTime = System.currentTimeMillis()

      log.info(
          s"""${SnowflakeResultSetRDD.MASTER_LOG_PREFIX}:
             | Total job time is ${Utils.getTimeString(endTime - startTime)}
             | including read & upload time:
             | ${Utils.getTimeString(startCopyInto - startTime)}
             | and COPY time: ${Utils.getTimeString(endTime - startCopyInto)}.
             |""".stripMargin.filter(_ >= ' '))
    } finally {
      SnowflakeTelemetry.send(conn.getTelemetry)
      conn.close()
    }

  }

  /**
    * load data from stage to table
    */
  private def writeToTable(sqlContext: SQLContext,
                           conn: Connection,
                           schema: StructType,
                           saveMode: SaveMode,
                           params: MergedParameters,
                           file: String,
                           tempStage: String,
                           format: SupportedFormat,
                           fileUploadResults: List[FileUploadResult]): Unit = {
    if (params.useStagingTable || !params.truncateTable) {
      writeToTableWithStagingTable(sqlContext, conn, schema, saveMode, params,
        file, tempStage, format, fileUploadResults)
    } else {
      writeToTableWithoutStagingTable(sqlContext, conn, schema, saveMode, params,
        file, tempStage, format, fileUploadResults)
    }
  }

  /**
    * load data from stage to table without staging table
    */
  private def writeToTableWithoutStagingTable(sqlContext: SQLContext,
                                              conn: Connection,
                                              schema: StructType,
                                              saveMode: SaveMode,
                                              params: MergedParameters,
                                              file: String,
                                              tempStage: String,
                                              format: SupportedFormat,
                                              fileUploadResults: List[FileUploadResult]): Unit = {
    val tableName: String = params.table.get.name
    val writeTableState = new WriteTableState(conn)

    try {
      val tableExists = DefaultJDBCWrapper.tableExists(params, tableName)
      // Drop table only if necessary.
      if (saveMode == SaveMode.Overwrite && tableExists && !params.truncateTable)
      {
        writeTableState.dropTable(tableName)
      }

      // If create table if table doesn't exist
      if (!tableExists)
      {
        writeTableState.createTable(tableName, schema, params)
      } else if (params.truncateTable && saveMode == SaveMode.Overwrite) {
        writeTableState.truncateTable(tableName)
      }

      // Run COPY INTO and related commands
      writeTableState.copyIntoTable(
        sqlContext,
        schema,
        saveMode,
        params,
        file,
        tempStage,
        format,
        fileUploadResults)

      // Commit a user transaction
      writeTableState.commit()
    } catch {
      case th: Throwable =>
        // Rollback all the changes
        writeTableState.rollback()

        log.error("Error occurred while loading files to Snowflake: " + th)
        throw th
    }
  }

  private[snowflake] def getStageTableName(tableName: String): String = {
    val trimmedName = tableName.trim
    val postfix = s"_staging_${Math.abs(Random.nextInt()).toString}"
    if (trimmedName.endsWith("\"")) {
      // The table name is quoted, insert the postfix before last '"'
      s"""${trimmedName.substring(0, trimmedName.length - 1)}$postfix""""
    } else {
      // Append the postfix
      s"$trimmedName$postfix"
    }
  }

  /**
    * load data from stage to table with staging table
    * This function is deprecated.
    */
  private def writeToTableWithStagingTable(sqlContext: SQLContext,
                                           conn: Connection,
                                           schema: StructType,
                                           saveMode: SaveMode,
                                           params: MergedParameters,
                                           file: String,
                                           tempStage: String,
                                           format: SupportedFormat,
                                           fileUploadResults: List[FileUploadResult])
  : Unit = {
    val table = params.table.get
    val tempTable = TableName(
      if (params.stagingTableNameRemoveQuotesOnly) {
        // NOTE: This is the staging table name generation for SC 2.8.1 and earlier.
        // It is kept for back-compatibility and it will be removed later without any notice.
        s"${table.name.replace('"', '_')}_staging_${Math.abs(Random.nextInt()).toString}"
      } else {
        getStageTableName(table.name)
      }
    )
    val targetTable =
      if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
        tempTable
      } else {
        table
      }

    try {
      val tableExists = DefaultJDBCWrapper.tableExists(params, table.toString)
      // purge tables when overwriting
      if (saveMode == SaveMode.Overwrite && tableExists) {
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
      if (saveMode == SaveMode.Overwrite || !tableExists)
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

      // Execute COPY INTO TABLE to load data
      StageWriter.executeCopyIntoTable(
        sqlContext,
        conn,
        schema,
        saveMode,
        params,
        targetTable,
        file,
        tempStage,
        format,
        fileUploadResults)

      // post actions
      Utils.executePostActions(
        DefaultJDBCWrapper,
        conn,
        params,
        Option(targetTable)
      )

      if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
        if (tableExists) {
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
    * Execute COPY INTO table command.
    * Firstly, it executes COPY INTO table commands without FILES clause.
    * Internally, snowflake uses LIST to get files for a prefix.
    * In rare cases, some files may be missing because the cloud service's
    * eventually consistency.
    * So it the return result for COPY command is checked. If any files are
    * missed, an additional COPY INTO table with FILES clause is used to load
    * the missed files.
    */
  private[io] def executeCopyIntoTable(sqlContext: SQLContext,
                                       conn: Connection,
                                       schema: StructType,
                                       saveMode: SaveMode,
                                       params: MergedParameters,
                                       targetTable: TableName,
                                       file: String,
                                       tempStage: String,
                                       format: SupportedFormat,
                                       fileUploadResults: List[FileUploadResult])
  : Unit = {
    val progress = new ArrayBuffer[String]()
    val start = System.currentTimeMillis()
    logAndAppend(progress, s"Begin to write at ${LocalDateTime.now()} ("
      + TimeZone.getDefault.getDisplayName + ")")

    // If a file is empty, there is no file are upload.
    // So the expected files are non empty files.
    var totalSize: Long = 0
    val expectedFileSet = mutable.Set[String]()
    fileUploadResults.foreach(fileUploadResult =>
      if (fileUploadResult.fileSize > 0) {
        expectedFileSet += fileUploadResult.fileName
        totalSize += fileUploadResult.fileSize
      })
    logAndAppend(progress, s"Total file count is ${fileUploadResults.size}, " +
      s"non-empty files count is ${expectedFileSet.size}, " +
      s"total file size is ${Utils.getSizeString(totalSize)}.")

    // Indicate whether to use FILES clause in the copy command
    var useFilesClause = false

    // For testing purpose, only load part of files if the test flag is on.
    // Expect the missed files are detected and loaded with 2nd COPY.
    val firstCopyFileSet: Option[mutable.Set[String]] =
      if (TestHook.isTestFlagEnabled(
        TestHookFlag.TH_COPY_INTO_TABLE_MISS_FILES_SUCCESS)) {
        useFilesClause = true
        Some(expectedFileSet.grouped(2).toList.head)
      } else {
        Some(expectedFileSet)
      }

    // Generate COPY statement without FILES clause.
    val copyStatement = StageWriter.copySql(
      schema,
      saveMode,
      params,
      targetTable,
      file,
      tempStage,
      format,
      conn,
      useFilesClause,
      firstCopyFileSet.get.toSet
    )
    log.debug(Utils.sanitizeQueryText(copyStatement.toString))
    logAndAppend(progress,
      s"Now executing below command to write into table:\n${copyStatement.toString}")

    var lastStatement = copyStatement
    try {
      // execute the COPY INTO TABLE statement
      val resultSet = if (params.isExecuteQueryWithSyncMode) {
        copyStatement.execute(params.bindVariableEnabled)(conn)
      } else {
        val asyncRs = copyStatement.executeAsync(params.bindVariableEnabled)(conn)
        val queryID = asyncRs.asInstanceOf[SnowflakeResultSet].getQueryID
        SparkConnectorContext.addRunningQuery(sqlContext.sparkContext, conn, queryID)
        logAndAppend(progress,
          s"The query ID for the write into table command is: $queryID; " +
            s"The query ID URL is:\n${params.getQueryIDUrl(queryID)}")
        // Call getMetaData() to wait fot the async query to be done
        // Note: do not call next() to wait for the query to be done because
        // it will change the ResultSet, so getCopyMissedFiles() doesn't work.
        asyncRs.getMetaData
        SparkConnectorContext.removeRunningQuery(sqlContext.sparkContext, conn, queryID)
        asyncRs
      }
      val firstCopyEnd = System.currentTimeMillis()
      logAndAppend(progress,
        s"""First COPY command is done in
           | ${Utils.getTimeString(firstCopyEnd - start)}
           | at ${LocalDateTime.now()}, queryID is
           | ${lastStatement.getLastQueryID()}
           |""".stripMargin.filter(_ >= ' '))

      // Save the original COPY command even if additional COPY is run.
      Utils.setLastCopyLoad(copyStatement.toString)

      // Get missed files if there are any.
      var missedFileSet = getCopyMissedFiles(params, resultSet, expectedFileSet)

      // If any files are missed, execute 2nd COPY command
      if (missedFileSet.nonEmpty) {
        // Negative test:
        // Only load part of missed files.
        // Exception is raised for the failure.
        val secondCopyFileSet: Option[mutable.Set[String]] =
        if (TestHook.isTestFlagEnabled(
          TestHookFlag.TH_COPY_INTO_TABLE_MISS_FILES_FAIL)) {
          Some(missedFileSet.grouped(2).toList.head)
        } else {
          Some(missedFileSet)
        }

        // Generate copy command with missed files only
        useFilesClause = true
        val copyWithFileClause = StageWriter.copySql(
          schema,
          saveMode,
          params,
          targetTable,
          file,
          tempStage,
          format,
          conn,
          useFilesClause,
          secondCopyFileSet.get.toSet
        )
        lastStatement = copyWithFileClause
        logAndAppend(progress, s"Second COPY command: $lastStatement")

        def getMissedFileInfo(missedFileSet: mutable.Set[String]): String = {
          s"""missedFileCount=${missedFileSet.size}
             | Files: (${missedFileSet.mkString(", ")})
             |""".stripMargin.filter(_ >= ' ')
        }

        // Log missed files info
        log.warn(
          s"""Some files are not loaded into the table, execute additional COPY
             | to load them: ${getMissedFileInfo(missedFileSet)}
             | """.stripMargin)

        // Run the 2nd COPY command
        val resultSet = if (params.isExecuteQueryWithSyncMode) {
          copyWithFileClause.execute(params.bindVariableEnabled)(conn)
        } else {
          val asyncRs = copyWithFileClause.executeAsync(params.bindVariableEnabled)(conn)
          val queryID = asyncRs.asInstanceOf[SnowflakeResultSet].getQueryID
          SparkConnectorContext.addRunningQuery(sqlContext.sparkContext, conn, queryID)
          logAndAppend(progress,
            s"The query ID for the write into table command is: $queryID; " +
              s"The query ID URL is:\n${params.getQueryIDUrl(queryID)}")
          // Call getMetaData() to wait fot the async query to be done
          // Note: do not call next() to wait for the query to be done because
          // it will change the ResultSet, so getCopyMissedFiles() doesn't work.
          asyncRs.getMetaData
          SparkConnectorContext.removeRunningQuery(sqlContext.sparkContext, conn, queryID)
          asyncRs
        }
        val secondCopyEnd = System.currentTimeMillis()
        logAndAppend(progress,
          s"""Second COPY command is done in
             | ${Utils.getTimeString(secondCopyEnd - firstCopyEnd)}
             | at ${LocalDateTime.now()}, queryID is
             | ${lastStatement.getLastQueryID()}
             |""".stripMargin.filter(_ >= ' '))
        missedFileSet = getCopyMissedFiles(params, resultSet, missedFileSet)

        // It is expected all the files must be loaded.
        if (missedFileSet.nonEmpty) {
          throw new SnowflakeConnectorException(
            s"""These files are missed when COPY INTO TABLE:
               | ${getMissedFileInfo(missedFileSet)}
               | """.stripMargin.filter(_ >= ' '))
        }
      }
      val end = System.currentTimeMillis()
      logAndAppend(progress,
        s"Succeed to write in ${Utils.getTimeString(end - start)}" +
          s" at ${LocalDateTime.now()}")
    } catch {
      case th: Throwable => {
        val end = System.currentTimeMillis()
        val message = s"Fail to write in ${Utils.getTimeString(end - start)} at ${LocalDateTime.now()}"
        logger.error(message)
        progress.append(message)
        // send telemetry message
        SnowflakeTelemetry.sendQueryStatus(conn,
          TelemetryConstValues.OPERATION_WRITE,
          lastStatement.getLastQueryID(),
          TelemetryConstValues.STATUS_FAIL,
          end - start,
          Some(th),
          progress.mkString("\n"))
        // Re-throw the exception
        throw th
      }
    }
  }

  // Check missed files for the COPY command
  private def getCopyMissedFiles(params: MergedParameters,
                                 copyResultSet: ResultSet,
                                 expectedFileSet: mutable.Set[String])
  : mutable.Set[String] = {
    val COPY_INTO_TABLE_RESULT_COLUMN_FILE = "file"
    val COPY_INTO_TABLE_RESULT_COLUMN_ROW_PARSED = "rows_parsed"
    val COPY_INTO_TABLE_RESULT_COLUMN_ROW_LOADED = "rows_loaded"

    // get column list from the COPY result set
    val metadata = copyResultSet.getMetaData
    val columnNameSet = mutable.Set[String]()
    for (i <- 1 to metadata.getColumnCount) {
      columnNameSet += metadata.getColumnName(i)
    }

    // Check the COPY result only when the result format is expected.
    if (!columnNameSet.contains(COPY_INTO_TABLE_RESULT_COLUMN_FILE) ||
      !columnNameSet.contains(COPY_INTO_TABLE_RESULT_COLUMN_ROW_PARSED) &&
        !columnNameSet.contains(COPY_INTO_TABLE_RESULT_COLUMN_ROW_LOADED)) {
      log.warn(
        s"""Fail to check the COPY result because format is not supported.
           | The column names are: ${columnNameSet.mkString(", ")}
           | Expect to include $COPY_INTO_TABLE_RESULT_COLUMN_FILE and
           | $COPY_INTO_TABLE_RESULT_COLUMN_ROW_PARSED and
           | $COPY_INTO_TABLE_RESULT_COLUMN_ROW_LOADED
           | """.stripMargin.filter(_ >= ' '))
      return mutable.Set.empty
    }

    // The missed file set is initialized as the expected files set.
    // The loaded files name are removed from it in the later iteration.
    // The left files are missed files.
    val missedFileSet = expectedFileSet.clone()
    var rowSkipped: Long = 0L
    while (copyResultSet.next()) {
      if (params.continueOnError) {
        rowSkipped +=
          copyResultSet.getLong(COPY_INTO_TABLE_RESULT_COLUMN_ROW_PARSED) -
            copyResultSet.getLong(COPY_INTO_TABLE_RESULT_COLUMN_ROW_LOADED)
      }
      // The file name from COPY ResultSet is different for
      // external/internal stage.
      // For internal stage, it is like: <stage_name>/<prefix>/<filename>
      // For external stage, it is like:
      // s3://<bucket>/<system_prefix>/<prefix>/<filename>
      // The file name in expectedFileSet is <prefix>/<filename>
      val fileFullName = copyResultSet
        .getString(COPY_INTO_TABLE_RESULT_COLUMN_FILE)
      val fileNameWithoutStage: String =
        fileFullName.replaceAll(".*/([^/]+/[^/]+)$", "$1")
      // Remove the found files from missed file set.
      if (missedFileSet.contains(fileNameWithoutStage)) {
        missedFileSet -= fileNameWithoutStage
      } else {
        log.warn(s"Load file which isn't uploaded by SC: $fileFullName")
      }
    }

    if (params.continueOnError) {
      log.error(s"ON_ERROR: Continue -> Skipped $rowSkipped rows")
    }

    missedFileSet
  }

  /**
    * Generate the COPY SQL command
    */
  private[io] def copySql(schema: StructType,
                          saveMode: SaveMode,
                          params: MergedParameters,
                          table: TableName,
                          prefix: String,
                          tempStage: String,
                          format: SupportedFormat,
                          conn: Connection,
                          useFilesClause: Boolean,
                          filesToCopy: Set[String]): SnowflakeSQLStatement = {

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
              .map(
                x =>
                  if (params.quoteJsonFieldName) {
                    if (params.keepOriginalColumnNameCase) {
                      Utils.quotedNameIgnoreCase(x.name)
                    } else {
                      Utils.ensureQuoted(x.name)
                    }
                  } else {
                    x.name
                  }
              )
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
              .map(x => "parse_json($1):".concat(
                if (params.quoteJsonFieldName) {
                  "\"" + x.name + "\""
                } else {
                  x.name
                }
              ))
              .mkString(",")
            ConstantString("from (select") + names + from + "tmp)"
          } else {
            ConstantString("from (select") +
              list.get
                .map(x => "parse_json($1):".concat(
                  if (params.quoteJsonFieldName) {
                    "\"" + schema(x._1 - 1).name + "\""
                  } else {
                    schema(x._1 - 1).name
                  }
                ))
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

    val fromString = ConstantString(s"FROM @$tempStage/$prefix/") !

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
               |    TIMESTAMP_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF9'
               |    DATE_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF9'
               |    BINARY_FORMAT=BASE64
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

    // Use FILES clause only when useFilesClause is true.
    val filesClause = if (useFilesClause && filesToCopy.nonEmpty) {
      // The original filename has a prefix which need to be removed
      // because it has been included in 'fromString'
      val filesWithoutPrefix = filesToCopy.map(
        x => x.substring(x.lastIndexOf("/") + 1))
      ConstantString(
        s"""FILES = ( '${filesWithoutPrefix.mkString("' , '")}' )
           |""".stripMargin) !
    } else {
      EmptySnowflakeSQLStatement()
    }

    // todo: replace table name to Identifier(?) after bug fixed
    ConstantString("copy into") + table.name + mappingToString +
      mappingFromString + filesClause + formatString + truncateCol +
      purge + onError
  }

  private def logAndAppend(messages: ArrayBuffer[String], message: String) : Unit = {
    log.info(message)
    messages.append(message)
  }

}
