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

package com.snowflakedb.spark.snowflakedb

import java.net.URI
import java.sql.{Connection, Date, SQLException, Timestamp}

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.TaskContext
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random
import scala.util.control.NonFatal

import com.snowflakedb.spark.snowflakedb.Parameters.MergedParameters

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SQLContext}
import org.apache.spark.sql.types._

/**
 * Functions to write data to Snowflake.
 *
 * At a high level, writing data back to Snowflake involves the following steps:
 *
 *   - Use the spark-csv library to save the DataFrame to S3 using CSV serialization.
 *   snowflake-todo: Verify the below:
 *     Prior to
 *     saving the data, certain data type conversions are applied in order to work around
 *     limitations in Avro's data type support and Redshift's case-insensitive identifier handling.
 *
 *     While writing the Avro files, we use accumulators to keep track of which partitions were
 *     non-empty. After the write operation completes, we use this to construct a list of non-empty
 *     Avro partition files.
 *
 *   - Use JDBC to issue any CREATE TABLE commands, if required.
 *
 *   - If there is data to be written (i.e. not all partitions were empty),
 *     copy all the files sharing the prefix we exported to into Snowflake.
 *
 *   - Use JDBC to issue a COPY command in order to instruct Snowflake to load the CSV data into
 *     the appropriate table.
 *     If the Overwrite SaveMode is being used, then by default the data
 *     will be loaded into a temporary staging table, which later will atomically replace the
 *     original table using SWAP.
 */
private[snowflakedb] class SnowflakeWriter(
    jdbcWrapper: JDBCWrapper,
    s3ClientFactory: AWSCredentials => AmazonS3Client) {

  private val log = LoggerFactory.getLogger(getClass)

  // Execute a given string, logging it
  def sql(conn : Connection, query : String): Boolean = {
    log.info(query)
    conn.prepareStatement(query).execute()
  }

  /**
   * Generate CREATE TABLE statement for Snowflake
   */
  // Visible for testing.
  private[snowflakedb] def createTableSql(data: DataFrame, params: MergedParameters): String = {
    val schemaSql = jdbcWrapper.schemaString(data.schema)
    // snowflake-todo: for now, we completely ignore
    // params.distStyle and params.distKey
    val table = params.table.get
    s"CREATE TABLE IF NOT EXISTS $table ($schemaSql)"
  }

  /**
   * Generate the COPY SQL command
   */
  private def copySql(
      sqlContext: SQLContext,
      params: MergedParameters,
      creds: AWSCredentials,
      filesToCopyPrefix: String): String = {
    val awsAccessKey = creds.getAWSAccessKeyId
    val awsSecretKey = creds.getAWSSecretKey
    val fixedUrl = Utils.fixS3Url(filesToCopyPrefix)
    s"""
       |COPY INTO ${params.table.get}
       |FROM '$fixedUrl'
       |CREDENTIALS = (
       |    AWS_KEY_ID='$awsAccessKey'
       |    AWS_SECRET_KEY='$awsSecretKey'
       |)
       |FILE_FORMAT = (
       |    TYPE=CSV
       |    /* COMPRESSION=none */
       |    FIELD_DELIMITER='|'
       |    /* ESCAPE='\\\\' */
       |    NULL_IF=()
       |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
       |    TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM'
       |  )
    """.stripMargin.trim
  }

  /**
   * Sets up a staging table then runs the given action, passing the temporary table name
   * as a parameter.
   */
  private def withStagingTable(
      conn: Connection,
      table: TableName,
      action: (String) => Unit) {
    val randomSuffix = Math.abs(Random.nextInt()).toString
    val tempTable =
      table.copy(unescapedTableName = s"${table.unescapedTableName}_staging_$randomSuffix")
    log.info(s"Loading new data for Snowflake table '$table' using temporary table '$tempTable'")

    try {
      action(tempTable.toString)

      if (jdbcWrapper.tableExists(conn, table.toString)) {
        // Rename temp table to final table. Use SWAP to make it atomic.bi
        sql(conn, s"ALTER TABLE ${table} SWAP WITH ${tempTable}")
      } else {
        // Table didn't exist, just rename temp table to it
        sql(conn, s"ALTER TABLE $tempTable RENAME TO $table")
      }
    } finally {
      // If anything went wrong (or if SWAP worked), delete the temp table
      sql(conn, s"DROP TABLE IF EXISTS $tempTable")
    }
  }

  /**
   * Perform the Snowflake load, including deletion of existing data in the case of an overwrite,
   * and creating the table if it doesn't already exist.
   */
  private def doSnowflakeLoad(
      conn: Connection,
      data: DataFrame,
      saveMode: SaveMode,
      params: MergedParameters,
      creds: AWSCredentials,
      filesToCopyPrefix: Option[String]): Unit = {

    // Overwrites must drop the table, in case there has been a schema update
    if (saveMode == SaveMode.Overwrite) {
      val deleteStatement = s"DROP TABLE IF EXISTS ${params.table.get}"
      log.info(deleteStatement)
      val deleteExisting = conn.prepareStatement(deleteStatement)
      deleteExisting.execute()
    }

    // If the table doesn't exist, we need to create it first, using JDBC to infer column types
    val createStatement = createTableSql(data, params)
    log.info(createStatement)
    val createTable = conn.prepareStatement(createStatement)
    createTable.execute()

    // Perform the load if there were files loaded
    if (filesToCopyPrefix.isDefined) {
      // Load the temporary data into the new file
      val copyStatement = copySql(data.sqlContext, params,
                                  creds, filesToCopyPrefix.get)
      log.info(copyStatement)
      val copyData = conn.prepareStatement(copyStatement)
      try {
        copyData.execute()
      } catch {
        case e: SQLException =>
          // snowflake-todo: try to provide more error information,
          // possibly from actual SQL output
          log.error("Error occurred while loading files to Snowflake: " + e)
          throw e
      }
    }

    // Execute postActions
    params.postActions.foreach { action =>
      val actionSql = if (action.contains("%s")) action.format(params.table.get) else action
      log.info("Executing postAction: " + actionSql)
      conn.prepareStatement(actionSql).execute()
    }
  }

  /**
   * Serialize temporary data to S3, ready for Snowflake COPY
   *
   * @return the URL of the file prefix in S3 that should be loaded
   *         (usually "$tempDir/part"),if at least one record was written,
   *         and None otherwise.
   */
  private def unloadData(
      sqlContext: SQLContext,
      data: DataFrame,
      params: MergedParameters,
      tempDir: String): Option[String] = {

    // Prepare the set of conversion functions, based on the data type
    val conversionFunctions: Array[Any => Any] = data.schema.fields.map { field =>
      field.dataType match {
        case DateType => (v: Any) => v match {
          case null => ""
          case t: Timestamp => Conversions.formatTimestamp(t)
          case d: Date => Conversions.formatDate(d)
        }
        case TimestampType => (v: Any) => {
          if (v == null) ""
          else Conversions.formatTimestamp(v.asInstanceOf[Timestamp])
        }
        case StringType => (v: Any) => {
          if (v == null) ""
          else Conversions.formatString(v.asInstanceOf[String])
        }
        case _ => (v: Any) => Conversions.formatAny(v)
      }
    }

    // Use Spark accumulators to determine which partitions were non-empty.
    val nonEmptyPartitions =
      sqlContext.sparkContext.accumulableCollection(mutable.HashSet.empty[Int])


    // Create RDD that will be saved as strings
    val strRDD = data.rdd.mapPartitionsWithIndex { case (index, iter) =>
      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): String = {
          nonEmptyPartitions += TaskContext.get().partitionId()

          if (iter.nonEmpty) {
            val row = iter.next()
            // Build a simple pipe-delimited string
            var str = new mutable.StringBuilder
            var i = 0
            while (i < conversionFunctions.length) {
              if (i > 0)
                str.append('|')
              str.append(conversionFunctions(i)(row(i)))
              i += 1
            }
            str.toString()
          } else {
            ""
          }
        }
      }
    }
    // Save, possibly with compression. Always use Gzip for now
    if (params.sfCompress.equals("on"))
      strRDD.saveAsTextFile(tempDir, classOf[GzipCodec])
    else
      strRDD.saveAsTextFile(tempDir)

    if (nonEmptyPartitions.value.isEmpty) {
      None
    } else {
      // Verify there was at least one file created.
      // The saved filenames are going to be of the form part*
      val fs = FileSystem.get(URI.create(tempDir), sqlContext.sparkContext.hadoopConfiguration)
      val firstFile = fs.listStatus(new Path(tempDir))
        .iterator
        .map(_.getPath.getName)
        .filter(_.startsWith("part"))
        .take(1)
        .toSeq
        .headOption.getOrElse(throw new Exception("No part files were written!"))
      // Note - temp dir already has "/", no need to add it
      Some(tempDir + "part")
    }
  }

  /**
   * Write a DataFrame to a Snowflake table, using S3 and CSV serialization
   */
  def saveToSnowflake(
      sqlContext: SQLContext,
      data: DataFrame,
      saveMode: SaveMode,
      params: MergedParameters) : Unit = {
    if (params.table.isEmpty) {
      throw new IllegalArgumentException(
        "For save operations you must specify a Snowflake table name with the 'dbtable' parameter")
    }

    val creds: AWSCredentials = params.temporaryAWSCredentials.getOrElse(
      AWSCredentialsUtils.load(params.rootTempDir, sqlContext.sparkContext.hadoopConfiguration))

    Utils.assertThatFileSystemIsNotS3BlockFileSystem(
      new URI(params.rootTempDir), sqlContext.sparkContext.hadoopConfiguration)

    Utils.checkThatBucketHasObjectLifecycleConfiguration(params.rootTempDir, s3ClientFactory(creds))

    val conn = jdbcWrapper.getConnector(params)

    try {
      val tempDir = params.createPerQueryTempDir()
      val filesToCopyPrefix = unloadData(sqlContext, data, params, tempDir)
      if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
        withStagingTable(conn, params.table.get, stagingTable => {
          val updatedParams = MergedParameters(params.parameters.updated("dbtable", stagingTable))
          doSnowflakeLoad(conn, data, saveMode, updatedParams, creds, filesToCopyPrefix)
        })
      } else {
        doSnowflakeLoad(conn, data, saveMode, params, creds, filesToCopyPrefix)
      }
    } finally {
      conn.close()
    }
  }
}

object DefaultSnowflakeWriter extends SnowflakeWriter(
  DefaultJDBCWrapper,
  awsCredentials => new AmazonS3Client(awsCredentials))
