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

import java.io._
import java.net.URI
import java.sql.{Connection, Date, SQLException, Timestamp}
import java.util.zip.GZIPOutputStream

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model._
import com.amazonaws.services.s3.transfer.TransferManager
import com.amazonaws.util.Base64
import net.snowflake.client.jdbc.SnowflakeUtil
import net.snowflake.spark.snowflake.ConnectorSFStageManager._
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.TaskContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

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
    jdbcWrapper: JDBCWrapper,
    s3ClientFactory: AWSCredentials => AmazonS3Client) {

  private val log = LoggerFactory.getLogger(getClass)

  // Format a row using provided conversion functions
  def formatRow(conversionFunctions: Array[Any => Any], row: Row): String = {
    // Build a simple pipe-delimited string
    val str = new mutable.StringBuilder
    var i   = 0
    while (i < conversionFunctions.length) {
      if (i > 0)
        str.append('|')
      str.append(conversionFunctions(i)(row(i)))
      i += 1
    }
    str.toString()
  }

  /**
    * Write a DataFrame to a Snowflake table, using S3 and CSV serialization
    */
  def saveToSnowflake(sqlContext: SQLContext,
                      data: DataFrame,
                      saveMode: SaveMode,
                      params: MergedParameters): Unit = {

    if (params.table.isEmpty) {
      throw new IllegalArgumentException(
        "For save operations you must specify a Snowflake table name with the 'dbtable' parameter")
    }

    val prologueSql = Utils.genPrologueSql(params)
    log.debug(prologueSql)

    if (params.usingExternalStage) {

      Utils.assertThatFileSystemIsNotS3BlockFileSystem(
        new URI(params.rootTempDir),
        sqlContext.sparkContext.hadoopConfiguration)

      val creds = AWSCredentialsUtils.getCreds(sqlContext, params)

      if (params.checkBucketConfiguration) {
        Utils.checkThatBucketHasObjectLifecycleConfiguration(
          params.rootTempDir,
          s3ClientFactory(creds))
      }

      val conn = jdbcWrapper.getConnector(params)

      try {
        jdbcWrapper.executeInterruptibly(conn, prologueSql)

        val filesToCopy =
          unloadData(sqlContext,
                     data,
                     params,
                     None,
                     Some(params.createPerQueryTempDir()))

        val action = (stagingTable: String) => {
          val updatedParams = MergedParameters(
            params.parameters.updated("dbtable", stagingTable))
          doSnowflakeLoad(conn,
                          data,
                          saveMode,
                          updatedParams,
                          filesToCopy,
                          None)
        }

        if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
          withStagingTable(conn, params.table.get, action)
        } else {
          doSnowflakeLoad(conn, data, saveMode, params, filesToCopy, None)
        }
      } finally {
        conn.close()
      }
    } else {

      val stageManager =
        new ConnectorSFStageManager(true, jdbcWrapper, params)

      try {

        stageManager.executeWithConnection(conn =>
          jdbcWrapper.executeInterruptibly(conn, prologueSql))

        val tempStage = Some("@" + stageManager.setupStageArea())

        val filesToCopy =
          unloadData(sqlContext, data, params, Some(stageManager), None)

        if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
          stageManager.executeWithConnection({ conn =>
            {
              val action = (stagingTable: String) => {
                val updatedParams = MergedParameters(
                  params.parameters.updated("dbtable", stagingTable))

                doSnowflakeLoad(conn,
                                data,
                                saveMode,
                                updatedParams,
                                filesToCopy,
                                tempStage)
              }
              withStagingTable(conn, params.table.get, action)
            }
          })
        } else {
          stageManager.executeWithConnection({ conn =>
            {
              doSnowflakeLoad(conn,
                              data,
                              saveMode,
                              params,
                              filesToCopy,
                              tempStage)
            }
          })

        }
      } finally {
        stageManager.closeConnection()
      }
    }
  }

  /**
    * Sets up a staging table then runs the given action, passing the temporary table name
    * as a parameter.
    */
  private def withStagingTable(conn: Connection,
                               table: TableName,
                               action: (String) => Unit) {
    val randomSuffix = Math.abs(Random.nextInt()).toString
    val tempTable    = TableName(s"${table.name}_staging_$randomSuffix")
    log.info(
      s"Loading new data for Snowflake table '$table' using temporary table '$tempTable'")

    try {
      action(tempTable.toString)

      if (jdbcWrapper.tableExists(conn, table.toString)) {
        // Rename temp table to final table. Use SWAP to make it atomic.bi
        sql(conn, s"ALTER TABLE $table SWAP WITH $tempTable")
      } else {
        // Table didn't exist, just rename temp table to it
        sql(conn, s"ALTER TABLE $tempTable RENAME TO $table")
      }
    } finally {
      // If anything went wrong (or if SWAP worked), delete the temp table
      sql(conn, s"DROP TABLE IF EXISTS $tempTable")
    }
  }

  // Execute a given string, logging it
  def sql(conn: Connection, query: String): Boolean = {
    log.debug(query)
    jdbcWrapper.executeInterruptibly(conn, query)
  }

  /**
    * Perform the Snowflake load, including deletion of existing data in the case of an overwrite,
    * and creating the table if it doesn't already exist.
    */
  private def doSnowflakeLoad(conn: Connection,
                              data: DataFrame,
                              saveMode: SaveMode,
                              params: MergedParameters,
                              filesToCopy: Option[(String, String)],
                              tempStage: Option[String]): Unit = {

    // Overwrites must drop the table, in case there has been a schema update
    if (saveMode == SaveMode.Overwrite) {
      val deleteStatement = s"DROP TABLE IF EXISTS ${params.table.get}"
      log.debug(deleteStatement)
      jdbcWrapper.executeInterruptibly(conn, deleteStatement)
    }

    // If the table doesn't exist, we need to create it first, using JDBC to infer column types
    val createStatement = createTableSql(data, params)
    log.debug(createStatement)
    jdbcWrapper.executeInterruptibly(conn, createStatement)

    // Execute preActions
    Utils.executePreActions(jdbcWrapper, conn, params)

    // Perform the load if there were files loaded
    if (filesToCopy.isDefined) {
      // Load the temporary data into the new file
      val copyStatement =
        copySql(data.sqlContext, params, filesToCopy.get, tempStage)
      log.debug(Utils.sanitizeQueryText(copyStatement))
      try {
        jdbcWrapper.executeInterruptibly(conn, copyStatement)
      } catch {
        case e: SQLException =>
          // snowflake-todo: try to provide more error information,
          // possibly from actual SQL output
          log.error("Error occurred while loading files to Snowflake: " + e)
          throw e
      }
    }

    Utils.executePostActions(jdbcWrapper, conn, params)
  }

  /**
    * Generate CREATE TABLE statement for Snowflake
    */
  // Visible for testing.
  private[snowflake] def createTableSql(data: DataFrame,
                                        params: MergedParameters): String = {
    val schemaSql = jdbcWrapper.schemaString(data.schema)
    // snowflake-todo: for now, we completely ignore
    // params.distStyle and params.distKey
    val table = params.table.get
    s"CREATE TABLE IF NOT EXISTS $table ($schemaSql)"
  }

  /**
    * Generate the COPY SQL command
    */
  private def copySql(sqlContext: SQLContext,
                      params: MergedParameters,
                      filesToCopy: (String, String),
                      tempStage: Option[String]): String = {
    val credsString =
      if (tempStage.isEmpty)
        AWSCredentialsUtils.getSnowflakeCredentialsString(sqlContext, params)
      else ""
    var fixedUrl           = filesToCopy._1
    var fromString: String = null
    if (fixedUrl.startsWith("file://")) {
      fromString =
        s"FROM '$fixedUrl' PATTERN='.*${filesToCopy._2}-\\\\d+(.gz|)'"
    } else {
      fixedUrl = Utils.fixS3Url(fixedUrl)
      val stage  = tempStage map (s => s + s"/${filesToCopy._2}")
      val source = stage.getOrElse(s"""'$fixedUrl${filesToCopy._2}'""")
      fromString = s"FROM $source"
    }
    s"""
       |COPY INTO ${params.table.get}
       |$fromString
       |$credsString
       |FILE_FORMAT = (
       |    TYPE=CSV
       |    /* COMPRESSION=none */
       |    FIELD_DELIMITER='|'
       |    /* ESCAPE='\\\\' */
       |    NULL_IF=()
       |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
       |    TIMESTAMP_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF3'
       |  )
    """.stripMargin.trim
  }

  /**
    * Serialize temporary data to S3, ready for Snowflake COPY
    *
    * @return the pair (URL, prefix) of the files that should be loaded,
    *         usually (tempDir, "part"),
    *         if at least one record was written,
    *         and None otherwise.
    */
  private def unloadData(sqlContext: SQLContext,
                         data: DataFrame,
                         params: MergedParameters,
                         stageMngr: Option[ConnectorSFStageManager],
                         temp: Option[String]): Option[(String, String)] = {

    @transient val stageManager = stageMngr.orNull
    // Prepare the set of conversion functions
    val conversionFunctions = genConversionFunctions(data.schema)

    // Use Spark accumulators to determine which partitions were non-empty.
    val nonEmptyPartitions =
      sqlContext.sparkContext.accumulableCollection(mutable.HashSet.empty[Int])

    val tempDir = temp.getOrElse("")

    // Create RDD that will be saved as strings
    val strRDD = data.rdd.mapPartitionsWithIndex {
      case (index, iter) =>
        new Iterator[String] {
          override def hasNext: Boolean = iter.hasNext

          override def next(): String = {
            nonEmptyPartitions += TaskContext.get().partitionId()

            if (iter.nonEmpty) {
              val row = iter.next()
              // Snowflake-todo: unify it with formatRow(), it's the same code
              // Build a simple pipe-delimited string
              val str = new mutable.StringBuilder
              var i   = 0
              while (i < conversionFunctions.length) {
                if (i > 0)
                  str.append('|')
                str.append(conversionFunctions(i)(row(i)))
                i += 1
              }
              if (!params.usingExternalStage) str.append(s"""\n""")
              str.toString()
            } else {
              ""
            }
          }
        }
    }

    if (params.usingExternalStage) {
      // Save, possibly with compression. Always use Gzip for now
      if (params.sfCompress) {
        strRDD.saveAsTextFile(tempDir, classOf[GzipCodec])
      } else {
        strRDD.saveAsTextFile(tempDir)
      }

      if (nonEmptyPartitions.value.isEmpty) {
        None
      } else {
        // Verify there was at least one file created.
        // The saved filenames are going to be of the form part*
        val fs = FileSystem.get(URI.create(tempDir),
                                sqlContext.sparkContext.hadoopConfiguration)
        fs.listStatus(new Path(tempDir))
          .iterator
          .map(_.getPath.getName)
          .filter(_.startsWith("part"))
          .take(1)
          .toSeq
          .headOption
          .getOrElse(throw new Exception("No part files were written!"))
        // Note - temp dir already has "/", no need to add it
        Some(tempDir, "part")
      }
    } else {

      @transient val keyIds = stageManager.getKeyIds

      val (_, queryId, smkId) =
        if (keyIds.nonEmpty) {
          keyIds.head
        } else ("", "", "")

      val awsID         = stageManager.awsId
      val awsKey        = stageManager.awsKey
      val awsToken      = stageManager.awsToken
      val is256         = stageManager.is256Encryption
      val masterKey     = stageManager.masterKey
      val stageLocation = stageManager.stageLocation

      strRDD.foreachPartition(rows => {

        val buffer = new ArrayBuffer[Byte]

        while (rows.hasNext) {
          val row = rows.next()
          buffer.appendAll(row.getBytes("UTF-8"))
        }

        val bytes = buffer.toArray

        val (bucketName, path) = extractBucketNameAndPath(stageLocation)

        val randFileName = Random.alphanumeric take 10 mkString ""

        var tempFileName = path + { if (!path.endsWith("/")) "/" else "" } + randFileName + ".csv"

        val meta = new ObjectMetadata

        val processedBytes =
          if (params.sfCompress) {
            val outStream = new ByteArrayOutputStream
            meta.setContentEncoding("GZIP")
            tempFileName = tempFileName + ".gz"
            val gzipStream = new GZIPOutputStream(outStream)
            gzipStream.write(bytes)
            gzipStream.close()
            outStream.toByteArray
          } else bytes

        var stream: InputStream = new ByteArrayInputStream(processedBytes)

        meta.setContentLength(processedBytes.length)
        SnowflakeConnectorUtils.log.debug(
          s"Using filename: $tempFileName, whose size is ${processedBytes.length} bytes.")

        if (!is256)
          stream = getEncryptedStream(stream, masterKey, queryId, smkId, meta)

        val digest =
          Base64.encodeAsString(DigestUtils.sha256(processedBytes): _*)

        meta.addUserMetadata("sfc-digest", digest)

        val amazonClient = createS3Client(is256,
                                          masterKey,
                                          queryId,
                                          smkId,
                                          awsID,
                                          awsKey,
                                          awsToken)

        val tx =
          new TransferManager(amazonClient,
                              SnowflakeUtil.createDefaultExecutorService(
                                "s3-transfer-manager-uploader-",
                                DEFAULT_PARALLELISM))

        try {

          val upload = tx.upload(bucketName, tempFileName, stream, meta)

          upload.waitForCompletion()

          val pathFileName = path + { if (!path.endsWith("/")) "/" else "" } + "complete-" + randFileName + ".csv" + {
            if (params.sfCompress) ".gz" else ""
          }

          SnowflakeConnectorUtils.log.debug(
            s"Completed upload. Renaming file from $tempFileName to $pathFileName.")

          // Only successful and completed transfers should be loaded, which avoids duplicate rows
          amazonClient
            .copyObject(bucketName, tempFileName, bucketName, pathFileName)

        } catch {
          case ex: Exception =>
            SnowflakeConnectorUtils.handleS3Exception(ex)
        } finally {
          tx.shutdownNow(true)
          stream.close()
        }
      })

      Some("s3n://" + stageLocation, "complete")
    }
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
      DefaultJDBCWrapper,
      awsCredentials => new AmazonS3Client(awsCredentials))
