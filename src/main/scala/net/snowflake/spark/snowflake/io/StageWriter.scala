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

import java.io.OutputStream
import java.net.URI
import java.sql.{Connection, SQLException}
import java.util.zip.GZIPOutputStream

import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.ObjectMetadata
import net.snowflake.client.jdbc.internal.amazonaws.auth.AWSCredentials
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3Client
import javax.crypto.{Cipher, CipherOutputStream}
import net.snowflake.client.jdbc.cloud.storage.StageInfo.StageType
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.{CloudBlobClient, CloudBlobContainer, CloudBlockBlob}
import net.snowflake.spark.snowflake.io.SFInternalStage._
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.SupportedSource.SupportedSource
import net.snowflake.spark.snowflake.s3upload.StreamTransferManager
import net.snowflake.spark.snowflake._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.slf4j.LoggerFactory

import scala.util.Random

private[io] object StageWriter {

  private val log = LoggerFactory.getLogger(getClass)

  def writeToStage(
                 rdd: RDD[String],
                 schema: StructType,
                 sqlContext: SQLContext,
                 saveMode: SaveMode,
                 params: MergedParameters,
                 jdbcWrapper: JDBCWrapper
               ): Unit = {

    val source: SupportedSource =
      if (params.usingExternalStage) SupportedSource.EXTERNAL
      else SupportedSource.INTERNAL


    if (params.table.isEmpty) {
      throw new IllegalArgumentException(
        "For save operations you must specify a Snowflake table name with the 'dbtable' parameter")
    }
    val prologueSql = Utils.genPrologueSql(params)
    log.debug(prologueSql)


    source match {
      case SupportedSource.EXTERNAL =>
        Utils.checkFileSystem(
          new URI(params.rootTempDir),
          sqlContext.sparkContext.hadoopConfiguration)

        if (params.checkBucketConfiguration) {
          // For now it is only needed for AWS, so put the following under the
          // check. checkBucketConfiguration implies we are using S3.
          val creds = CloudCredentialsUtils.getAWSCreds(sqlContext, params)

          val s3ClientFactory: AWSCredentials => AmazonS3Client
          = awsCredentials => new AmazonS3Client(awsCredentials)

          Utils.checkThatBucketHasObjectLifecycleConfiguration(
            params.rootTempDir,
            params.rootTempDirStorageType,
            s3ClientFactory(creds))
        }

        val conn = jdbcWrapper.getConnector(params)

        try {
          jdbcWrapper.executeInterruptibly(conn, prologueSql)

          val filesToCopy =
            unloadData(sqlContext, rdd, params, source, None)

          val action = (stagingTable: String) => {
            val updatedParams = MergedParameters(
              params.parameters.updated("dbtable", stagingTable))
            doSnowflakeLoad(
              sqlContext,
              conn,
              rdd,
              schema,
              saveMode,
              updatedParams,
              jdbcWrapper,
              filesToCopy,
              None)
          }

          if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
            withStagingTable(conn, jdbcWrapper, params.table.get, action)
          } else {
            doSnowflakeLoad(
              sqlContext,
              conn,
              rdd,
              schema,
              saveMode,
              params,
              jdbcWrapper,
              filesToCopy,
              None)
          }

          //purge files after loading
          if(params.purge()){
            val fs = FileSystem.get(URI.create(filesToCopy.get._1), sqlContext.sparkContext.hadoopConfiguration)
            fs.delete(new Path(filesToCopy.get._1), true)
          }

        } finally {
          SnowflakeTelemetry.send(jdbcWrapper.getTelemetry(conn))
          conn.close()
        }
      case SupportedSource.INTERNAL =>
        val stageManager = new SFInternalStage(true, jdbcWrapper, params)

        try {
          stageManager.executeWithConnection(conn =>
            jdbcWrapper.executeInterruptibly(conn, prologueSql))

          val tempStage = Some("@" + stageManager.setupStageArea())

          val filesToCopy =
            unloadData(sqlContext, rdd, params, source, Some(stageManager))

          if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
            stageManager.executeWithConnection({ conn => {
              val action = (stagingTable: String) => {
                val updatedParams = MergedParameters(
                  params.parameters.updated("dbtable", stagingTable))

                doSnowflakeLoad(
                  sqlContext,
                  conn,
                  rdd,
                  schema,
                  saveMode,
                  updatedParams,
                  jdbcWrapper,
                  filesToCopy,
                  tempStage)
              }
              withStagingTable(conn, jdbcWrapper, params.table.get, action)
            }
            })
          } else {
            stageManager.executeWithConnection({ conn => {
              doSnowflakeLoad(
                sqlContext,
                conn,
                rdd,
                schema,
                saveMode,
                params,
                jdbcWrapper,
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
  private def withStagingTable(
                                conn: Connection,
                                jdbcWrapper: JDBCWrapper,
                                table: TableName,
                                action: (String) => Unit
                              ): Unit = {
    val randomSuffix = Math.abs(Random.nextInt()).toString
    val tempTable = TableName(s"${table.name}_staging_$randomSuffix")
    log.info(
      s"Loading new data for Snowflake table '$table' using temporary table '$tempTable'")

    try {
      action(tempTable.toString)

      if (jdbcWrapper.tableExists(conn, table.toString)) {
        // Rename temp table to final table. Use SWAP to make it atomic.bi
        sql(conn, s"ALTER TABLE $table SWAP WITH $tempTable", jdbcWrapper)
      } else {
        // Table didn't exist, just rename temp table to it
        sql(conn, s"ALTER TABLE $tempTable RENAME TO $table", jdbcWrapper)
      }
    } finally {
      // If anything went wrong (or if SWAP worked), delete the temp table
      sql(conn, s"DROP TABLE IF EXISTS $tempTable", jdbcWrapper)
    }
  }

  // Execute a given string, logging it
  def sql(conn: Connection, query: String, jdbcWrapper: JDBCWrapper): Boolean = {
    log.debug(query)
    jdbcWrapper.executeInterruptibly(conn, query)
  }


  /**
    * Perform the Snowflake load, including deletion of existing data in the case of an overwrite,
    * and creating the table if it doesn't already exist.
    */
  private def doSnowflakeLoad(
                               sqlContext: SQLContext,
                               conn: Connection,
                               data: RDD[String],
                               schema: StructType,
                               saveMode: SaveMode,
                               params: MergedParameters,
                               jdbcWrapper: JDBCWrapper,
                               filesToCopy: Option[(String, String)],
                               tempStage: Option[String]): Unit = {

    // Overwrites must drop the table, in case there has been a schema update
    if (saveMode == SaveMode.Overwrite) {
      val deleteStatement = s"DROP TABLE IF EXISTS ${params.table.get}"
      log.debug(deleteStatement)
      jdbcWrapper.executeInterruptibly(conn, deleteStatement)
    }

    // If the table doesn't exist, we need to create it first, using JDBC to infer column types
    val createStatement = createTableSql(data, schema, params, jdbcWrapper)
    log.debug(createStatement)
    jdbcWrapper.executeInterruptibly(conn, createStatement)

    // Execute preActions
    Utils.executePreActions(jdbcWrapper, conn, params)

    // Perform the load if there were files loaded
    if (filesToCopy.isDefined) {
      // Load the temporary data into the new file
      val copyStatement =
        copySql(sqlContext, data, schema, saveMode, params, filesToCopy.get, tempStage)
      log.debug(Utils.sanitizeQueryText(copyStatement))
      try {
        jdbcWrapper.executeInterruptibly(conn, copyStatement)
        Utils.setLastCopyLoad(copyStatement)
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
  private[snowflake] def createTableSql(
                                         data: RDD[String],
                                         schema: StructType,
                                         params: MergedParameters,
                                         jdbcWrapper: JDBCWrapper
                                       ): String = {
    val schemaSql = jdbcWrapper.schemaString(schema)
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
                       data: RDD[String],
                       schema: StructType,
                       saveMode: SaveMode,
                       params: MergedParameters,
                       filesToCopy: (String, String),
                       tempStage: Option[String]): String = {

    if (saveMode != SaveMode.Append && params.columnMap.isDefined)
      throw new UnsupportedOperationException("The column mapping only works in append mode.")

    def getMappingToString(list: Option[List[(Int, String)]]): String = {
      if (list.isEmpty || list.get.isEmpty) ""
      else
        s"(${list.get.map(x => Utils.ensureQuoted(x._2)).mkString(", ")})"
    }

    def getMappingFromString(list: Option[List[(Int, String)]], from: String): String = {
      if (list.isEmpty || list.get.isEmpty) from
      else
        s"from (select ${list.get.map(x => "tmp.$".concat(x._1.toString)).mkString(", ")} $from tmp)"

    }

    val credsString =
      if (tempStage.isEmpty)
        CloudCredentialsUtils.getSnowflakeCredentialsString(sqlContext, params)
      else ""

    var fixedUrl = filesToCopy._1
    var fromString: String = null
    if (fixedUrl.startsWith("file://")) {
      fromString =
        s"FROM '$fixedUrl' PATTERN='.*${filesToCopy._2}-\\\\d+(.gz|)'"
    } else {
      fixedUrl = Utils.fixUrlForCopyCommand(fixedUrl)
      val stage = tempStage map (s => s + s"/${filesToCopy._2}")
      val source = stage.getOrElse(s"""'$fixedUrl${filesToCopy._2}'""")
      fromString = s"FROM $source"
    }

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

    val truncateCol =
      if (params.truncateColumns())
        "TRUNCATECOLUMNS = TRUE"
      else
        ""

    val purge = if (params.purge()) "PURGE = TRUE" else ""

    /** TODO(etduwx): Refactor this to be a collection of different options, and use a mapper
      * function to individually set each file_format and copy option. */

    s"""
       |COPY INTO ${params.table.get} $mappingToString
       |$mappingFromString
       |
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
       |  $truncateCol
       |  $purge
    """.stripMargin.trim
  }


  /**
    * Serialize temporary data to cloud storage (S3 or Azure), ready for
    * Snowflake COPY
    *
    * @return the pair (URL, prefix) of the files that should be loaded,
    *         usually (tempDir, "part"),
    *         if at least one record was written,
    *         and None otherwise.
    */
  private def unloadData(sqlContext: SQLContext,
                         data: RDD[String],
                         params: MergedParameters,
                         source: SupportedSource,
                         stageMngr: Option[SFInternalStage]
                        ): Option[(String, String)] = {

    @transient val stageManager = stageMngr.orNull

    source match {
      case SupportedSource.INTERNAL =>
        @transient val keyIds = stageManager.getKeyIds
        val (_, queryId, smkId) = if (keyIds.nonEmpty) keyIds.head else ("", "", "")
        val masterKey = stageManager.masterKey
        val stageLocation = stageManager.stageLocation
        val (bucketName, path) = extractBucketNameAndPath(stageLocation)

        val is256 = stageManager.is256Encryption


        stageManager.stageType match {

          case StageType.S3 =>

            val awsID = stageManager.awsId
            val awsKey = stageManager.awsKey
            val awsToken = stageManager.awsToken

            data.foreachPartition(rows => {

              val randStr = Random.alphanumeric take 10 mkString ""
              var fileName = path + {
                if (!path.endsWith("/")) "/" else ""
              } + randStr + ".csv"

              val amazonClient = createS3Client(is256,
                masterKey,
                queryId,
                smkId,
                awsID.get,
                awsKey.get,
                awsToken.get)
              val (fileCipher: Cipher, meta: ObjectMetadata) =
                if (!is256)
                  getCipherAndS3Metadata(masterKey, queryId, smkId)
                else (_: Cipher, new ObjectMetadata())

              if (params.sfCompress)
                meta.setContentEncoding("GZIP")
              val parallelism = params.parallelism.getOrElse(1)

              val streamManager = new StreamTransferManager(
                bucketName,
                fileName,
                amazonClient,
                meta,
                1, //numStreams
                parallelism, //numUploadThreads.
                5 * parallelism, //queueCapacity
                50) //partSize: Max 10000 parts, 50MB * 10K = 500GB per partition limit

              try {
                // TODO: Can we parallelize this write? Currently we don't because the Row Iterator is not thread safe.

                val uploadOutStream = streamManager.getMultiPartOutputStreams.get(0)
                var outStream: OutputStream = uploadOutStream

                if (!is256)
                  outStream = new CipherOutputStream(outStream, fileCipher)

                if (params.sfCompress) {
                  fileName = fileName + ".gz"
                  outStream = new GZIPOutputStream(outStream)
                }

                SnowflakeConnectorUtils.log.debug("Begin upload.")

                while (rows.hasNext) {
                  outStream.write(rows.next.getBytes("UTF-8"))
                  outStream.write('\n')
                  uploadOutStream.checkSize()
                }

                outStream.close()

                SnowflakeConnectorUtils.log.debug(
                  "Completed S3 upload for partition.")

                streamManager.complete()

              } catch {
                case ex: Exception =>
                  streamManager.abort()
                  SnowflakeConnectorUtils.handleS3Exception(ex)
              }
            })

            Some("s3n://" + stageLocation, "")

          case StageType.AZURE =>

            val azureSAS = stageManager.azureSAS
            val azureAccount = stageManager.azureAccountName
            val azureEndpoint = stageManager.azureEndpoint


            data.foreachPartition(rows => {

              val randStr = Random.alphanumeric take 10 mkString ""
              val fileName = path + {
                if (!path.endsWith("/")) "/" else ""
              } + randStr + ".csv"

              val azureClient: CloudBlobClient =
                createAzureClient(
                  azureAccount.get,
                  azureEndpoint.get,
                  azureSAS
                )

              val (cipher, meta) = getCipherAndAZMetaData(masterKey, queryId, smkId)
              val outputFileName =
                if (params.sfCompress) fileName.concat("gz") else fileName

              val container: CloudBlobContainer = azureClient.getContainerReference(bucketName)
              val blob: CloudBlockBlob = container.getBlockBlobReference(outputFileName)
              blob.setMetadata(meta)

              val EncryptedStream = new CipherOutputStream(blob.openOutputStream(), cipher)

              val outputStream: OutputStream =
                if (params.sfCompress) new GZIPOutputStream(EncryptedStream)
                else EncryptedStream

              while(rows.hasNext) {
                outputStream.write(rows.next.getBytes("UTF-8"))
                outputStream.write('\n')
              }
              outputStream.close()
            })

            Some(s"wasb://$azureAccount.$azureEndpoint/$stageLocation","")

          case _ =>
            throw new UnsupportedOperationException(
              s"Only support S3 or Azure stage, stage type: ${stageManager.stageType} ")
        }

      case SupportedSource.EXTERNAL =>
        val tempDir = params.createPerQueryTempDir()
        // Save, possibly with compression. Always use Gzip for now
        if (params.sfCompress) {
          data.saveAsTextFile(tempDir, classOf[GzipCodec])
        } else {
          data.saveAsTextFile(tempDir)
        }

        // Verify there was at least one file created.
        // The saved filenames are going to be of the form part*
        val fs = FileSystem
          .get(URI.create(tempDir), sqlContext.sparkContext.hadoopConfiguration)
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


  }

}
