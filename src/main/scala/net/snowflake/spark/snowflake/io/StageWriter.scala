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
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
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
                    jdbcWrapper: JDBCWrapper,
                    format: SupportedFormat
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

          val tempStage = createTempStage(filesToCopy._1, params, conn, jdbcWrapper)

          writeToTable(sqlContext, conn, rdd, schema, saveMode, params,
            jdbcWrapper, filesToCopy._2, tempStage, format)

          //purge files after loading
          if (params.purge()) {
            val fs = FileSystem.get(URI.create(filesToCopy._1), sqlContext.sparkContext.hadoopConfiguration)
            fs.delete(new Path(filesToCopy._1), true)
          }

        } finally {
          SnowflakeTelemetry.send(jdbcWrapper.getTelemetry(conn))
          conn.close()
        }
      case SupportedSource.INTERNAL =>
        val stageManager = new SFInternalStage(true, jdbcWrapper, params)

        try {
          stageManager.executeWithConnection(
            jdbcWrapper.executeInterruptibly(_, prologueSql)
          )
          val tempStage = stageManager.setupStageArea()

          val file =
            unloadData(sqlContext, rdd, params, source, Some(stageManager))._2

          stageManager.executeWithConnection(
            writeToTable(sqlContext, _, rdd, schema, saveMode, params,
              jdbcWrapper, file, tempStage, format)
          )
        } finally {
          stageManager.closeConnection()
        }
    }

  }


  /**
    * load data from stage to table
    */
  private def writeToTable(
                            sqlContext: SQLContext,
                            conn: Connection,
                            data: RDD[String],
                            schema: StructType,
                            saveMode: SaveMode,
                            params: MergedParameters,
                            jdbcWrapper: JDBCWrapper,
                            file: String,
                            tempStage: String,
                            format: SupportedFormat
                          ): Unit = {
    def execute(statement: String): Unit = {
      log.debug(statement)
      jdbcWrapper.executeInterruptibly(conn, statement)
    }

    val table = params.table.get
    val tempTable =
      TableName(s"${table.name}_staging_${Math.abs(Random.nextInt()).toString}")
    val targetTable = if (saveMode == SaveMode.Overwrite
      && params.useStagingTable) tempTable else table

    // Load the temporary data into the new file
    val copyStatement =
      copySql(sqlContext, data, schema, saveMode, params,
        targetTable, file, tempStage, format)

    try {
      //purge tables when overwriting
      if (saveMode == SaveMode.Overwrite &&
        jdbcWrapper.tableExists(conn, table.toString)) {
        if (params.useStagingTable) {
          if (params.truncateTable)
            execute(s"create or replace table $tempTable like $table")
        }
        else if (params.truncateTable) execute(s"truncate $table")
        else execute(s"drop table if exists $table")
      }

      //create table
      execute(createTableSql(data, schema, targetTable, jdbcWrapper))

      //pre actions
      Utils.executePreActions(jdbcWrapper, conn, params, Option(targetTable))

      //copy
      log.debug(Utils.sanitizeQueryText(copyStatement))
      //todo: handle on_error parameter on spark side
      //jdbcWrapper.executeInterruptibly(conn, copyStatement)

      //report the number of skipped files.
      val resultSet = jdbcWrapper.executeQueryInterruptibly(conn, copyStatement)
      if (params.continueOnError) {
        var rowSkipped: Long = 0l
        while (resultSet.next()) {
          rowSkipped +=
            resultSet.getLong("rows_parsed") -
              resultSet.getLong("rows_loaded")
        }
        log.error(s"ON_ERROR: Continue -> Skipped $rowSkipped rows")
      }
      Utils.setLastCopyLoad(copyStatement)

      //post actions
      Utils.executePostActions(jdbcWrapper, conn, params, Option(targetTable))

      if (saveMode == SaveMode.Overwrite && params.useStagingTable) {
        if (jdbcWrapper.tableExists(conn, table.toString))
          execute(s"alter table $table swap with $tempTable")
        else
          execute(s"alter table $tempTable rename to $table")
      }
    } catch {
      case e: SQLException =>
        // snowflake-todo: try to provide more error information,
        // possibly from actual SQL output
        log.error("Error occurred while loading files to Snowflake: " + e)
        throw e
    }
    finally {
      if (targetTable == targetTable)
        execute(s"drop table if exists $tempTable")
    }


  }

  /**
    * Generate CREATE TABLE statement for Snowflake
    */
  // Visible for testing.
  private[snowflake] def createTableSql(
                                         data: RDD[String],
                                         schema: StructType,
                                         table: TableName,
                                         jdbcWrapper: JDBCWrapper
                                       ): String = {
    val schemaSql = jdbcWrapper.schemaString(schema)
    // snowflake-todo: for now, we completely ignore
    // params.distStyle and params.distKey
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
                       table: TableName,
                       file: String,
                       tempStage: String,
                       format: SupportedFormat
                     ): String = {

    if (saveMode != SaveMode.Append && params.columnMap.isDefined)
      throw new UnsupportedOperationException("The column mapping only works in append mode.")

    def getMappingToString(list: Option[List[(Int, String)]]): String = {
      if (format == SupportedFormat.JSON)
        s"(${schema.fields.map(_.name).mkString(",")})"
      else if(list.isEmpty || list.get.isEmpty) ""
      else
        s"(${list.get.map(x => Utils.ensureQuoted(x._2)).mkString(", ")})"
    }

    def getMappingFromString(list: Option[List[(Int, String)]], from: String): String = {
      if(format == SupportedFormat.JSON){
        val names = schema.fields.map(x=>"parse_json($1):".concat(x.name)).mkString(",")
        s"from (select $names $from tmp)"
      }
      else if (list.isEmpty || list.get.isEmpty) from
      else
        s"from (select ${list.get.map(x => "tmp.$".concat(x._1.toString)).mkString(", ")} $from tmp)"

    }

    val fromString = s"FROM @$tempStage/$file"

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
          s"""
             |FILE_FORMAT = (
             |    TYPE=CSV
             |    FIELD_DELIMITER='|'
             |    NULL_IF=()
             |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
             |    TIMESTAMP_FORMAT='TZHTZM YYYY-MM-DD HH24:MI:SS.FF3'
             |  )
           """.stripMargin
        case SupportedFormat.JSON =>
          s"""
             |FILE_FORMAT = (
             |    TYPE = JSON
             |)
           """.stripMargin
      }

    val truncateCol =
      if (params.truncateColumns())
        "TRUNCATECOLUMNS = TRUE"
      else
        ""

    val purge = if (params.purge()) "PURGE = TRUE" else ""

    val onError = if (params.continueOnError) "ON_ERROR = CONTINUE" else ""

    /** TODO(etduwx): Refactor this to be a collection of different options, and use a mapper
      * function to individually set each file_format and copy option. */

    s"""
       |COPY INTO $table $mappingToString
       |$mappingFromString
       |$formatString
       |  $truncateCol
       |  $purge
       |  $onError
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
                        ): (String, String) = {

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

            ("s3n://" + stageLocation, "")

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

              while (rows.hasNext) {
                outputStream.write(rows.next.getBytes("UTF-8"))
                outputStream.write('\n')
              }
              outputStream.close()
            })

            (s"wasb://$azureAccount.$azureEndpoint/$stageLocation", "")

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
        (tempDir, "part")
    }


  }

  /**
    * create temporary external stage for given path
    */
  @deprecated
  private[io] def createTempStage(
                                   path: String,
                                   params: MergedParameters,
                                   conn: Connection,
                                   jdbcWrapper: JDBCWrapper
                                 ): String = {
    def convertURL(url: String): String = {
      val azure_url = "wasbs?://([^@]+)@([^\\.]+)\\.([^/]+)/(.+)?".r
      val s3_url = "s3[an]://(.+)".r

      url match {
        case azure_url(container, account, endpoint, path) =>
          s"azure://$account.$endpoint/$container/$path"
        case s3_url(path) =>
          s"s3://$path"
        case _ =>
          throw new IllegalArgumentException(s"invalid url: $url")
      }

    }

    val stageName = s"tmp_spark_stage_${Random.alphanumeric take 10 mkString ""}"
    val urlString = convertURL(path)
    val credString =
      params.rootTempDirStorageType match {
        case FSType.Azure =>
          val sas = params.azureSAS
          if (sas.isEmpty) throw new IllegalArgumentException("Azure SAS is undefined")
          s"(azure_sas_token='${sas.get}')"
        case FSType.S3 =>
          val awsAccessKey = params.awsAccessKey
          val awsSecretKey = params.awsSecretKey
          if (awsAccessKey.isEmpty || awsSecretKey.isEmpty)
            throw new IllegalArgumentException("AWS credential is undefined")
          s"(aws_key_id='${awsAccessKey.get}' aws_secret_key='${awsSecretKey.get}')"

        case _ =>
          throw new UnsupportedOperationException("Only support Azure and S3 external stage")
      }

    val stageString =
      s"""
         |create or replace temporary stage $stageName
         |url = '$urlString'
         |credentials=$credString
       """.stripMargin

    jdbcWrapper.executeInterruptibly(conn, stageString)

    stageName
  }


}
