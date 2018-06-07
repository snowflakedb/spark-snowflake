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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}
import java.util.zip.GZIPInputStream

import net.snowflake.client.jdbc._
import net.snowflake.client.jdbc.cloud.storage.StageInfo
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.blob.{CloudBlob, CloudBlobClient, CloudBlobContainer}
import net.snowflake.spark.snowflake.io.SFInternalStage._
import net.snowflake.spark.snowflake.{JDBCWrapper, SnowflakeConnectorUtils}
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.io.SupportedFormat.SupportedFormat
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory


private[io] class SFInternalPartition(
    val srcFiles: Seq[(java.lang.String, java.lang.String, java.lang.String)],
    val rddId: Int,
    val index: Int)
    extends Partition {

  override def hashCode(): Int = 31 * (31 + rddId) + index

  override def equals(other: Any): Boolean = super.equals(other)
}

private[io] class SFInternalRDD(
                                        @transient val sqlContext: SQLContext,
                                        @transient val params: MergedParameters,
                                        @transient val sql: String,
                                        @transient val jdbcWrapper: JDBCWrapper,
                                        val format: SupportedFormat = SupportedFormat.CSV
) extends RDD[String](sqlContext.sparkContext, Nil) with DataUnloader {

  @transient override val log = LoggerFactory.getLogger(getClass)
  @transient private val stageManager =
    new SFInternalStage(false, jdbcWrapper, params)

  @transient val tempStage = stageManager.setupStageArea()

  @transient private val FILES_PER_PARTITION = 2

  private val compress = if (params.sfCompress) "gzip" else "none"
  private val parallel = params.parallelism

  private[io] val rowCount = stageManager
    .executeWithConnection({ c =>
      setup(preStatements = Seq.empty,
            sql = buildUnloadStmt(sql, s"@$tempStage", compress, None),
            conn = c,
            keepOpen = true)
    })
    .asInstanceOf[Long]

  private val stageLocation = stageManager.stageLocation
  private val awsID         = stageManager.awsId
  private val awsKey        = stageManager.awsKey
  private val awsToken      = stageManager.awsToken
  private val masterKey     = stageManager.masterKey
  private val is256         = stageManager.is256Encryption

  private val azureSAS      = stageManager.azureSAS
  private val azureAccount  = stageManager.azureAccountName
  private val azureEndpoint = stageManager.azureEndpoint

  private val stageType: StageInfo.StageType = stageManager.stageType

  override def getPartitions: Array[Partition] = {

    val encryptionMaterialsGrouped =
      stageManager.getKeyIds.grouped(FILES_PER_PARTITION).toList

    val partitions = new Array[Partition](encryptionMaterialsGrouped.length)

    var i = 0

    while (i < encryptionMaterialsGrouped.length) {
      partitions(i) =
        new SFInternalPartition(encryptionMaterialsGrouped(i), id, i)
      i = i + 1
    }
    partitions
  }

  override def compute(thePartition: Partition,
                       context: TaskContext): Iterator[String] = {

    val mats   = thePartition.asInstanceOf[SFInternalPartition].srcFiles
    val stringIterator = new StringIterator(format)

    try {
      mats.foreach {
        case (file, queryId, smkId) =>
          if (queryId != null) {
            var stream: InputStream = null
            val (bucketName, stagePath) =
              extractBucketNameAndPath(stageLocation)
            var stageFilePath = file
            if (!stagePath.isEmpty) {
              stageFilePath =
                SnowflakeUtil.concatFilePathNames(stagePath, file, "/")
            }


            stageType match {
              case StageInfo.StageType.S3 =>
                val amazonClient =
                  createS3Client(is256,
                    masterKey,
                    queryId,
                    smkId,
                    awsID.get,
                    awsKey.get,
                    awsToken.get,
                    parallel)

                val dataObject = amazonClient.getObject(bucketName, stageFilePath)
                stream = dataObject.getObjectContent
                if (!is256) {
                  stream = getDecryptedStream(stream,
                    masterKey,
                    dataObject.getObjectMetadata.getUserMetadata,
                    stageType)
                }
              case StageInfo.StageType.AZURE =>

                val azureClient: CloudBlobClient =
                  createAzureClient(
                    azureAccount.get,
                    azureEndpoint.get,
                    azureSAS
                  )

                val container: CloudBlobContainer = azureClient.getContainerReference(bucketName)
                val blob: CloudBlob = container.getBlockBlobReference(stageFilePath)
                val azureStream: ByteArrayOutputStream = new ByteArrayOutputStream()
                blob.download(azureStream)
                blob.downloadAttributes()

                stream = new ByteArrayInputStream(azureStream.toByteArray)
                stream = getDecryptedStream(stream, masterKey, blob.getMetadata, stageType)

              case _ =>
                throw new UnsupportedOperationException(
                  s"Only support S3 or Azure stage, stage type: $stageType ")
            }


            stream = compress match {
              case "gzip" => new GZIPInputStream(stream)
              case _      => stream
            }

            stringIterator.addStream(stream)
          }
      }
    } catch {
      case ex: Exception =>
        stringIterator.closeAll()
        SnowflakeConnectorUtils.handleS3Exception(ex)
    }
    new InterruptibleIterator[String](context, stringIterator)


  }

  override def finalize(): Unit = {
    stageManager.closeConnection()
    super.finalize()
  }
}
