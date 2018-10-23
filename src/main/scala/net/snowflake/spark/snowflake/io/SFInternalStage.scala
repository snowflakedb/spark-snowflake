/*
 * Copyright 2017 - 2018 Snowflake Computing
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

import net.snowflake.client.jdbc.internal.amazonaws.util.Base64
import net.snowflake.client.core.SFStatement
import net.snowflake.client.jdbc._
import net.snowflake.client.jdbc.cloud.storage.StageInfo
import net.snowflake.spark.snowflake._
import net.snowflake.spark.snowflake.Parameters.MergedParameters

import scala.collection.JavaConverters._
import scala.util.Random



private[io] class SFInternalStage(isWrite: Boolean,
                                  params: MergedParameters,
                                  tempStage: String,
                                  connection: SnowflakeConnectionV1,
                                  fileName: String = ""
                                 ) {

  private[io] final val DUMMY_LOCATION =
    "file:///tmp/dummy_location_spark_connector_tmp/"
  private[io] final val CREATE_TEMP_STAGE_STMT =
    s"""CREATE OR REPLACE TEMP STAGE """

  private[io] final def TEMP_STAGE_LOCATION: String =
    "spark_connector_unload_stage_" + (Random.alphanumeric take 10 mkString "")


  private lazy val sfAgent = {
    Utils.setLastPutCommand(command)
    Utils.setLastGetCommand(command)

    new SnowflakeFileTransferAgent(command,
      connection.getSfSession,
      new SFStatement(connection.getSfSession))
  }

  private lazy val encryptionMaterials = sfAgent.getEncryptionMaterial
  private lazy val srcMaterialsMap =
    if (!isWrite) sfAgent.getSrcToMaterialsMap else null
  private lazy val stageCredentials = sfAgent.getStageCredentials

  private lazy val stageInfo: StageInfo = sfAgent.getStageInfo

  private lazy val encMat =
    if (encryptionMaterials.size() > 0) {
      encryptionMaterials.get(0)
    } else null

  private[io] lazy val stageType: StageInfo.StageType =
    stageInfo.getStageType

  //try get aws credentials
  private[io] lazy val awsId: Option[String] =
    if (stageType == StageInfo.StageType.S3)
      Option(stageCredentials.get("AWS_ID").toString)
    else None

  private[io] lazy val awsKey: Option[String] =
    if (stageType == StageInfo.StageType.S3)
      Option(stageCredentials.get("AWS_KEY").toString)
    else None

  private[io] lazy val awsToken: Option[String] =
    if (stageType == StageInfo.StageType.S3)
      Option(stageCredentials.get("AWS_TOKEN").toString)
    else None

  //try get azure credentials

  private[io] lazy val azureSAS: Option[String] =
    if (stageType == StageInfo.StageType.AZURE)
      Option(stageCredentials.get("AZURE_SAS_TOKEN").toString)
    else None

  private[io] lazy val azureEndpoint: Option[String] =
    if (stageType == StageInfo.StageType.AZURE)
      Option(stageInfo.getEndPoint)
    else None

  private[io] lazy val azureAccountName: Option[String] =
    if (stageType == StageInfo.StageType.AZURE)
      Option(stageInfo.getStorageAccount)
    else None

  private[io] lazy val stageLocation: String =
    sfAgent.getStageLocation

  private[io] lazy val getKeyIds: Seq[(String, String, String)] = {
    if (srcMaterialsMap != null) {
      srcMaterialsMap.asScala.toList.map {
        case (k, v) =>
          (k,
            if (v != null) v.getQueryId else null,
            if (v != null) v.getSmkId.toString else null)
      }
    } else {
      encryptionMaterials.asScala map { encMat =>
        ("", encMat.getQueryId, encMat.getSmkId.toString)
      }
    }
  }

  private[io] lazy val masterKey =
    if (encMat != null) encMat.getQueryStageMasterKey else null
  private[io] lazy val decodedKey =
    if (masterKey != null) Base64.decode(masterKey) else null

  private[io] lazy val is256Encryption: Boolean = {
    val length = if (decodedKey != null) decodedKey.length * 8 else 128
    if (length == 256)
      true
    else if (length == 128)
      false
    else
      throw new SnowflakeConnectorException(s"Unsupported Key Size: $length")
  }


  private val dummyLocation = DUMMY_LOCATION

  private val command = {
    val comm =
      if (isWrite)
        s"PUT $dummyLocation @$tempStage"
      else
        s"GET @$tempStage/$fileName $dummyLocation"

    if (params.parallelism.isDefined) {
      comm + s" PARALLEL=${params.parallelism.get}"
    }
    else
      comm
  }

  private[io] def getEncryptionMaterials = encryptionMaterials

}
