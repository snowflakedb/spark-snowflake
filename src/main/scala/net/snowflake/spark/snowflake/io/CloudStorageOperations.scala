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
package net.snowflake.spark.snowflake.io

import java.io.{InputStream, OutputStream}
import java.sql.Connection

import net.snowflake.client.jdbc.internal.amazonaws.ClientConfiguration
import net.snowflake.client.jdbc.internal.amazonaws.auth.{AWSCredentials, BasicAWSCredentials, BasicSessionCredentials}
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3Client
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.DeleteObjectsRequest
import net.snowflake.spark.snowflake.DefaultJDBCWrapper
import net.snowflake.spark.snowflake.Parameters.MergedParameters

import scala.util.Random

object CloudStorageOperations {
  private[io] final val DEFAULT_PARALLELISM = 10
  private[io] final val S3_MAX_RETRIES = 3


  /**
    * @return Storage client and stage name
    */
  def createStorageClient(param: MergedParameters, conn: Connection): (CloudStorage, String) = {
    val azure_url = "wasbs?://([^@]+)@([^\\.]+)\\.([^/]+)/(.+)?".r
    val s3_url = "s3[an]://([^/]+)/(.*)".r

    param.rootTempDir match {
      //External Stage
      case azure_url(container, account, endpoint, path) =>
        //todo
        throw new UnsupportedOperationException("Not support azure in streaming")
      case s3_url(bucket, prefix) =>
        require(param.awsAccessKey.isDefined, "missing aws access key")
        require(param.awsSecretKey.isDefined, "missing aws secret key")

        val stageName = s"tmp_spark_stage_${Random.alphanumeric take 10 mkString ""}"

        val sql =
          s"""
             |create or replace temporary stage $stageName
             |url = 's3://$bucket/$prefix'
             |credentials =
             |(aws_key_id='${param.awsAccessKey.get}' aws_secret_key='${param.awsSecretKey.get}')
         """.stripMargin

        DefaultJDBCWrapper.executeQueryInterruptibly(conn, sql)

        (S3Storage(bucket, param.awsAccessKey.get, param.awsSecretKey.get, prefix), stageName)
      case _ => // Internal Stage
        throw new UnsupportedOperationException("Not support internal stage")
    }
  }


}

sealed trait CloudStorage {

  def upload(fileName: String): OutputStream

  def download(fileName: String): InputStream

  def deleteFile(fileName: String): Unit

  def deleteFiles(fileNames: List[String]): Unit =
    fileNames.foreach(deleteFile)
}

case class S3Storage(
                      bucketName: String,
                      awsId: String,
                      awsKey: String,
                      prefix: String = "",
                      awsToken: Option[String] = None,
                      parallelism: Int = CloudStorageOperations.DEFAULT_PARALLELISM
                    ) extends CloudStorage {

  private val awsCredentials =
    awsToken match {
      case Some(token) => new BasicSessionCredentials(awsId, awsKey, token)
      case None => new BasicAWSCredentials(awsId, awsKey)
    }

  private val clientConfig = new ClientConfiguration()
  clientConfig
    .setMaxConnections(parallelism)
  clientConfig
    .setMaxErrorRetry(CloudStorageOperations.S3_MAX_RETRIES)

  private val s3Client: AmazonS3Client = new AmazonS3Client(awsCredentials, clientConfig)


  //future work, replace io operation in RDD and writer
  //todo
  override def upload(fileName: String): OutputStream = null

  //todo
  override def download(fileName: String): InputStream = null

  override def deleteFile(fileName: String): Unit =
    s3Client.deleteObject(bucketName, prefix.concat(fileName))

  override def deleteFiles(fileNames: List[String]): Unit =
    s3Client.deleteObjects(
      new DeleteObjectsRequest(bucketName)
        .withKeys(fileNames.map(prefix.concat): _*)
    )

}

//todo case class AzureStorage() extends CloudStorage
//todo: google cloud, local file for testing?
