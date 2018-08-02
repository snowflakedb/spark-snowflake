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

import net.snowflake.client.jdbc.internal.amazonaws.ClientConfiguration
import net.snowflake.client.jdbc.internal.amazonaws.auth.{AWSCredentials, BasicAWSCredentials, BasicSessionCredentials}
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.AmazonS3Client
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.DeleteObjectsRequest

object CloudStorageOperations {
  private[io] final val DEFAULT_PARALLELISM = 10
  private[io] final val S3_MAX_RETRIES = 3


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
                      awsToken: Option[String] = None,
                      parallelism: Option[Int] = None
                    ) extends CloudStorage {

  private val awsCredentials =
    awsToken match {
      case Some(token) => new BasicSessionCredentials(awsId, awsKey, token)
      case None => new BasicAWSCredentials(awsId, awsKey)
    }

  private val clientConfig = new ClientConfiguration()
  clientConfig
    .setMaxConnections(parallelism.getOrElse(CloudStorageOperations.DEFAULT_PARALLELISM))
  clientConfig
    .setMaxErrorRetry(CloudStorageOperations.S3_MAX_RETRIES)

  private val s3Client:AmazonS3Client = new AmazonS3Client(awsCredentials, clientConfig)


  //future work, replace io operation in RDD and writer
  //todo
  override def upload(fileName: String): OutputStream = null
  //todo
  override def download(fileName: String): InputStream = null

  override def deleteFile(fileName: String): Unit =
    s3Client.deleteObject(bucketName, fileName)

  override def deleteFiles(fileNames: List[String]): Unit =
    s3Client.deleteObjects(new DeleteObjectsRequest(bucketName).withKeys(fileNames: _*))

}
//todo case class AzureStorage() extends CloudStorage
//todo: google cloud, local file for testing?
