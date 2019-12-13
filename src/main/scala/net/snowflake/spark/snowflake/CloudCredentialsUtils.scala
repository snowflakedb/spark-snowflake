/*
 * Copyright 2015-2018 Snowflake Computing
 * Copyright 2015 Databricks
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

import java.net.URI

import net.snowflake.client.jdbc.internal.amazonaws.auth.{
  AWSCredentials,
  AWSSessionCredentials,
  BasicAWSCredentials,
  InstanceProfileCredentialsProvider
}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.StorageCredentialsSharedAccessSignature
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SQLContext

private[snowflake] object CloudCredentialsUtils {

  /**
    * Generates a credentials string for use in Snowflake COPY in-out statements.
    */
  def getSnowflakeCredentialsString(
    sqlContext: SQLContext,
    params: MergedParameters
  ): SnowflakeSQLStatement = {
    params.rootTempDirStorageType match {
      case FSType.LocalFile =>
        throw new UnsupportedOperationException(
          "only supports Azure and S3 stage"
        )
      case FSType.Azure =>
        val creds = getAzureCreds(params)
        getSnowflakeCredentialsStringForAzure(creds)
      case FSType.S3 =>
        val creds = getAWSCreds(sqlContext, params)
        getSnowflakeCredentialsStringForAWS(creds)
      case _ =>
        EmptySnowflakeSQLStatement()
    }
  }

  def getSnowflakeCredentialsStringForAWS(
    creds: AWSCredentials
  ): SnowflakeSQLStatement = {
    val awsAccessKey = creds.getAWSAccessKeyId
    val awsSecretKey = creds.getAWSSecretKey
    val tokenString = creds match {
      case sessionCreds: AWSSessionCredentials =>
        s"AWS_TOKEN='${sessionCreds.getSessionToken}'"
      case _ => ""
    }

    ConstantString(s"""
       |CREDENTIALS = (
       |    AWS_KEY_ID='$awsAccessKey'
       |    AWS_SECRET_KEY='$awsSecretKey'
       |    $tokenString
       |)
       |""".stripMargin.trim) !
  }

  def getSnowflakeCredentialsStringForAzure(
    creds: StorageCredentialsSharedAccessSignature
  ): SnowflakeSQLStatement = {
    val sasToken = creds.getToken
    ConstantString(s"""
       |CREDENTIALS = (
       |    AZURE_SAS_TOKEN='$sasToken'
       |)
       |""".stripMargin.trim) !
  }

  def getAWSCreds(sqlContext: SQLContext,
                  params: MergedParameters): AWSCredentials = {
    if (params.rootTempDirStorageType == FSType.S3) {
      params.temporaryAWSCredentials.getOrElse(
        CloudCredentialsUtils
          .load(params.rootTempDir, sqlContext.sparkContext.hadoopConfiguration)
      )
    } else {
      null
    }
  }

  def getAzureCreds(
    params: MergedParameters
  ): StorageCredentialsSharedAccessSignature = {
    if (params.rootTempDirStorageType == FSType.Azure) {
      params.temporaryAzureStorageCredentials.orNull
    } else {
      null
    }
  }

  private def getS3Credentials(uri: URI, conf: Configuration) : (String, String) = {
    if (uri.getHost == null) {
      throw new IllegalArgumentException(s"Invalid hostname in URI $uri")
    }

    var accessKey: String = null
    var secretAccessKey: String = null

    val userInfo = uri.getUserInfo
    if (userInfo != null) {
      val index = userInfo.indexOf(':')
      if (index != -1) {
        accessKey = userInfo.substring(0, index)
        secretAccessKey = userInfo.substring(index + 1)
      } else {
        accessKey = userInfo
      }
    }

    val scheme = uri.getScheme
    val accessKeyProperty = String.format("fs.%s.awsAccessKeyId", scheme)
    val secretAccessKeyProperty = String.format("fs.%s.awsSecretAccessKey", scheme)
    if (accessKey == null) {
      accessKey = conf.get(accessKeyProperty)
    }
    if (secretAccessKey == null) {
      secretAccessKey = conf.get(secretAccessKeyProperty)
    }
    if (accessKey == null && secretAccessKey == null) {
      throw new IllegalArgumentException("AWS Access Key ID and Secret Access " +
        "Key must be specified as the username or password (respectively) of a " +
        scheme + " URL, or by setting the " + accessKeyProperty + " or " +
        secretAccessKeyProperty + " properties (respectively).")
    } else if (accessKey == null) {
      throw new IllegalArgumentException("AWS Access Key ID must be specified " +
        "as the username of a " + scheme + " URL, or by setting the " +
        accessKeyProperty + " property.")
    } else if (secretAccessKey == null) {
      throw new IllegalArgumentException("AWS Secret Access Key must be " +
        "specified as the password of a " + scheme + " URL, or by setting the " +
        secretAccessKeyProperty + " property.")
    }

    (accessKey, secretAccessKey)
  }

  def load(tempPath: String,
           hadoopConfiguration: Configuration): AWSCredentials = {
    // scalastyle:off
    // A good reference on Hadoop's configuration loading / precedence is
    // https://github.com/apache/hadoop/blob/trunk/hadoop-tools/hadoop-aws/src/site/markdown/tools/hadoop-aws/index.md
    // scalastyle:on
    val uri = new URI(tempPath)
    uri.getScheme match {
      case "s3" | "s3n" =>
        val (accessKey, secretAccessKey) = getS3Credentials (uri, hadoopConfiguration)
        new BasicAWSCredentials(accessKey, secretAccessKey)
      case "s3a" =>
        // This matches what S3A does, with one exception: we don't support anonymous credentials.
        // First, try to parse from URI:
        Option(uri.getUserInfo)
          .flatMap { userInfo =>
            if (userInfo.contains(":")) {
              val Array(accessKey, secretKey) = userInfo.split(":")
              Some(new BasicAWSCredentials(accessKey, secretKey))
            } else {
              None
            }
          }
          .orElse {
            // Next, try to read from configuration
            val accessKey = hadoopConfiguration.get("fs.s3a.access.key", null)
            val secretKey = hadoopConfiguration.get("fs.s3a.secret.key", null)
            if (accessKey != null && secretKey != null) {
              Some(new BasicAWSCredentials(accessKey, secretKey))
            } else {
              None
            }
          }
          .getOrElse {
            // Finally, fall back on the instance profile provider
            new InstanceProfileCredentialsProvider().getCredentials
          }
      case "file" =>
        // Do nothing
        null
      case other =>
        throw new IllegalArgumentException(
          s"Unrecognized scheme $other; expected s3, s3n, or s3a"
        )
    }
  }
}
