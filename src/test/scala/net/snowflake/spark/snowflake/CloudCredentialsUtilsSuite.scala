/*
 * Copyright 2015-2016 Snowflake Computing
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

import net.snowflake.client.jdbc.internal.amazonaws.AmazonClientException
import net.snowflake.client.jdbc.internal.amazonaws.auth.{
  BasicAWSCredentials,
  BasicSessionCredentials
}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.StorageCredentialsSharedAccessSignature
import org.apache.hadoop.conf.Configuration
import org.scalatest.FunSuite

class CloudCredentialsUtilsSuite extends FunSuite {

  test("credentialsString with regular S3 keys") {
    val creds =
      new BasicAWSCredentials("ACCESSKEYID", "SECRET/KEY/WITH/SLASHES")
    val result = CloudCredentialsUtils
      .getSnowflakeCredentialsStringForAWS(creds)
      .toString
      .replaceAll("\\s+", " ")
    assert(
      result ===
        "CREDENTIALS = ( AWS_KEY_ID='ACCESSKEYID' AWS_SECRET_KEY='SECRET/KEY/WITH/SLASHES' )"
    )
  }

  test("credentialsString with STS temporary keys") {
    val creds =
      new BasicSessionCredentials("ACCESSKEYID", "SECRET/KEY", "SESSION/Token")
    val result = CloudCredentialsUtils
      .getSnowflakeCredentialsStringForAWS(creds)
      .toString
      .replaceAll("\\s+", " ")
    assert(
      result ===
        "CREDENTIALS = ( AWS_KEY_ID='ACCESSKEYID' AWS_SECRET_KEY='SECRET/KEY'" +
          " AWS_TOKEN='SESSION/Token' )"
    )
  }

  test("credentialsString with Azure storage SAS token") {
    val creds = new StorageCredentialsSharedAccessSignature("SAS_TOKEN")
    val result = CloudCredentialsUtils
      .getSnowflakeCredentialsStringForAzure(creds)
      .toString
      .replaceAll("\\s+", " ")
    assert(
      result ===
        "CREDENTIALS = ( AZURE_SAS_TOKEN='SAS_TOKEN' )"
    )
  }

  test("AWSCredentials.load() credentials precedence for s3:// URIs") {
    val conf = new Configuration(false)
    conf.set("fs.s3.awsAccessKeyId", "CONFID")
    conf.set("fs.s3.awsSecretAccessKey", "CONFKEY")

    {
      val creds =
        CloudCredentialsUtils.load("s3://URIID:URIKEY@bucket/path", conf)
      assert(creds.getAWSAccessKeyId === "URIID")
      assert(creds.getAWSSecretKey === "URIKEY")
    }

    {
      val creds = CloudCredentialsUtils.load("s3://bucket/path", conf)
      assert(creds.getAWSAccessKeyId === "CONFID")
      assert(creds.getAWSSecretKey === "CONFKEY")
    }

    // The s3:// protocol does not work with EC2 IAM instance profiles.
    val e = intercept[IllegalArgumentException] {
      CloudCredentialsUtils.load("s3://bucket/path", new Configuration(false))
    }
    assert(e.getMessage.contains("Key must be specified"))
  }

  test("AWSCredentials.load() credentials precedence for s3n:// URIs") {
    val conf = new Configuration(false)
    conf.set("fs.s3n.awsAccessKeyId", "CONFID")
    conf.set("fs.s3n.awsSecretAccessKey", "CONFKEY")

    {
      val creds =
        CloudCredentialsUtils.load("s3n://URIID:URIKEY@bucket/path", conf)
      assert(creds.getAWSAccessKeyId === "URIID")
      assert(creds.getAWSSecretKey === "URIKEY")
    }

    {
      val creds = CloudCredentialsUtils.load("s3n://bucket/path", conf)
      assert(creds.getAWSAccessKeyId === "CONFID")
      assert(creds.getAWSSecretKey === "CONFKEY")
    }

    // The s3n:// protocol does not work with EC2 IAM instance profiles.
    val e = intercept[IllegalArgumentException] {
      CloudCredentialsUtils.load("s3n://bucket/path", new Configuration(false))
    }
    assert(e.getMessage.contains("Key must be specified"))
  }

  test("AWSCredentials.load() credentials precedence for s3a:// URIs") {
    val conf = new Configuration(false)
    conf.set("fs.s3a.access.key", "CONFID")
    conf.set("fs.s3a.secret.key", "CONFKEY")

    {
      val creds =
        CloudCredentialsUtils.load("s3a://URIID:URIKEY@bucket/path", conf)
      assert(creds.getAWSAccessKeyId === "URIID")
      assert(creds.getAWSSecretKey === "URIKEY")
    }

    {
      val creds = CloudCredentialsUtils.load("s3a://bucket/path", conf)
      assert(creds.getAWSAccessKeyId === "CONFID")
      assert(creds.getAWSSecretKey === "CONFKEY")
    }

    // The s3a:// protocol supports loading of credentials from EC2 IAM instance profiles, but
    // our Travis integration tests will not be able to provide these credentials. In the meantime,
    // just check that this test fails because the AWS client fails to obtain those credentials.
    // TODO: refactor and mock to enable proper tests here.
    val e = intercept[AmazonClientException] {
      CloudCredentialsUtils.load("s3a://bucket/path", new Configuration(false))
    }
    assert(
      e.getMessage === "Unable to load credentials from service endpoint" ||
        e.getMessage.contains("The requested metadata is not found at")
    )
  }
}
