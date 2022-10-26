/*
 * Copyright 2015-2020 Snowflake Computing
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

import java.net.Proxy.Type
import java.net.{InetSocketAddress, Proxy}
import java.security.InvalidKeyException
import java.util.Properties

import net.snowflake.client.core.SFSessionProperty
import net.snowflake.client.jdbc.SnowflakeSQLException
import net.snowflake.client.jdbc.internal.amazonaws.ClientConfiguration
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.OperationContext
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSuite, Matchers}

/**
  * Unit tests for all kinds of some classes
  */
class MiscSuite01 extends FunSuite with Matchers {

  private val mapper = new ObjectMapper()

  test("test ProxyInfo with all fields") {
    val sfOptions = Map(
      Parameters.PARAM_USE_PROXY -> "true",
      Parameters.PARAM_PROXY_HOST -> "proxyHost",
      Parameters.PARAM_PROXY_PORT -> "1234",
      Parameters.PARAM_PROXY_USER -> "proxyUser",
      Parameters.PARAM_PROXY_PASSWORD -> "proxyPassword",
      Parameters.PARAM_NON_PROXY_HOSTS -> "nonProxyHosts"
    )
    val param = Parameters.MergedParameters(sfOptions)
    val proxyInfo = param.proxyInfo.get

    // Set proxy for JDBC
    val jdbcProperties = new Properties()
    param.setJDBCProxyIfNecessary(jdbcProperties)
    assert(jdbcProperties.getProperty(
      SFSessionProperty.USE_PROXY.getPropertyKey).equals("true"))
    assert(jdbcProperties.getProperty(
      SFSessionProperty.PROXY_HOST.getPropertyKey).equals("proxyHost"))
    assert(jdbcProperties.getProperty(
      SFSessionProperty.PROXY_PORT.getPropertyKey).equals("1234"))
    assert(jdbcProperties.getProperty(
      SFSessionProperty.PROXY_USER.getPropertyKey).equals("proxyUser"))
    assert(jdbcProperties.getProperty(
      SFSessionProperty.PROXY_PASSWORD.getPropertyKey).equals("proxyPassword"))
    assert(jdbcProperties.getProperty(
      SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey).equals("nonProxyHosts"))

    // Set proxy for AWS
    val clientConfig = new ClientConfiguration()
    proxyInfo.setProxyForS3(clientConfig)
    assert(clientConfig.getProxyHost.equals("proxyHost"))
    assert(clientConfig.getProxyPort.equals(1234))
    assert(clientConfig.getProxyUsername.equals("proxyUser"))
    assert(clientConfig.getProxyPassword.equals("proxyPassword"))
    assert(clientConfig.getNonProxyHosts.equals("nonProxyHosts"))

    // Set proxy for Azure
    proxyInfo.setProxyForAzure()
    assert(OperationContext.getDefaultProxy.equals(new Proxy(
      Type.HTTP,
      new InetSocketAddress("proxyHost", 1234)
    )))
  }

  test("test ProxyInfo with hostname and port only") {
    val sfOptions = Map(
      Parameters.PARAM_USE_PROXY -> "true",
      Parameters.PARAM_PROXY_HOST -> "proxyHost",
      Parameters.PARAM_PROXY_PORT -> "1234"
    )
    val param = Parameters.MergedParameters(sfOptions)
    val proxyInfo = param.proxyInfo.get

    // Set proxy for JDBC
    val jdbcProperties = new Properties()
    param.setJDBCProxyIfNecessary(jdbcProperties)
    assert(jdbcProperties.getProperty(
      SFSessionProperty.USE_PROXY.getPropertyKey).equals("true"))
    assert(jdbcProperties.getProperty(
      SFSessionProperty.PROXY_HOST.getPropertyKey).equals("proxyHost"))
    assert(jdbcProperties.getProperty(
      SFSessionProperty.PROXY_PORT.getPropertyKey).equals("1234"))

    // Set proxy for AWS
    val clientConfig = new ClientConfiguration()
    proxyInfo.setProxyForS3(clientConfig)
    assert(clientConfig.getProxyHost.equals("proxyHost"))
    assert(clientConfig.getProxyPort.equals(1234))

    // Set proxy for Azure
    proxyInfo.setProxyForAzure()
    assert(OperationContext.getDefaultProxy.equals(new Proxy(
      Type.HTTP,
      new InetSocketAddress("proxyHost", 1234)
    )))
  }

  test("test ProxyInfo with negative value") {
    // Wrong case 1. Don't set proxyport
    var sfOptions = Map(
      Parameters.PARAM_USE_PROXY -> "true",
      Parameters.PARAM_PROXY_HOST -> "proxyHost"
    )
    var param = Parameters.MergedParameters(sfOptions)
    assertThrows[IllegalArgumentException]({
      param.proxyInfo.get.setProxyForAzure()
    })

    // Wrong case 2. port is not number
    sfOptions = Map(
      Parameters.PARAM_USE_PROXY -> "true",
      Parameters.PARAM_PROXY_HOST -> "proxyHost",
      Parameters.PARAM_PROXY_PORT -> "notNumber"
    )
    param = Parameters.MergedParameters(sfOptions)
    assertThrows[IllegalArgumentException]({
      param.proxyInfo.get.setProxyForAzure()
    })

    // Wrong case 3. password set, user name is not set
    sfOptions = Map(
      Parameters.PARAM_USE_PROXY -> "true",
      Parameters.PARAM_PROXY_HOST -> "proxyHost",
      Parameters.PARAM_PROXY_PORT -> "1234",
      Parameters.PARAM_PROXY_PASSWORD -> "proxyPassword"
    )
    param = Parameters.MergedParameters(sfOptions)
    assertThrows[IllegalArgumentException]({
      param.proxyInfo.get.setProxyForAzure()
    })
  }

  test("test Parameters.removeQuoteForStageTableName()") {
    // Explicitly set it as true
    var sfOptions = Map(Parameters.PARAM_INTERNAL_STAGING_TABLE_NAME_REMOVE_QUOTES_ONLY -> "true")
    var param = Parameters.MergedParameters(sfOptions)
    assert(param.stagingTableNameRemoveQuotesOnly)

    // Explicitly set it as false
    sfOptions = Map(Parameters.PARAM_INTERNAL_STAGING_TABLE_NAME_REMOVE_QUOTES_ONLY -> "false")
    param = Parameters.MergedParameters(sfOptions)
    assert(!param.stagingTableNameRemoveQuotesOnly)

    // It is false by default.
    sfOptions = Map(Parameters.PARAM_USE_PROXY -> "false")
    param = Parameters.MergedParameters(sfOptions)
    assert(!param.stagingTableNameRemoveQuotesOnly)
  }

  test("test SnowflakeConnectorUtils.handleS3Exception") {
    // positive test
    val ex1 = new Exception("test S3Exception",
      new InvalidKeyException("test InvalidKeyException"))
    assertThrows[SnowflakeConnectorException]({
      SnowflakeConnectorUtils.handleS3Exception(ex1)
    })

    // negative test
    val ex2 = new IllegalArgumentException("test IllegalArgumentException")
    assertThrows[IllegalArgumentException]({
      SnowflakeConnectorUtils.handleS3Exception(ex2)
    })
  }

  test("test SnowflakeFailMessage") {
    // scalastyle:off println
    println(SnowflakeFailMessage.FAIL_PUSHDOWN_AGGREGATE_EXPRESSION)
    println(SnowflakeFailMessage.FAIL_PUSHDOWN_GENERATE_QUERY)
    println(SnowflakeFailMessage.FAIL_PUSHDOWN_SET_TO_EXPR)
    println(SnowflakeFailMessage.FAIL_PUSHDOWN_STATEMENT)
    println(SnowflakeFailMessage.FAIL_PUSHDOWN_UNSUPPORTED_CONVERSION)
    // scalastyle:on println
  }

  test("unit test for SnowflakeTelemetry.getClientConfig") {
    // Configure some spark options for the spark session
    SparkSession.builder
      .master("local")
      .appName("test config info sent")
      .config("spark.driver.memory", "2G")
      .config("spark.executor.memory", "888M")
      .config("spark.driver.extraJavaOptions", s"-Duser.timezone=GMT")
      .config("spark.executor.extraJavaOptions", s"-Duser.timezone=UTC")
      .config("spark.sql.session.timeZone", "America/Los_Angeles")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    val metric = SnowflakeTelemetry.getClientConfig()
    // Check one version
    assert(metric.get(TelemetryClientInfoFields.SPARK_CONNECTOR_VERSION)
      .asText().equals(Utils.VERSION))
    // check one JVM option
    assert(metric.get(TelemetryClientInfoFields.MAX_MEMORY_IN_MB).asLong() > 0)
    assert(metric.get(TelemetryClientInfoFields.SPARK_APPLICATION_ID).asText().equals(
      SparkEnv.get.conf.get("spark.app.id")))
    assert(!metric.get(TelemetryClientInfoFields.IS_PYSPARK).asBoolean())
    // check Spark options
    val sparkConfNode = metric.get(TelemetryClientInfoFields.SPARK_CONFIG)
    assert(sparkConfNode.get("spark.master").asText().equals("local"))
    assert(sparkConfNode.get("spark.app.name").asText().equals("test config info sent"))
    assert(sparkConfNode.get("spark.driver.memory").asText().equals("2G"))
    assert(sparkConfNode.get("spark.executor.memory").asText().equals("888M"))
    assert(sparkConfNode.get("spark.driver.extraJavaOptions")
      .asText().contains("-Duser.timezone=GMT"))
    assert(sparkConfNode.get("spark.executor.extraJavaOptions")
      .asText().contains("-Duser.timezone=UTC"))
    assert(sparkConfNode.get("spark.sql.session.timeZone").asText().equals("America/Los_Angeles"))
  }

  test("unit test for SnowflakeTelemetry.addThrowable()") {
    val errorMessage = "SnowflakeTelemetry.addThrowable() test exception message"
    val queryId = "019840d2-04c3-90c5-0000-0ca911a381c6"
    val sqlState = "22018"
    val errorCode = 100038

    // Test a SnowflakeSQLException Exception
    var metric: ObjectNode = mapper.createObjectNode()
    SnowflakeTelemetry.addThrowable(metric,
      new SnowflakeSQLException(queryId, errorMessage, sqlState, errorCode))
    assert(metric.get(TelemetryQueryStatusFields.EXCEPTION_CLASS_NAME).asText()
      .equals("class net.snowflake.client.jdbc.SnowflakeSQLException"))
    var expectedMessage = s"SnowflakeSQLException: ErrorCode=$errorCode" +
      s" SQLState=$sqlState QueryId=$queryId"
    assert(metric.get(TelemetryQueryStatusFields.EXCEPTION_MESSAGE)
      .asText().equals(expectedMessage))
    assert(metric.get(TelemetryQueryStatusFields.STACKTRACE).asText().contains(expectedMessage))
    assert(!metric.get(TelemetryQueryStatusFields.STACKTRACE).asText().contains(errorMessage))

    // Test an OutOfMemoryError
    metric = mapper.createObjectNode()
    SnowflakeTelemetry.addThrowable(metric, new OutOfMemoryError(errorMessage))
    assert(metric.get(TelemetryQueryStatusFields.EXCEPTION_CLASS_NAME).asText()
      .equals("class java.lang.OutOfMemoryError"))
    assert(metric.get(TelemetryQueryStatusFields.EXCEPTION_MESSAGE).asText().equals(errorMessage))
    assert(metric.get(TelemetryQueryStatusFields.STACKTRACE).asText().contains(errorMessage))

    // Test an Exception
    metric = mapper.createObjectNode()
    SnowflakeTelemetry.addThrowable(metric, new Exception(errorMessage))
    assert(metric.get(TelemetryQueryStatusFields.EXCEPTION_CLASS_NAME).asText()
      .equals("class java.lang.Exception"))
    assert(metric.get(TelemetryQueryStatusFields.EXCEPTION_MESSAGE).asText().equals(errorMessage))
    assert(metric.get(TelemetryQueryStatusFields.STACKTRACE).asText().contains(errorMessage))
  }

  test("test Parameters invalid option values") {
    var sfOptions = Map(
      Parameters.PARAM_EXPECTED_PARTITION_SIZE_IN_MB -> "wrong_number",
      Parameters.PARAM_UPLOAD_CHUNK_SIZE_IN_MB -> "1"
    )
    var param = Parameters.MergedParameters(sfOptions)
    assertThrows[IllegalArgumentException]({
      param.expectedPartitionSize
    })
    assertThrows[IllegalArgumentException]({
      param.uploadChunkSize
    })
  }

  test("test timestamp setting") {
    var sfOptions: Map[String, String] = Parameters.DEFAULT_PARAMETERS
    var param = Parameters.MergedParameters(sfOptions)
    // test default values
    assert(param.sfTimestampNTZOutputFormat.get.equals("YYYY-MM-DD HH24:MI:SS.FF3"))
    assert(!param.isTimestampSnowflake(param.sfTimestampNTZOutputFormat.get))
    assert(param.sfTimestampLTZOutputFormat.get.equals("TZHTZM YYYY-MM-DD HH24:MI:SS.FF3"))
    assert(!param.isTimestampSnowflake(param.sfTimestampLTZOutputFormat.get))
    assert(param.sfTimestampTZOutputFormat.get.equals("TZHTZM YYYY-MM-DD HH24:MI:SS.FF3"))
    assert(!param.isTimestampSnowflake(param.sfTimestampTZOutputFormat.get))

    sfOptions = Map(
      Parameters.PARAM_TIMESTAMP_NTZ_OUTPUT_FORMAT -> "YYYY-MM-DD HH24:MI:SS.FF4",
      Parameters.PARAM_TIMESTAMP_LTZ_OUTPUT_FORMAT -> "YYYY-MM-DD HH24:MI:SS.FF5",
      Parameters.PARAM_TIMESTAMP_TZ_OUTPUT_FORMAT -> "YYYY-MM-DD HH24:MI:SS.FF6"
    )
    param = Parameters.MergedParameters(sfOptions)
    // test normal settings
    assert(param.sfTimestampNTZOutputFormat.get.equals("YYYY-MM-DD HH24:MI:SS.FF4"))
    assert(!param.isTimestampSnowflake(param.sfTimestampNTZOutputFormat.get))
    assert(param.sfTimestampLTZOutputFormat.get.equals("YYYY-MM-DD HH24:MI:SS.FF5"))
    assert(!param.isTimestampSnowflake(param.sfTimestampLTZOutputFormat.get))
    assert(param.sfTimestampTZOutputFormat.get.equals("YYYY-MM-DD HH24:MI:SS.FF6"))
    assert(!param.isTimestampSnowflake(param.sfTimestampTZOutputFormat.get))

    sfOptions = Map(
      Parameters.PARAM_SF_TIMEZONE -> "sf_current",
      Parameters.PARAM_TIMESTAMP_NTZ_OUTPUT_FORMAT -> "sf_current",
      Parameters.PARAM_TIMESTAMP_LTZ_OUTPUT_FORMAT -> "sf_current",
      Parameters.PARAM_TIMESTAMP_TZ_OUTPUT_FORMAT -> "sf_current"
    )
    param = Parameters.MergedParameters(sfOptions)
    // test sf_current settings
    assert(param.sfTimestampNTZOutputFormat.get.equals("sf_current"))
    assert(param.isTimestampSnowflake(param.sfTimestampNTZOutputFormat.get))
    assert(param.sfTimestampLTZOutputFormat.get.equals("sf_current"))
    assert(param.isTimestampSnowflake(param.sfTimestampLTZOutputFormat.get))
    assert(param.sfTimestampTZOutputFormat.get.equals("sf_current"))
    assert(param.isTimestampSnowflake(param.sfTimestampTZOutputFormat.get))
  }

  test("test Utils.genPrologueSql()") {
    var sfOptions: Map[String, String] = Parameters.DEFAULT_PARAMETERS +
      (Parameters.PARAM_SF_TIMEZONE -> "GMT")
    var param = Parameters.MergedParameters(sfOptions)
    // test default values
    var setString = Utils.genPrologueSql(param)
    assert(setString.get.toString.equals("alter session set timezone = 'GMT' , " +
      "timestamp_ntz_output_format = 'YYYY-MM-DD HH24:MI:SS.FF3', " +
      "timestamp_ltz_output_format = 'TZHTZM YYYY-MM-DD HH24:MI:SS.FF3', " +
      "timestamp_tz_output_format = 'TZHTZM YYYY-MM-DD HH24:MI:SS.FF3' ;"))

    sfOptions = Map(
      Parameters.PARAM_SF_TIMEZONE -> "UTC",
      Parameters.PARAM_TIMESTAMP_NTZ_OUTPUT_FORMAT -> "YYYY-MM-DD HH24:MI:SS.FF4",
      Parameters.PARAM_TIMESTAMP_LTZ_OUTPUT_FORMAT -> "YYYY-MM-DD HH24:MI:SS.FF5",
      Parameters.PARAM_TIMESTAMP_TZ_OUTPUT_FORMAT -> "YYYY-MM-DD HH24:MI:SS.FF6"
    )
    param = Parameters.MergedParameters(sfOptions)
    // test normal settings
    setString = Utils.genPrologueSql(param)
    assert(setString.get.toString.equals("alter session set timezone = 'UTC' , " +
      "timestamp_ntz_output_format = 'YYYY-MM-DD HH24:MI:SS.FF4', " +
      "timestamp_ltz_output_format = 'YYYY-MM-DD HH24:MI:SS.FF5', " +
      "timestamp_tz_output_format = 'YYYY-MM-DD HH24:MI:SS.FF6' ;"))

    sfOptions = Map(
      Parameters.PARAM_SF_TIMEZONE -> "sf_current",
      Parameters.PARAM_TIMESTAMP_NTZ_OUTPUT_FORMAT -> "YYYY-MM-DD HH24:MI:SS.FF4",
      Parameters.PARAM_TIMESTAMP_LTZ_OUTPUT_FORMAT -> "YYYY-MM-DD HH24:MI:SS.FF5",
      Parameters.PARAM_TIMESTAMP_TZ_OUTPUT_FORMAT -> "YYYY-MM-DD HH24:MI:SS.FF6"
    )
    param = Parameters.MergedParameters(sfOptions)
    // test timezone is sf_current
    setString = Utils.genPrologueSql(param)
    assert(setString.get.toString.equals("alter session set " +
      "timestamp_ntz_output_format = 'YYYY-MM-DD HH24:MI:SS.FF4', " +
      "timestamp_ltz_output_format = 'YYYY-MM-DD HH24:MI:SS.FF5', " +
      "timestamp_tz_output_format = 'YYYY-MM-DD HH24:MI:SS.FF6' ;"))

    sfOptions = Map(
      Parameters.PARAM_SF_TIMEZONE -> "UTC",
      Parameters.PARAM_TIMESTAMP_NTZ_OUTPUT_FORMAT -> "YYYY-MM-DD HH24:MI:SS.FF4",
      Parameters.PARAM_TIMESTAMP_LTZ_OUTPUT_FORMAT -> "sf_current",
      Parameters.PARAM_TIMESTAMP_TZ_OUTPUT_FORMAT -> "YYYY-MM-DD HH24:MI:SS.FF6"
    )
    param = Parameters.MergedParameters(sfOptions)
    // test timestamp_ltz_output_format is sf_current
    setString = Utils.genPrologueSql(param)
    assert(setString.get.toString.equals("alter session set timezone = 'UTC' , " +
      "timestamp_ntz_output_format = 'YYYY-MM-DD HH24:MI:SS.FF4', " +
      "timestamp_tz_output_format = 'YYYY-MM-DD HH24:MI:SS.FF6' ;"))

    sfOptions = Map(
      Parameters.PARAM_SF_TIMEZONE -> "sf_current",
      Parameters.PARAM_TIMESTAMP_NTZ_OUTPUT_FORMAT -> "sf_current",
      Parameters.PARAM_TIMESTAMP_LTZ_OUTPUT_FORMAT -> "YYYY-MM-DD HH24:MI:SS.FF5",
      Parameters.PARAM_TIMESTAMP_TZ_OUTPUT_FORMAT -> "sf_current"
    )

    param = Parameters.MergedParameters(sfOptions)
    // test timezone, timestamp_ntz_output_format and timestamp_tz_output_format are sf_current
    setString = Utils.genPrologueSql(param)
    assert(setString.get.toString.equals("alter session set " +
      "timestamp_ltz_output_format = 'YYYY-MM-DD HH24:MI:SS.FF5' ;"))

    sfOptions = Map(
      Parameters.PARAM_SF_TIMEZONE -> "sf_current",
      Parameters.PARAM_TIMESTAMP_NTZ_OUTPUT_FORMAT -> "sf_current",
      Parameters.PARAM_TIMESTAMP_LTZ_OUTPUT_FORMAT -> "sf_current",
      Parameters.PARAM_TIMESTAMP_TZ_OUTPUT_FORMAT -> "sf_current"
    )

    param = Parameters.MergedParameters(sfOptions)
    // test all are sf_current
    setString = Utils.genPrologueSql(param)
    assert(setString.isEmpty)
  }

  test("test getQueryIDUrl") {
    // test URL without port, with postfix
    var sfOptions = Map(
      Parameters.PARAM_SF_URL -> "account123.us-east-1.snowflakecomputing.com?QUERY_TAG=tag1",
      Parameters.PARAM_SF_SSL -> "on"
    )
    var param = Parameters.MergedParameters(sfOptions)
    var queryURL = param.getQueryIDUrl("my_query_id_123")
    assert(queryURL.equals("https://account123.us-east-1.snowflakecomputing.com" +
      "/console#/monitoring/queries/detail?queryId=my_query_id_123"))

    // test URL without port, without postfix
    sfOptions = Map(
      Parameters.PARAM_SF_URL -> "account123.us-east-1.snowflakecomputing.com",
      Parameters.PARAM_SF_SSL -> "on"
    )
    param = Parameters.MergedParameters(sfOptions)
    queryURL = param.getQueryIDUrl("my_query_id_123")
    assert(queryURL.equals("https://account123.us-east-1.snowflakecomputing.com" +
      "/console#/monitoring/queries/detail?queryId=my_query_id_123"))

    // test URL with port, with postfix
    sfOptions = Map(
      Parameters.PARAM_SF_URL -> "account123.us-east-1.snowflakecomputing.com:443/",
      Parameters.PARAM_SF_SSL -> "on"
    )
    param = Parameters.MergedParameters(sfOptions)
    queryURL = param.getQueryIDUrl("my_query_id_123")
    assert(queryURL.equals("https://account123.us-east-1.snowflakecomputing.com:443" +
      "/console#/monitoring/queries/detail?queryId=my_query_id_123"))

    // test URL with port, without postfix
    sfOptions = Map(
      Parameters.PARAM_SF_URL -> "ecosystem_sparktest.preprod3.int.snowflakecomputing.com:8084",
      Parameters.PARAM_SF_SSL -> "on"
    )
    param = Parameters.MergedParameters(sfOptions)
    queryURL = param.getQueryIDUrl("my_query_id_123")
    assert(queryURL.equals("https://ecosystem_sparktest.preprod3.int.snowflakecomputing.com:8084" +
      "/console#/monitoring/queries/detail?queryId=my_query_id_123"))

    // negative test.
    sfOptions = Map(
      Parameters.PARAM_SF_URL -> "unrecognized_url",
      Parameters.PARAM_SF_SSL -> "on"
    )
    param = Parameters.MergedParameters(sfOptions)
    queryURL = param.getQueryIDUrl("my_query_id_123")
    assert(queryURL.startsWith("Cannot generate queryID URL for https://unrecognized_url"))
  }

}
