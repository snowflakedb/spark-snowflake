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

import java.io.{ByteArrayOutputStream, OutputStream}
import java.net.Proxy.Type
import java.net.{InetSocketAddress, Proxy}
import java.security.InvalidKeyException
import java.util.Properties

import net.snowflake.client.core.SFSessionProperty
import net.snowflake.client.jdbc.internal.amazonaws.ClientConfiguration
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.OperationContext
import org.scalatest.{FunSuite, Matchers}
import org.slf4j.LoggerFactory

// Below is mock class to tesst LoggerWrapper
private[snowflake] class MockTelemetryReporter(outputStream: OutputStream)
  extends TelemetryReporter() {
  private[snowflake] override def sendLogTelemetry(level: String, msg: String): Unit = {
    // Write data to OutputStream
    val data = s"$level: $msg"
    outputStream.write(data.getBytes())
    outputStream.flush()
  }
}

/**
  * Unit tests for all kinds of some classes
  */
class MiscSuite01 extends FunSuite with Matchers {

  test("test ProxyInfo with all fields") {
    val sfOptions = Map(
      Parameters.PARAM_USE_PROXY -> "true",
      Parameters.PARAM_PROXY_HOST -> "proxyHost",
      Parameters.PARAM_PROXY_PORT -> "1234",
      Parameters.PARAM_PROXY_USER -> "proxyUser",
      Parameters.PARAM_PROXY_PASSWORD -> "proxyPassword",
      Parameters.PARAM_NON_PROXY_HOSTS -> "nonProxyHosts",
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
    println(SnowflakeFailMessage.FAIL_PUSHDOWN_AGGREGATE_EXPRESSION)
    println(SnowflakeFailMessage.FAIL_PUSHDOWN_GENERATE_QUERY)
    println(SnowflakeFailMessage.FAIL_PUSHDOWN_SET_TO_EXPR)
    println(SnowflakeFailMessage.FAIL_PUSHDOWN_STATEMENT)
  }

  test("test LoggerWrapper") {
    // Test LoggerWrapper with MockTelemetryReporter
    val targetOutputStream = new ByteArrayOutputStream()
    var logger = new LoggerWrapper(LoggerFactory.getLogger("TestLogger 1"))
    TelemetryReporter.setDriverTelemetryReporter(new MockTelemetryReporter(targetOutputStream))

    // log one message with TRACE; logger is created before setting Driver telemetry
    val message = "Hello Logger!"
    logger.trace(message)
    var loggedMessage = new String(targetOutputStream.toByteArray)
    assert(loggedMessage.startsWith("TRACE") && loggedMessage.endsWith(message))

    // log one message with DEBUG; logger is created after setting Driver telemetry
    logger = new LoggerWrapper(LoggerFactory.getLogger("TestLogger 2"))
    targetOutputStream.reset()
    logger.debug(message)
    loggedMessage = new String(targetOutputStream.toByteArray)
    assert(loggedMessage.startsWith("DEBUG") && loggedMessage.endsWith(message))

    // log one message with INFO
    targetOutputStream.reset()
    logger.info(message)
    loggedMessage = new String(targetOutputStream.toByteArray)
    assert(loggedMessage.startsWith("INFO") && loggedMessage.endsWith(message))

    // log one message with warn(msg: String)
    targetOutputStream.reset()
    logger.warn(message)
    loggedMessage = new String(targetOutputStream.toByteArray)
    assert(loggedMessage.startsWith("WARN") && loggedMessage.endsWith(message))

    // log one message with warn(msg: String, t: Throwable)
    targetOutputStream.reset()
    logger.warn(message, new Exception("Test Exception String"))
    loggedMessage = new String(targetOutputStream.toByteArray)
    assert(loggedMessage.startsWith("WARN") &&
      loggedMessage.contains("Test Exception String") &&
      loggedMessage.contains(message))

    // log one message with error(msg: String)
    targetOutputStream.reset()
    logger.error(message)
    loggedMessage = new String(targetOutputStream.toByteArray)
    assert(loggedMessage.startsWith("ERROR") &&
      loggedMessage.contains(message))

    // log one message with error(msg: String, t: Throwable)
    targetOutputStream.reset()
    logger.error(message, new Exception("Test Exception String"))
    loggedMessage = new String(targetOutputStream.toByteArray)
    assert(loggedMessage.startsWith("ERROR") &&
      loggedMessage.contains("Test Exception String") &&
      loggedMessage.contains(message))

    // log one message with error(format: String, arg1: Any, arg2: Any)
    targetOutputStream.reset()
    logger.error("test format: {}. {}", "TEST_STRING_1", "TEST_STRING_2")
    loggedMessage = new String(targetOutputStream.toByteArray)
    assert(loggedMessage.startsWith("ERROR") &&
      loggedMessage.contains("TEST_STRING_1") &&
      loggedMessage.contains("TEST_STRING_2") &&
      loggedMessage.contains("test format:"))

    // reset TelemetryReporter
    TelemetryReporter.resetDriverTelemetryReporter()
  }

  test("negative test LoggerWrapper") {
    // set an invalid OutputStream, but no exception is raised.
    val invalidOutputStream = new OutputStream {
      override def write(b: Int): Unit = {
        throw new Throwable("negative test invalid OutputStream")
      }
    }
    TelemetryReporter.setDriverTelemetryReporter(new MockTelemetryReporter(invalidOutputStream))
    val logger = new LoggerWrapper(LoggerFactory.getLogger("TestLogger"))
    // MockTelemetryReporter raise exception, but warn() doesn't
    logger.warn("no exception is raised")

    // reset TelemetryReporter
    TelemetryReporter.resetDriverTelemetryReporter()
  }

  test("driver/executor logger set/get") {
    val logger = new LoggerWrapper(LoggerFactory.getLogger("TestLogger"))
    // It's NoopTelemetry by default
    assert(TelemetryReporter.getTelemetryReporter().isInstanceOf[NoopTelemetryReporter])

    // Set driver telemetry report
    TelemetryReporter.setDriverTelemetryReporter(new DriverTelemetryReporter)
    assert(TelemetryReporter.getTelemetryReporter().isInstanceOf[DriverTelemetryReporter])

    // Set executor telemetry report
    TelemetryReporter.setExecutorTelemetryReporter(new ExecutorTelemetryReporter)
    assert(TelemetryReporter.getTelemetryReporter().isInstanceOf[ExecutorTelemetryReporter])

    // reset TelemetryReporter
    TelemetryReporter.resetDriverTelemetryReporter()
    TelemetryReporter.resetExecutorTelemetryReporter()
  }

}

