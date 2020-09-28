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
import java.sql._

import net.snowflake.client.core.SFSessionProperty
import net.snowflake.client.jdbc.internal.amazonaws.ClientConfiguration
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.OperationContext
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}

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

  test("test FilterPushdown.buildValueWithType") {
    val expectedSql = "123"
    // Long/Short/Byte are not tested yet, cover them.
    val v = FilterPushdown.buildValueWithType(LongType, 123.toLong)
    assert(expectedSql.equals(
      FilterPushdown.buildValueWithType(LongType, 123.toLong).toString))
    assert(expectedSql.equals(
      FilterPushdown.buildValueWithType(ShortType, 123.toShort).toString))
    assert(expectedSql.equals(
      FilterPushdown.buildValueWithType(ByteType, 123.toByte).toString))
  }

  test("test FilterPushdown.buildValue") {
    // case x: Date => StringVariable(x.toString) + "::DATE"
    val date = Date.valueOf("2020-12-12")
    val dateSql = "'2020-12-12'::DATE"
    assert(dateSql.equals(
      FilterPushdown.buildValue(date).toString.replaceAll(" ", "")
    ))

    // case x: Timestamp => StringVariable(x.toString) + "::TIMESTAMP(3)"
    val ts = Timestamp.valueOf("2020-12-12 12:12:12.123")
    val tsSql = "'2020-12-1212:12:12.123'::TIMESTAMP(3)"
    assert(tsSql.equals(
      FilterPushdown.buildValue(ts).toString.replaceAll(" ", "")
    ))

    // case x: Int => IntVariable(x) !
    // case x: Long => LongVariable(x) !
    // case x: Short => ShortVariable(x) !
    // case x: Byte => ByteVariable(x) !
    val numSql = "123"
    assert(numSql.equals(FilterPushdown.buildValue(123.toInt).toString))
    assert(numSql.equals(FilterPushdown.buildValue(123.toLong).toString))
    assert(numSql.equals(FilterPushdown.buildValue(123.toShort).toString))
    assert(numSql.equals(FilterPushdown.buildValue(123.toByte).toString))

    // case x: Boolean => BooleanVariable(x) !
    assert("true".equals(FilterPushdown.buildValue(true).toString))
    assert("false".equals(FilterPushdown.buildValue(false).toString))

    // case x: Float => FloatVariable(x) !
    // case x: Double => DoubleVariable(x) !
    val floatSql = "123.456"
    assert(floatSql.equals(FilterPushdown.buildValue(123.456.toFloat).toString))
    assert(floatSql.equals(FilterPushdown.buildValue(123.456.toDouble).toString))

    // case _ => ConstantString(value.toString) !
    assert("123.456".equals(FilterPushdown.buildValue(BigDecimal("123.456")).toString))
  }

  test("test SnowflakeTelemetry.addPushdownFailMessage()") {
    // logs is empty in the begin
    assert(SnowflakeTelemetry.get_logs().isEmpty)
    // put one message in the logs
    SnowflakeTelemetry.addPushdownFailMessage(
      new SnowflakePushdownUnsupportedException(
        "unit test operation, should not be seen actually",
        "unit test operation, should not be seen actually",
        "unit test operation, should not be seen actually",
        false
      )
    )
    assert(SnowflakeTelemetry.get_logs().length == 1)
    // clean up it.
    SnowflakeTelemetry.set_logs(Nil)
  }
}
