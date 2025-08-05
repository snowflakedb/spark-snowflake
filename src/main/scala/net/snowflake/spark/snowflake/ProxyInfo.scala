package net.snowflake.spark.snowflake

import java.net.{InetSocketAddress, Proxy}
import java.net.Proxy.Type
import java.util.Properties

import net.snowflake.client.core.SFSessionProperty
import net.snowflake.client.jdbc.internal.amazonaws.{ClientConfiguration, Protocol, ProxyAuthenticationMethod}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.OperationContext
import scala.collection.JavaConverters._

private[snowflake] class ProxyInfo(proxyProtocol: Option[String],
                                   proxyHost: Option[String],
                                   proxyPort: Option[String],
                                   proxyUser: Option[String],
                                   proxyPassword: Option[String],
                                   nonProxyHosts: Option[String])
    extends Serializable {
  private def validate(): Unit = {
    if (proxyHost.isEmpty || proxyPort.isEmpty) {
      throw new IllegalArgumentException(
        "proxy host and port are mandatory when using proxy."
      )
    }

    try {
      proxyPort.get.toInt
    } catch {
      case _: Any =>
        throw new IllegalArgumentException("proxy port must be a valid number.")
    }

    if (proxyPassword.isDefined && proxyUser.isEmpty) {
      throw new IllegalArgumentException(
        "proxy user must be set if proxy password is set."
      )
    }

    if (proxyProtocol.isDefined && !Set("http", "https").contains(proxyProtocol.get)) {
      throw new IllegalArgumentException(
        "Valid values for proxy protocol are 'http' and 'https'."
      )
    }
  }

  def setProxyForJDBC(jdbcProperties: Properties): Unit = {
    validate()

    // Setup 3 mandatory properties
    jdbcProperties.put(SFSessionProperty.USE_PROXY.getPropertyKey, "true")
    jdbcProperties.put(
      SFSessionProperty.PROXY_HOST.getPropertyKey,
      proxyHost.get
    )
    jdbcProperties.put(
      SFSessionProperty.PROXY_PORT.getPropertyKey,
      proxyPort.get
    )

    // Hard code to manually test proxy is used by negative test
    // if (jdbcProperties.size() <= 3) {
    //  jdbcProperties.put(SFSessionProperty.PROXY_HOST.getPropertyKey(), "wronghost")
    //  jdbcProperties.put(SFSessionProperty.PROXY_PORT.getPropertyKey(), "12345")
    // }

    // Setup 4 optional properties if they are provided.
    proxyProtocol match {
      case Some(optionValue) =>
        jdbcProperties.put(
          SFSessionProperty.PROXY_PROTOCOL.getPropertyKey,
          optionValue
        )
      case None =>
    }
    proxyUser match {
      case Some(optionValue) =>
        jdbcProperties.put(
          SFSessionProperty.PROXY_USER.getPropertyKey,
          optionValue
        )
      case None =>
    }
    proxyPassword match {
      case Some(optionValue) =>
        jdbcProperties.put(
          SFSessionProperty.PROXY_PASSWORD.getPropertyKey,
          optionValue
        )
      case None =>
    }
    nonProxyHosts match {
      case Some(optionValue) =>
        jdbcProperties.put(
          SFSessionProperty.NON_PROXY_HOSTS.getPropertyKey,
          optionValue
        )
      case None =>
    }
  }

  def setProxyForS3(s3client: ClientConfiguration): Unit = {
    validate()

    // Setup 2 mandatory properties
    s3client.setProxyHost(proxyHost.get)
    s3client.setProxyPort(proxyPort.get.toInt)

    // Hard code to manually test proxy is used by negative test
    // s3client.setProxyHost("wronghost")
    // s3client.setProxyPort(12345)

    // Setup 4 optional properties if they are provided.
    proxyProtocol match {
      case Some(optionValue) =>
        val protocol = if (optionValue.equalsIgnoreCase("https")) Protocol.HTTPS else Protocol.HTTP
        s3client.setProxyProtocol(protocol)
      case None =>
    }
    proxyUser match {
      case Some(optionValue) =>
        s3client.setProxyUsername(optionValue)
      case None =>
    }
    proxyPassword match {
      case Some(optionValue) =>
        s3client.setProxyPassword(optionValue)
      case None =>
    }

    // Force the use of BASIC authentication only for proxy authentication
    // This ensures that when multiple authentication schemes are offered by the proxy
    // (e.g., NEGOTIATE, NTLM, BASIC), we only use BASIC authentication
    if (proxyUser.isDefined && proxyPassword.isDefined) {
      s3client.setProxyAuthenticationMethods(List(ProxyAuthenticationMethod.BASIC).asJava)
    }

    nonProxyHosts match {
      case Some(optionValue) =>
        s3client.setNonProxyHosts(optionValue)
      case None =>
    }
  }

  def setProxyForAzure(): Unit = {
    validate()

    OperationContext.setDefaultProxy(
      new Proxy(
        Type.HTTP,
        new InetSocketAddress(proxyHost.get, proxyPort.get.toInt)
      )
    )

    // Hard code to manually test proxy is used by negative test
    // OperationContext.setDefaultProxy(new Proxy(Type.HTTP,
    //    new InetSocketAddress("wronghost", 12345)))
  }
}
