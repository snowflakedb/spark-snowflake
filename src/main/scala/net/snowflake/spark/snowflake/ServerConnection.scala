package net.snowflake.spark.snowflake

import net.snowflake.client.core.{SFBaseSession, SFSession, SFSessionProperty}
import net.snowflake.client.jdbc.SnowflakeConnectionV1
import net.snowflake.spark.snowflake.Parameters.{MergedParameters, PARAM_SF_URL, PARAM_SF_USER}
import net.snowflake.spark.snowflake.Utils.JDBC_DRIVER
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.slf4j.LoggerFactory

import java.sql.{Connection, DriverManager, PreparedStatement, SQLException, Statement}
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/*
 * This class is the key if a JDBC connection is cached for sharing.
 */
private[snowflake]
sealed class ConnectionCacheKey(private val parameters: MergedParameters) {
  ServerConnection.serverConnectionCount.incrementAndGet()
  private val PARAMETERS_NOT_AFFECT_SHARE_CONNECTION = Set(
    // If it is SAVE, SC added overwrite internally, refer to DefaultSource.createRelation()
    "overwrite",
    Parameters.PARAM_POSTACTIONS,
    Parameters.PARAM_PREACTIONS,
    Parameters.PARAM_FORCE_SKIP_PRE_POST_ACTION_CHECK_FOR_SESSION_SHARING,
    Parameters.PARAM_SF_QUERY,
    Parameters.PARAM_SF_DBTABLE
  )
  private val connectionSharableCheckOptions =
    parameters.parameters -- PARAMETERS_NOT_AFFECT_SHARE_CONNECTION

  override def toString: String = "Connection Cache Key"

  override def equals(obj: Any): Boolean = obj match {
    case other: ConnectionCacheKey =>
      connectionSharableCheckOptions.equals(other.connectionSharableCheckOptions)
    case _ => false
  }

  override def hashCode(): Int =
    connectionSharableCheckOptions.toArray.sorted.mkString(",").hashCode

  private[snowflake] def isQueryInWhiteList(query: String) = {
    // CREATE [ OR REPLACE ]
    //    [ { [ LOCAL | GLOBAL ] TEMP[ORARY] | VOLATILE } | TRANSIENT ]
    //    TABLE ...
    val createTablePattern =
    """CREATE\s+(OR\s+REPLACE\s+)?
      |(LOCAL\s+|GLOBAL\s+)?(TEMP(ORARY)?\s+|VOLATILE\s+|TRANSIENT\s+)?
      |TABLE(.*)""".stripMargin.replaceAll("\\s+", "").r

    // CREATE [ OR REPLACE ] [ TEMPORARY ] STAGE
    val createStagePattern = """CREATE\s+(OR\s+REPLACE\s+)?(TEMP(ORARY)?\s+)?STAGE(.*)""".r

    // DROP TABLE [ IF EXISTS ] <name> [ CASCADE | RESTRICT ]
    // DROP STAGE [ IF EXISTS ] <name>
    val dropObjects = """DROP\s+(TABLE\s+|STAGE\s+)(.*)""".r

    // MERGE INTO
    val mergeInto = """MERGE\s+INTO\s+(.*)""".r

    query.toUpperCase.trim match {
      case "" => true
      case createTablePattern(_, _, _, _, _) => true
      case createStagePattern(_, _, _, _) => true
      case dropObjects(_, _) => true
      case mergeInto(_) => true
      case _ => false
    }
  }

  private[snowflake] def isPrePostActionsQualifiedForConnectionShare: Boolean =
    parameters.forceSkipPrePostActionsCheck ||
      (parameters.preActions.forall(isQueryInWhiteList) &&
        parameters.postActions.forall(isQueryInWhiteList))

  def isConnectionCacheSupported: Boolean = {
    // Support sharing connection if pre/post actions doesn't change context
    ServerConnection.supportSharingJDBCConnection &&
      parameters.supportShareConnection &&
      isPrePostActionsQualifiedForConnectionShare
  }
}

private[snowflake] object ServerConnection {
  private val log = LoggerFactory.getLogger(getClass)
  private[snowflake] val serverConnectionCount = new AtomicLong
  private[snowflake] val jdbcConnectionCount = new AtomicLong
  private[snowflake] var supportSharingJDBCConnection = true

  private[snowflake] def setSupportSharingJDBCConnection(enabled: Boolean): Unit =
    supportSharingJDBCConnection = enabled

  def apply(jdbcConnection: Connection, enableCache: Boolean): ServerConnection =
    new ServerConnection(jdbcConnection, enableCache)

  def apply(jdbcConnection: Connection): ServerConnection =
    new ServerConnection(jdbcConnection, true)

  private val cachedJdbcConnections = mutable.Map[ConnectionCacheKey, Connection]()

  def closeAllCachedConnections: Unit = synchronized {
    log.info(s"Close all ${cachedJdbcConnections.size} cached connection.")
    cachedJdbcConnections.values.foreach(_.close())
    cachedJdbcConnections.clear()
  }

  def getServerConnection(parameters: MergedParameters, useCache: Boolean = true)
  : ServerConnection = synchronized {
    val connectionCacheKey = new ConnectionCacheKey(parameters)
    // when use connection from Snowpark, always reuse cached the connection
    // since the Snowpark connection in the staging area will be purged after get.
    val enableCache = (useCache || parameters.getConnectionId.isDefined) &&
      connectionCacheKey.isConnectionCacheSupported
    val (jdbcConnection, newConnection) =
      if (enableCache && cachedJdbcConnections.keySet.contains(connectionCacheKey)) {
        (cachedJdbcConnections(connectionCacheKey), false)
      } else {
        val newConnection: Connection = parameters.getConnectionId match {
          case Some(connectionId) =>
            val snowparkConnection =
              ServerConnection.providedConnections.getConnection(connectionId).getOrElse{
                throw new RuntimeException("Internal Error: Can't find the Snowpark Session")
              }
            log.debug(s"Reuse connection from Snowpark")
            cachedJdbcConnections.put(connectionCacheKey, snowparkConnection)
            snowparkConnection
          case _ =>
            val createdJdbcConnection = createJDBCConnection(parameters)
            if (enableCache) {
              log.debug(s"Cache the new created JDBCConnection")
              cachedJdbcConnections.put(connectionCacheKey, createdJdbcConnection)
            }
            createdJdbcConnection
        }
        (newConnection, true)
      }
    val serverConnection = ServerConnection(jdbcConnection, enableCache)

    log.info(s"Create ServerConnection with ${if (newConnection) "new" else "cached"}" +
      s" JDBC connection: ${serverConnection.getSessionID}")

    // Send client info telemetry message.
    if (newConnection) {
      val extraValues = Map(TelemetryClientInfoFields.SFURL -> parameters.sfURL,
        TelemetryClientInfoFields.SHARED -> enableCache.toString)
      SnowflakeTelemetry.sendClientInfoTelemetry(extraValues, serverConnection)
    }

    serverConnection
  }

  private def createJDBCConnection(params: MergedParameters): Connection = {
    // Derive class name
    val driverClassName = JDBC_DRIVER
    try {
      val driverClass = Utils.classForName(driverClassName)
      DriverRegistry.register(driverClass.getCanonicalName)
    } catch {
      case e: ClassNotFoundException =>
        throw new ClassNotFoundException(
          s"Could not load a Snowflake JDBC driver class < $driverClassName > ",
          e
        )
    }

    val sfURL = params.sfURL
    val jdbcURL = s"""jdbc:snowflake://$sfURL"""

    val jdbcProperties = new Properties()

    // Obligatory properties
    jdbcProperties.put("db", params.sfDatabase)
    jdbcProperties.put("schema", params.sfSchema) // Has a default
    if (params.sfUser != null) {
      // user is optional when using OAuth token or OAuth Client Credentials
      jdbcProperties.put("user", params.sfUser)
    }

    // Handle different authentication methods
    if (params.isOAuthClientCredentials) {
      // OAuth Client Credentials flow - JDBC driver obtains token automatically
      // Set the OAuth Client Credentials parameters for JDBC
      params.oauthClientId.foreach(v => jdbcProperties.put("oauthClientId", v))
      params.oauthClientSecret.foreach(v => jdbcProperties.put("oauthClientSecret", v))
      params.oauthTokenRequestUrl.foreach(v => jdbcProperties.put("oauthTokenRequestUrl", v))
      params.oauthScope.foreach(v => jdbcProperties.put("oauthScope", v))
      // No password or token needed - JDBC driver handles token acquisition
    } else {
      params.privateKey match {
        case Some(privateKey) =>
          jdbcProperties.put("privateKey", privateKey)
        case None =>
          // Adding OAuth Token parameter
          params.sfToken match {
            case Some(value) =>
              jdbcProperties.put("token", value)
            case None => jdbcProperties.put("password", params.sfPassword)
          }
      }
    }
    jdbcProperties.put("ssl", params.sfSSL) // Has a default
    // Optional properties
    if (params.sfAccount.isDefined) {
      jdbcProperties.put("account", params.sfAccount.get)
    }
    if (params.sfWarehouse.isDefined) {
      jdbcProperties.put("warehouse", params.sfWarehouse.get)
    }
    if (params.sfRole.isDefined) {
      jdbcProperties.put("role", params.sfRole.get)
    }
    params.getTimeOutputFormat match {
      case Some(value) =>
        jdbcProperties.put(Parameters.PARAM_TIME_OUTPUT_FORMAT, value)
      case _ => // No default value for it.
    }
    params.getQueryResultFormat match {
      case Some(value) =>
        jdbcProperties.put(Parameters.PARAM_JDBC_QUERY_RESULT_FORMAT, value)
      case _ => // No default value for it.
    }

    // Set up proxy info if it is configured.
    params.setJDBCProxyIfNecessary(jdbcProperties)

    // Adding Authenticator parameter
    params.sfAuthenticator match {
      case Some(value) =>
        jdbcProperties.put("authenticator", value)
      case _ => // No default value for it.
    }

    // Always set CLIENT_SESSION_KEEP_ALIVE.
    // Note, can be overridden with options
    jdbcProperties.put("client_session_keep_alive", "true")

    // Force DECIMAL for NUMBER (SNOW-33227)
    if (params.treatDecimalAsLong) {
      jdbcProperties.put("JDBC_TREAT_DECIMAL_AS_INT", "true")
    } else {
      jdbcProperties.put("JDBC_TREAT_DECIMAL_AS_INT", "false")
    }

    // Add extra properties from sfOptions
    val extraOptions = params.sfExtraOptions
    for ((k: String, v: Object) <- extraOptions) {
      jdbcProperties.put(k.toLowerCase, v.toString)
    }

    // Only one JDBC version is certified for each spark connector.
    if (!Utils.CERTIFIED_JDBC_VERSION.equals(Utils.jdbcVersion)) {
      log.warn(
        s"""JDBC ${Utils.jdbcVersion} is being used.
           | But the certified JDBC version
           | ${Utils.CERTIFIED_JDBC_VERSION} is recommended.
           |""".stripMargin.filter(_ >= ' '))
    }

    // Important: Set client_info is very important!
    // For more details, refer to PROPERTY_NAME_OF_CONNECTOR_VERSION
    // NOTE: From JDBC 3.13.3, the client info can be set with JDBC properties
    // instead of system property: "snowflake.client.info".
    jdbcProperties.put(SFSessionProperty.CLIENT_INFO.getPropertyKey, Utils.getClientInfoString())

    val conn: Connection = DriverManager.getConnection(jdbcURL, jdbcProperties)

    // Setup query result format explicitly because this option is not supported
    // to be set with JDBC properties
    if (jdbcProperties.getProperty(Parameters.PARAM_JDBC_QUERY_RESULT_FORMAT) != null) {
      try {
        val resultFormat =
          jdbcProperties.getProperty(Parameters.PARAM_JDBC_QUERY_RESULT_FORMAT)
        conn
          .createStatement()
          .execute(
            s"alter session set JDBC_QUERY_RESULT_FORMAT = '$resultFormat'"
          )
      } catch {
        case e: SQLException =>
          log.info(e.getMessage)
        case other: Any =>
          // Rethrow other errors.
          throw other
      }
    }

    // abort_detached_query
    conn.createStatement().execute(
      s"alter session set ABORT_DETACHED_QUERY = ${params.abortDetachedQuery}"
    )

    // Setup query result format explicitly because this option is not supported
    // to be set with JDBC properties
    if (params.supportAWSStageEndPoint) {
      params.getS3StageVpceDnsName.map {
        x => conn.createStatement().execute(s"alter session set S3_STAGE_VPCE_DNS_NAME = '$x'")
      }
    }

    jdbcConnectionCount.incrementAndGet()
    conn
  }

  object providedConnections {
    // use provided JDBC connection
    private val providedConn = mutable.Map[String, Connection]()
    def register(conn: Connection): String = {
      val sessionId: String = conn.asInstanceOf[SnowflakeConnectionV1].getSessionID
      providedConn.put(sessionId, conn)
      sessionId
    }
    def hasConnectionID(id: String): Boolean =
      providedConn.contains(id)

    def getConnection(id: String): Option[Connection] = this.synchronized {
      val result = providedConn.get(id)
      if (result.isDefined) {
        providedConn.remove(id)
      }
      result
    }

    def getParameters(id: String): Map[String, String] =
      providedConn.get(id) match {
        case Some(sfConn: SnowflakeConnectionV1) =>
          val map = mutable.Map[String, String]()
          map.put(PARAM_SF_URL, sfConn.getSfSession.getUrl)
          map.put(PARAM_SF_USER, sfConn.getSfSession.getUser)
          map.toMap
        case _ => Map.empty
      }
  }
}

/*
 * This class is a wrapper for JDBC Connection
 */
private[snowflake] class ServerConnection(val jdbcConnection: Connection, val enableCache: Boolean)
  extends AutoCloseable {
  override def close(): Unit = if (!enableCache) jdbcConnection.close()

  def createStatement(): Statement = jdbcConnection.createStatement()

  def prepareStatement(query: String): PreparedStatement = jdbcConnection.prepareStatement(query)

  def isClosed: Boolean = jdbcConnection.isClosed

  def commit(): Unit = jdbcConnection.commit()

  def rollback(): Unit = jdbcConnection.rollback()

  def getSessionID: String = jdbcConnection.asInstanceOf[SnowflakeConnectionV1].getSessionID

  def getSfSession: SFSession = jdbcConnection.asInstanceOf[SnowflakeConnectionV1].getSfSession

  def getSFBaseSession: SFBaseSession =
    jdbcConnection.asInstanceOf[SnowflakeConnectionV1].getSFBaseSession
}
