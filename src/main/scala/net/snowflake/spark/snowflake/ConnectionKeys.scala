package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.spark.sql.catalyst.optimizer.OptimizeInSuite

import java.security.PrivateKey
import java.sql.Connection
import scala.collection.mutable

case class ConnectionKeys(
                           sfFullURL: String,
                           sfAccount: Option[String],
                           sfUser: String,
                           privateKey: Option[PrivateKey],
                           sfPassword: Option[String],
                           sfToken: Option[String],
                           sfTimeZone: Option[String],
                           sfDatabase: String,
                           sfSchema: String,
                           sfWarehouse: Option[String],
                           sfRole: Option[String]
                         ) {
  // TODO save hash instead of secret.
  override def toString: String = "ConnectionKeys: not print out secret"
}


object ConnectionKeys {
  def getConnectionKeys (params: MergedParameters): ConnectionKeys =
    ConnectionKeys(params.sfFullURL, params.sfAccount, params.sfUser, params.privateKey,
      params.sfPassword, params.sfToken,
    params.sfTimezone, params.sfDatabase, params.sfSchema,
      params.sfWarehouse, params.sfRole)

  def getConnection(connectionKeys: ConnectionKeys): Option[Connection] =
    lockObject.synchronized {
      val connection = cachedConnections.get(connectionKeys)
      if (connection.nonEmpty && connection.get.isClosed) {
        cachedConnections.remove(connectionKeys)
        None
      } else {
        connection
      }
    }

  def setConnection(connectionKeys: ConnectionKeys, connection: Connection): Option[Connection] =
    lockObject.synchronized {
      cachedConnections.put(connectionKeys, connection)
    }

  private val cachedConnections = mutable.Map[ConnectionKeys, Connection]()

  private val lockObject = new Object()
}