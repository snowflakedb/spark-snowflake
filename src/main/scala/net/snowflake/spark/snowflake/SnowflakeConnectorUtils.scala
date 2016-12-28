package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.pushdowns.SnowflakeStrategy
import org.apache.spark.sql.SparkSession

/**
  * Connector utils, including what needs to be invoked to enable pushdowns.
  */
object SnowflakeConnectorUtils {

  /** Enable more advanced query pushdowns to Snowflake.
    *
    * @param session The SparkSession for which pushdowns are to be enabled.
    */
  def enablePushdownSession(session: SparkSession): Unit = {
    if (session.experimental.extraStrategies
          .find(s => s.isInstanceOf[SnowflakeStrategy])
          .isEmpty)
      session.experimental.extraStrategies ++= Seq(new SnowflakeStrategy)
  }

  /** Disable more advanced query pushdowns to Snowflake.
    *
    * @param session The SparkSession for which pushdowns are to be disabled.
    */
  def disablePushdownSession(session: SparkSession): Unit = {
    session.experimental.extraStrategies =
      session.experimental.extraStrategies.filterNot(strategy =>
        strategy.isInstanceOf[SnowflakeStrategy])
  }

  def setPushdownSession(session: SparkSession, enabled: Boolean): Unit = {
    if (enabled)
      enablePushdownSession(session)
    else
      disablePushdownSession(session)
  }
}

class SnowflakeConnectorException(message: String) extends Exception(message)
class SnowflakePushdownException(message: String)
    extends SnowflakeConnectorException(message)
