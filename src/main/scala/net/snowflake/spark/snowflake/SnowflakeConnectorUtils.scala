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
    session.experimental.extraStrategies ++= Seq(new SnowflakeStrategy)
  }

  /** Disable more advanced query pushdowns to Snowflake.
    *
    * @param session The SparkSession for which pushdowns are to be disabled.
    */
  def disablePushdownSession(session: SparkSession): Unit = {
    session.experimental.extraStrategies = session.experimental.extraStrategies
      .filterNot(strategy => strategy.isInstanceOf[SnowflakeStrategy])
  }
}

class SnowflakePushdownException(message: String) extends Exception(message)
