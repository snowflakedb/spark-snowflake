package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.pushdowns.SnowflakeStrategy
import org.apache.spark.sql.SparkSession

/**
  * Created by ema on 11/22/16.
  */

object SnowflakeConnectorUtils {

  var currentPushdownSparkSession: Option[SparkSession] = None

  def enablePushdownSession(session: SparkSession): Unit = {
    currentPushdownSparkSession = Some(session)
    session.experimental.extraStrategies ++= Seq(new SnowflakeStrategy)
  }

  def disablePushdownSession(session: SparkSession): Unit = {
    currentPushdownSparkSession = None
    session.experimental.extraStrategies = session.experimental.extraStrategies
      .filterNot(strategy => strategy.isInstanceOf[SnowflakeStrategy])
  }
}
