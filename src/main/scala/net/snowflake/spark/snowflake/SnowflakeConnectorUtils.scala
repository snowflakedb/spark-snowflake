package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.pushdowns.SnowflakeStrategy
import org.apache.spark.sql.SparkSession

/**
  * Created by ema on 11/22/16.
  */

object SnowflakeConnectorUtils {
  def enablePushdownSession(session: SparkSession): Unit = {
    session.experimental.extraStrategies ++= Seq(new SnowflakeStrategy(session))
  }

  def disablePushdownSession(session: SparkSession): Unit = {
    session.experimental.extraStrategies = session.experimental.extraStrategies
      .filterNot(strategy => strategy.isInstanceOf[SnowflakeStrategy])
  }
}
