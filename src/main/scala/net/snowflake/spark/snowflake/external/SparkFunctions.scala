package org.apache.spark.snowflake

import org.apache.spark.util.ShutdownHookManager

object SparkFunctions {

  def addShutdownHook(hook: () => Unit): AnyRef =
    ShutdownHookManager.addShutdownHook(hook)

}
