package org.apache.spark.sql.thundersnow

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object WrapperUtils {

  private val locker = new Object

  private var curSparkContext: SparkContext = createSparkContext

  def sparkContext: SparkContext = locker.synchronized {
    if (curSparkContext.isStopped) curSparkContext = createSparkContext
    curSparkContext
  }

  private def createSparkContext: SparkContext = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val config = new SparkConf().setAppName("TS").setMaster("local[1]")
    config.set("spark.ui.enabled", "false")
    new SparkContext(config)
  }

  def sparkSession: SparkSession = {
    val session = new SparkSession(sparkContext)
    SparkSession.setActiveSession(session)
    session
  }

}
