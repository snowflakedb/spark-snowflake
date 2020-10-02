package org.apache.spark.sql.thundersnow

import net.snowflake.spark.snowflake.{IntegrationEnv, IntegrationSuiteBase}
import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}

trait SFTestSessionBase extends IntegrationEnv {
  private var _spark: SFTestWrapperSparkSession = null

  protected object testSQLImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = _spark.sqlContext
  }

  def createSession(): SFTestWrapperSparkSession = {
    System.setProperty("hadoop.home.dir", "/tmp/hadoop")
    SFTestWrapperSparkSession(sc, connectorOptionsTestTempSchema)
  }

  private def initializeSession(): Unit = {
    if (_spark == null) {
      _spark = createSession
      SparkSession.setDefaultSession(_spark)
      SparkSession.setActiveSession(_spark)
    }
  }

  def getSnowflakeSession(): SFTestWrapperSparkSession = {
    initializeSession()
    _spark
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val tempSchema = connectorOptionsTestTempSchema.get("sfschema")
    if (tempSchema.isDefined) {
      jdbcUpdate(s"create or replace schema ${tempSchema.get}")
    }
    initializeSession()
  }

  override def afterAll(): Unit = {
    try {
      if (_spark != null) {
        try {
          _spark.sessionState.catalog.reset()
        } finally {
          _spark.stop()
          _spark = null
        }
      }
    } finally {
      val tempSchema = connectorOptionsTestTempSchema.get("sfschema")
      if (tempSchema.isDefined) {
        jdbcUpdate(s"drop schema if exists ${tempSchema.get}")
      }
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }
}
