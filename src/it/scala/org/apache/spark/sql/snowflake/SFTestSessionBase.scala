package org.apache.spark.sql.snowflake

import net.snowflake.spark.snowflake.{IntegrationEnv, Parameters}
import org.apache.spark.sql.SparkSession

import scala.language.implicitConversions
import scala.util.Random

trait SFTestSessionBase extends IntegrationEnv {
  private var _spark: SFTestWrapperSparkSession = null
  private var tempSchema: String = _
  private var optionsTestTempSchema: Map[String, String] = _
  private val PORTED_TEST_LOG4J_PROPERTY = "src/it/resources/ported_test_log4j.properties"

  def getSnowflakeSession(): SFTestWrapperSparkSession = {
    initializeSession()
    _spark
  }

  private def initializeSession(): Unit = {
    if (_spark == null) {
      _spark = createSession
      SparkSession.setDefaultSession(_spark)
      SparkSession.setActiveSession(_spark)
    }
  }

  def createSession(): SFTestWrapperSparkSession = {
    System.setProperty("hadoop.home.dir", "/tmp/hadoop")
    SFTestWrapperSparkSession(sc, optionsTestTempSchema)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Run ported test in WARN level to avoid large log volume
    reconfigureLogFile(PORTED_TEST_LOG4J_PROPERTY)
    // connectorOptionsTestTempSchema should have the temp schema replacing
    // sfSchema
    tempSchema = s"testTempSchema_${Math.abs(Random.nextLong()).toString}"
    val optionsWithoutSchema = collection.mutable.Map() ++
      connectorOptionsNoTable.filterKeys(_ != Parameters.PARAM_SF_SCHEMA)

    optionsWithoutSchema.put(Parameters.PARAM_SF_SCHEMA, tempSchema)
    optionsTestTempSchema = optionsWithoutSchema.toMap

    jdbcUpdate(s"create or replace schema $tempSchema")
    initializeSession()
  }

  override def afterAll(): Unit = {
    try {
      // Reset test log level to default.
      reconfigureLogFile(DEFAULT_LOG4J_PROPERTY)
      if (_spark != null) {
        try {
          _spark.sessionState.catalog.reset()
        } finally {
          _spark.stop()
          _spark = null
        }
      }
    } finally {
      jdbcUpdate(s"drop schema if exists $tempSchema")
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  protected object SFTestImplicits extends SnowflakeSQLImplicits(getSnowflakeSession()) {}
}
