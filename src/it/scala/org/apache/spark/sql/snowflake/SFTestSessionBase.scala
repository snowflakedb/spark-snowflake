package org.apache.spark.sql.snowflake

import net.snowflake.spark.snowflake.{IntegrationEnv, Parameters}
import org.apache.spark.sql.{DatasetHolder, Encoder, SQLContext, SQLImplicits, SparkSession}

import scala.language.implicitConversions
import scala.util.Random

trait SFTestSessionBase extends IntegrationEnv {
  private var _spark: SFTestWrapperSparkSession = null
  private var tempSchema: String = _
  private var optionsTestTempSchema: Map[String, String] = _

  protected object SFTestImplicits extends SQLImplicits {
    override def _sqlContext: SQLContext = null

    override implicit def localSeqToDatasetHolder[T: Encoder](data: Seq[T]): DatasetHolder[T] = {
      SFDatasetHolder(data, getSnowflakeSession())
    }
  }

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

  protected object testSQLImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = _spark.sqlContext
  }
}
