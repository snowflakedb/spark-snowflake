/*
 * Copyright 2015-2016 Snowflake Computing
 * Copyright 2015 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.snowflake.spark.snowflake

import java.net.URI
import java.sql.Connection

import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

import scala.util.Random
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import org.slf4j.LoggerFactory


/**
 * Base class for writing integration tests which run against a real Snowflake cluster.
 */
trait IntegrationSuiteBase
  extends QueryTest
  with Matchers
  with BeforeAndAfterAll
  with BeforeAndAfterEach {

  private val log = LoggerFactory.getLogger(getClass)

  /** We read config from this file */
  private final val CONFIG_FILE_VARIABLE = "IT_SNOWFLAKE_CONF"

  protected def loadConfig(): Map[String, String] = {
    val fname = System.getenv(CONFIG_FILE_VARIABLE)
    if (fname == null)
      fail(s"Must set $CONFIG_FILE_VARIABLE environment variable")
    Utils.readMapFromFile(sc, fname)
  }

  protected var connectorOptions: Map[String, String] = _
  protected var connectorOptionsNoTable: Map[String, String] = _

  // Options encoded as a Spark-sql string - no dbtable
  protected var connectorOptionsString : String = _

  protected var params : MergedParameters = _

  protected def getConfigValue(name : String): String = {
    connectorOptions.getOrElse(name.toLowerCase, {
      fail("Config file needs to contain $name value")
    })
  }

  // AWS access variables
  protected var AWS_ACCESS_KEY_ID: String = _
  protected var AWS_SECRET_ACCESS_KEY: String = _
  // Path to a directory in S3 (e.g. 's3n://bucket-name/path/to/scratch/space').
  private var AWS_S3_SCRATCH_SPACE: String = _

  /**
   * Random suffix appended appended to table and directory names in order to avoid collisions
   * between separate Travis builds.
   */
  protected val randomSuffix: String = Math.abs(Random.nextLong()).toString

  protected var tempDir: String = _

  /**
   * Spark Context with Hadoop file overridden to point at our local test data file for this suite,
   * no-matter what temp directory was generated and requested.
   */
  protected var sc: SparkContext = _
  protected var sqlContext: SQLContext = _
  protected var conn: Connection = _

  def runSql(query: String): Unit = {
    log.debug("RUNNING: " + Utils.sanitizeQueryText(query))
    sqlContext.sql(query).collect()
  }

  def jdbcUpdate(query: String): Unit = {
    log.debug("RUNNING: " + Utils.sanitizeQueryText(query))
    conn.createStatement.executeUpdate(query)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Always run in UTC
    System.setProperty("user.timezone", "GMT")

    sc = new SparkContext("local", "SnowflakeSourceSuite")

    // Initialize variables
    connectorOptions = loadConfig()
    connectorOptionsNoTable = connectorOptions.filterKeys(_ != "dbtable")
    params = Parameters.mergeParameters(connectorOptions)
    // Create a single string with the Spark SQL options
    connectorOptionsString = connectorOptionsNoTable.map{ case (key, value) => s"""$key "$value"""" }.mkString(" , ")

    AWS_ACCESS_KEY_ID = getConfigValue("awsAccessKey")
    AWS_SECRET_ACCESS_KEY = getConfigValue("awsSecretKey")
    AWS_S3_SCRATCH_SPACE = getConfigValue("tempDir")
    require(AWS_S3_SCRATCH_SPACE.startsWith("s3n://") || AWS_S3_SCRATCH_SPACE.startsWith("file://"),
      "must use s3n:// or file:// URL")
    tempDir = params.rootTempDir + randomSuffix + "/"

    // Bypass Hadoop's FileSystem caching mechanism so that we don't cache the credentials:
    sc.hadoopConfiguration.setBoolean("fs.s3.impl.disable.cache", true)
    sc.hadoopConfiguration.setBoolean("fs.s3n.impl.disable.cache", true)
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)
    conn = DefaultJDBCWrapper.getConnector(params)

    // Force UTC also on the JDBC connection
    jdbcUpdate("alter session set timezone='UTC'")

    sqlContext = new TestHiveContext(sc, loadTestTables = false)

    // Use fewer partitions to make tests faster
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")
  }

  override def afterAll(): Unit = {
    try {
      val conf = new Configuration(false)
      conf.set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
      conf.set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)
      // Bypass Hadoop's FileSystem caching mechanism so that we don't cache the credentials:
      conf.setBoolean("fs.s3.impl.disable.cache", true)
      conf.setBoolean("fs.s3n.impl.disable.cache", true)
      val fs = FileSystem.get(URI.create(tempDir), conf)
      fs.delete(new Path(tempDir), true)
      fs.close()
    } finally {
      try {
        conn.close()
      } finally {
        try {
          sc.stop()
        } finally {
          super.afterAll()
        }
      }
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
  }

  /**
   * Save the given DataFrame to Snowflake, then load the results back into a DataFrame and check
   * that the returned DataFrame matches the one that we saved.
   *
   * @param tableName the table name to use
   * @param df the DataFrame to save
   * @param expectedSchemaAfterLoad if specified, the expected schema after loading the data back
   *                                from Snowflake. This should be used in cases where you expect
   *                                the schema to differ due to reasons like case-sensitivity.
   * @param saveMode the [[SaveMode]] to use when writing data back to Snowflake
   */
  def testRoundtripSaveAndLoad(
      tableName: String,
      df: DataFrame,
      expectedSchemaAfterLoad: Option[StructType] = None,
      saveMode: SaveMode = SaveMode.ErrorIfExists): Unit = {
    try {
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(saveMode)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = sqlContext.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .load()
      assert(loadedDf.schema === expectedSchemaAfterLoad.getOrElse(df.schema))
      checkAnswer(loadedDf, df.collect())
    } finally {
      conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
      conn.commit()
    }
  }
}
