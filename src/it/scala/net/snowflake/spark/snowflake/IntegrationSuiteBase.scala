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
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.mutable

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
  protected final val MISSING_PARAM_ERROR = "Missing required configuration value: "

  protected lazy val configsFromEnv: Map[String, String] = {
    var settingsMap = new mutable.HashMap[String, String]
    Parameters.KNOWN_PARAMETERS foreach { param =>
      val opt = readConfigValueFromEnv(param)
      if (opt.isDefined)
        settingsMap += (param -> opt.get)
    }
    settingsMap.toMap
  }

  protected lazy val configsFromFile: Map[String, String] = {
    val fname = System.getenv(CONFIG_FILE_VARIABLE)
    if (fname != null) {
      Utils.readMapFromFile(sc, fname)
    } else scala.collection.Map.empty[String, String]
  }

  // Merges maps, preferring file values over env ones
  protected def loadConfig(): Map[String, String] = {
    (configsFromFile.keySet ++ configsFromEnv.keySet) map { key =>
      (key -> configsFromFile.getOrElse(key,
                                        configsFromEnv.getOrElse(key, "")))
    } toMap
  }

  // Used for internal integration testing in SF env.
  protected def readConfigValueFromEnv(name: String): Option[String] = {
    scala.util.Properties.envOrNone(s"SPARK_CONN_ENV_${name.toUpperCase}")
  }

  protected var connectorOptions: Map[String, String]        = _
  protected var connectorOptionsNoTable: Map[String, String] = _
  protected var connectorOptionsNoTableNoS3: Map[String, String] = _

  // Options encoded as a Spark-sql string - no dbtable
  protected var connectorOptionsString: String = _

  protected var params: MergedParameters = _

  protected def getConfigValue(name: String,
                               required: Boolean = true): String = {

    connectorOptions.getOrElse(name.toLowerCase, {
      if (required)
        fail(s"$MISSING_PARAM_ERROR $name")
      else null
    })
  }

  // AWS access variables
  protected var AWS_ACCESS_KEY_ID: String     = _
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
  protected var sc: SparkContext       = _
  protected var sqlContext: SQLContext = _
  protected var conn: Connection       = _

  protected var sparkSession: SparkSession = _

  def runSql(query: String): Unit = {
    log.debug("RUNNING: " + Utils.sanitizeQueryText(query))
    sqlContext.sql(query).collect()
  }

  def jdbcUpdate(query: String): Unit = {
    log.debug("RUNNING: " + Utils.sanitizeQueryText(query))
    val _ = conn.createStatement.executeQuery(query)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Always run in UTC
    System.setProperty("user.timezone", "GMT")

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SnowflakeSourceSuite")

    sc = SparkContext.getOrCreate(conf)

    // Initialize variables
    connectorOptions = loadConfig()
    connectorOptionsNoTable = connectorOptions.filterKeys(_ != "dbtable")
    connectorOptionsNoTableNoS3 = connectorOptionsNoTable.filterKeys(_ != "tempdir")
    params = Parameters.mergeParameters(connectorOptions)
    // Create a single string with the Spark SQL options
    connectorOptionsString = connectorOptionsNoTable.map {
      case (key, value) => s"""$key "$value""""
    }.mkString(" , ")

    AWS_ACCESS_KEY_ID = getConfigValue("awsAccessKey", false)
    AWS_SECRET_ACCESS_KEY = getConfigValue("awsSecretKey", false)
    AWS_S3_SCRATCH_SPACE = getConfigValue("tempDir", false)

    if (AWS_S3_SCRATCH_SPACE != null) {
      require(AWS_S3_SCRATCH_SPACE.startsWith("s3n://") || AWS_S3_SCRATCH_SPACE
                .startsWith("file://"),
              "must use s3n:// or file:// URL")
      tempDir = params.rootTempDir + randomSuffix + "/"
    }

    // Bypass Hadoop's FileSystem caching mechanism so that we don't cache the credentials:
    sc.hadoopConfiguration.setBoolean("fs.s3.impl.disable.cache", true)
    sc.hadoopConfiguration.setBoolean("fs.s3n.impl.disable.cache", true)

    if (AWS_SECRET_ACCESS_KEY != null && AWS_ACCESS_KEY_ID != null) {
      sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
      sc.hadoopConfiguration
        .set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)
    }
    conn = DefaultJDBCWrapper.getConnector(params)

    // Force UTC also on the JDBC connection
    jdbcUpdate("alter session set timezone='UTC'")

    sqlContext = new TestHiveContext(sc, loadTestTables = false)

    // Use fewer partitions to make tests faster
    sqlContext.setConf("spark.sql.shuffle.partitions", "6")

    sparkSession = sqlContext.sparkSession
  }

  override def afterAll(): Unit = {
    try {
      if (AWS_ACCESS_KEY_ID != null && AWS_SECRET_ACCESS_KEY != null && tempDir != null) {
        val conf = new Configuration(false)
        conf.set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY_ID)
        conf.set("fs.s3n.awsSecretAccessKey", AWS_SECRET_ACCESS_KEY)
        // Bypass Hadoop's FileSystem caching mechanism so that we don't cache the credentials:

        conf.setBoolean("fs.s3.impl.disable.cache", true)
        conf.setBoolean("fs.s3n.impl.disable.cache", true)
        val fs = FileSystem.get(URI.create(tempDir), conf)
        fs.delete(new Path(tempDir), true)
        fs.close()
      }
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
    * Verify that the pushdown was done by looking at the generated SQL,
    * and check the results are as expected
    */
  def testPushdown(reference: String,
                   result: DataFrame,
                   expectedAnswer: Seq[Row],
                   bypass: Boolean = false): Unit = {

    // Verify the query issued is what we expect
    checkAnswer(result, expectedAnswer)

    if (!bypass) {
      assert(
        Utils.getLastSelect.replaceAll("\\s+", "") == reference.trim
          .replaceAll("\\s+", ""))
    }
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
      conn.createStatement.executeUpdate(s"drop table if exists $tableName")
      conn.commit()
    }
  }
}
