/*
 * Copyright 2020 Snowflake Computing
 * Copyright 2020 Databricks
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

import java.io.File
import java.sql.{Connection, Timestamp}
import java.time.{LocalDateTime, ZonedDateTime}
import java.util.TimeZone

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.Random

/**
  * Base class for writing integration tests which run against a real Snowflake cluster.
  */
trait IntegrationEnv
    extends FunSuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val log = LoggerFactory.getLogger(getClass)

  /** We read config from this file */
  private final val CONFIG_FILE_VARIABLE = "IT_SNOWFLAKE_CONF"
  private final val CONFIG_JSON_FILE = "snowflake.travis.json"
  private[snowflake] final val SNOWFLAKE_TEST_ACCOUNT = "SNOWFLAKE_TEST_ACCOUNT"
  protected final val MISSING_PARAM_ERROR =
    "Missing required configuration value: "

  protected val DEFAULT_LOG4J_PROPERTY = "src/it/resources/log4j_default.properties"

  protected def reconfigureLogFile(propertyFileName: String): Unit = {
    // Load the log properties for the security test to output more info
    val log4jfile = new File(propertyFileName)
    PropertyConfigurator.configure(log4jfile.getAbsolutePath)
  }

  // Some integration tests are for large Data, it needs long time to run.
  // But when the test suite is run on travis, there are job time limitation.
  // The suite can't be finished successfully for USE_COPY_UNLOAD.
  // So this environment variable is introduced to skip the test for big data.
  // It only should be used on travis for USE_COPY_UNLOAD.
  // By default, it is false
  final val SKIP_BIG_DATA_TEST = "SKIP_BIG_DATA_TEST"
  protected lazy val skipBigDataTest : Boolean =
    "true".equalsIgnoreCase(System.getenv(SKIP_BIG_DATA_TEST))

  // Some integration tests are for extra test coverage,
  // they are not necessary to run in all test env.
  // Typically, the test is run in one test env.
  // By default, it is false
  final val EXTRA_TEST_FOR_COVERAGE = "EXTRA_TEST_FOR_COVERAGE"
  protected lazy val extraTestForCoverage : Boolean =
    "true".equalsIgnoreCase(System.getenv(EXTRA_TEST_FOR_COVERAGE))

  protected lazy val configsFromEnv: Map[String, String] = {
    var settingsMap = new mutable.HashMap[String, String]
    Parameters.KNOWN_PARAMETERS foreach { param =>
      val opt = readConfigValueFromEnv(param)
      if (opt.isDefined) {
        settingsMap += (param -> opt.get)
      }
    }
    settingsMap.toMap
  }

  protected lazy val configsFromFile: Map[String, String] = {
    val fname = System.getenv(CONFIG_FILE_VARIABLE)
    if (fname != null) {
      Utils.readMapFromFile(sc, fname)
    } else Map.empty[String, String]
  }

  // Merges maps, preferring file values over env ones
  protected def loadConfig(): Map[String, String] =
    loadJsonConfig().getOrElse(configsFromFile) ++ configsFromEnv

  // Used for internal integration testing in SF env.
  protected def readConfigValueFromEnv(name: String): Option[String] = {
    scala.util.Properties.envOrNone(s"SPARK_CONN_ENV_${name.toUpperCase}")
  }

  protected def dropOldStages(stagePrefix: String, hoursAgo: Long): Unit = {
    val rs = conn.createStatement().executeQuery(
      s"SHOW STAGES LIKE '$stagePrefix%'")

    val today = Timestamp.valueOf(LocalDateTime.now())
    val expireTs = Timestamp.valueOf(
      LocalDateTime.now().minusHours(hoursAgo))
    println(s"Start clean up on $today. Drop test stages created before $expireTs")

    while (rs.next()) {
      val createTs = rs.getTimestamp("created_on")
      val name = rs.getString("name")
      if (expireTs.compareTo(createTs) > 0) {
        println(s"On $today, drop stage $name created_on: $createTs")
        conn.createStatement().executeQuery(s"drop stage if exists $name")
      }
    }
  }

  protected def replaceOption(originalOptions: Map[String, String],
                              optionName: String,
                              optionValue: String): Map[String, String] = {
    var resultOptions: Map[String, String] = Map()
    originalOptions.foreach(tup => {
      if (!tup._1.equalsIgnoreCase(optionName)) {
        resultOptions += tup
      }
    })
    resultOptions += (optionName -> optionValue)
    resultOptions
  }

  protected def getRowCount(tableName: String,
                            sfOptions: Map[String, String] = connectorOptionsNoTable
                           ): Long = {
    val rs = Utils.runQuery(sfOptions,
      s"select count(*) from $tableName")
    rs.next()
    rs.getLong(1)
  }

  protected var connectorOptions: Map[String, String] = _
  protected var connectorOptionsNoTable: Map[String, String] = _

  protected var connectorOptionsNoExternalStageNoTable: Map[String, String] = _

  // Options encoded as a Spark-sql string - no dbtable
  protected var connectorOptionsString: String = _

  protected var params: MergedParameters = _

  protected def getConfigValue(name: String,
                               required: Boolean = true): String = {

    connectorOptions.getOrElse(name.toLowerCase, {
      if (required) {
        fail(s"$MISSING_PARAM_ERROR $name")
      } else {
        null
      }
    })
  }

  /**
    * Random suffix appended appended to table and directory names in order to avoid collisions
    * between separate Travis builds.
    */
  protected val randomSuffix: String = Math.abs(Random.nextLong()).toString

  /**
    * Spark Context with Hadoop file overridden to point at our local test data file for this suite,
    * no-matter what temp directory was generated and requested.
    */
  protected var sc: SparkContext = _
  protected var conn: Connection = _

  protected var sparkSession: SparkSession = _

  def runSql(query: String): Unit = {
    log.debug("RUNNING: " + Utils.sanitizeQueryText(query))
    sparkSession.sql(query).collect()
  }

  def jdbcUpdate(query: String): Unit = {
    log.debug("RUNNING: " + Utils.sanitizeQueryText(query))
    val _ = conn.createStatement.executeQuery(query)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Always run in UTC
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"))
    log.debug(s"time zone:${ZonedDateTime.now()}")

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("SnowflakeSourceSuite")

    sc = SparkContext.getOrCreate(conf)

    // Initialize variables
    connectorOptions = loadConfig()
    connectorOptionsNoTable = connectorOptions.filterKeys(_ != "dbtable").toMap
    connectorOptionsNoExternalStageNoTable =
      connectorOptionsNoTable.filterKeys(_ != "tempdir").toMap
    params = Parameters.mergeParameters(connectorOptions.toMap)
    // Create a single string with the Spark SQL options
    connectorOptionsString = connectorOptionsNoTable
      .map {
        case (key, value) => s"""$key "$value""""
      }
      .mkString(" , ")

    conn = DefaultJDBCWrapper.getConnector(params)

    // Force UTC also on the JDBC connection
    jdbcUpdate("alter session set timezone='UTC'")

    // Use fewer partitions to make tests faster
    sparkSession = SparkSession.builder
      .master("local")
      .appName("SnowflakeSourceSuite")
      .config("spark.sql.shuffle.partitions", "6")
      // "spark.sql.legacy.timeParserPolicy = LEGACY" is added to allow
      // spark 3.0 to support legacy conversion for unix_timestamp().
      // It may not be necessary for spark 2.X.
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
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

  override protected def beforeEach(): Unit = {
    super.beforeEach()
  }

  /**
   * read snowflake.travis.json
   */
  def loadJsonConfig(): Option[Map[String, String]] = {

    var result: Map[String, String] = Map()
    def read(node: JsonNode): Unit =
      node
        .fields()
        .asScala
        .foreach(
          entry => result = result + (entry.getKey.toLowerCase -> entry.getValue.asText())
        )

    try {
      val jsonConfigFile = Source.fromFile(CONFIG_JSON_FILE)
      val file = jsonConfigFile.mkString
      val mapper: ObjectMapper = new ObjectMapper()
      val json = mapper.readTree(file)
      val commonConfig = json.get("common")
      val accountConfig = json.get("account_info")
      val accountName: String =
        Option(System.getenv(SNOWFLAKE_TEST_ACCOUNT)).getOrElse("aws")

      log.info(s"test account: $accountName")

      read(commonConfig)

      var accountUrlJson: Option[JsonNode] = None
      for (i <- 0 until accountConfig.size()) {
        val node = accountConfig.get(i)
        if (node.get("name").asText().equals(accountName)) {
          accountUrlJson = Some(node.get("config"))
        }
      }
      read(accountUrlJson.get)

      log.info(s"load config from $CONFIG_JSON_FILE")
      jsonConfigFile.close()
      Some(result)
    } catch {
      case _: Throwable =>
        log.info(s"Can't read $CONFIG_JSON_FILE, load config from other source")
        None
    }
  }
}
