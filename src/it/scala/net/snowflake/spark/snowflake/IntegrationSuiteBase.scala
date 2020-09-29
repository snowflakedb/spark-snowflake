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

import java.sql.{Connection, Timestamp}
import java.time.{LocalDateTime, ZonedDateTime}
import java.util.TimeZone

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{
  JsonNode,
  ObjectMapper
}
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source
import scala.collection.JavaConversions._
import scala.util.matching.Regex

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
  private final val CONFIG_JSON_FILE = "snowflake.travis.json"
  private final val SNOWFLAKE_TEST_ACCOUNT = "SNOWFLAKE_TEST_ACCOUNT"
  protected final val MISSING_PARAM_ERROR =
    "Missing required configuration value: "

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
    } else scala.collection.Map.empty[String, String]
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
    connectorOptionsNoTable = connectorOptions.filterKeys(_ != "dbtable")
    connectorOptionsNoExternalStageNoTable =
      connectorOptionsNoTable.filterKeys(_ != "tempdir")
    params = Parameters.mergeParameters(connectorOptions)
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

  def getAzureURL(input: String): String = {
    val azure_url = "wasbs?://([^@]+)@([^.]+)\\.([^/]+)/(.+)?".r
    input match {
      case azure_url(container, account, endpoint, _) =>
        s"fs.azure.sas.$container.$account.$endpoint"
      case _ => throw new IllegalArgumentException(s"invalid wasb url: $input")
    }
  }

  /**
    * Verify that the pushdown was done by looking at the generated SQL,
    * and check the results are as expected
    */
  def testPushdown(reference: String,
                   result: DataFrame,
                   expectedAnswer: Seq[Row],
                   bypass: Boolean = false,
                   printSqlText: Boolean = false): Unit = {

    // Verify the query issued is what we expect
    checkAnswer(result, expectedAnswer)

    // It is used to retrieve expected query text.
    if (printSqlText) {
      println(Utils.getLastSelect)
    }

    if (!bypass) {
      assert(
        Utils.getLastSelect.replaceAll("\\s+", "").toLowerCase == reference.trim
          .replaceAll("\\s+", "")
          .toLowerCase
      )
    }
  }

  /**
    * Save the given DataFrame to Snowflake, then load the results back into a DataFrame and check
    * that the returned DataFrame matches the one that we saved.
    *
    * @param tableName               the table name to use
    * @param df                      the DataFrame to save
    * @param expectedSchemaAfterLoad if specified, the expected schema after loading the data back
    *                                from Snowflake. This should be used in cases where you expect
    *                                the schema to differ due to reasons like case-sensitivity.
    * @param saveMode                the [[SaveMode]] to use when writing data back to Snowflake
    */
  def testRoundtripSaveAndLoad(
    tableName: String,
    df: DataFrame,
    expectedSchemaAfterLoad: Option[StructType] = None,
    saveMode: SaveMode = SaveMode.ErrorIfExists
  ): Unit = {
    try {
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(saveMode)
        .save()
      assert(DefaultJDBCWrapper.tableExists(conn, tableName))
      val loadedDf = sparkSession.read
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

  /**
    * read snowflake.travis.json
    */
  def loadJsonConfig(): Option[Map[String, String]] = {

    var result: Map[String, String] = Map()
    def read(node: JsonNode): Unit =
      node
        .fields()
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

      read(
        (
          for (i <- 0 until accountConfig.size()
               if accountConfig.get(i).get("name").asText() == accountName)
            yield accountConfig.get(i).get("config")
        ).get(0)
      )

      log.info(s"load config from $CONFIG_JSON_FILE")
      jsonConfigFile.close()
      Some(result)
    } catch {
      case _: Throwable =>
        log.info(s"Can't read $CONFIG_JSON_FILE, load config from other source")
        None
    }
  }

  // Utility function to drop some garbage test tables.
  // Be careful to use this function which drops a bunch of tables.
  // Suggest you to run with "printOnly = true" to make sure the tables are correct.
  // And then run with "printOnly = false"
  // For example, dropTestTables(".*TEST_TABLE_.*\\d+".r, true)
  def dropTestTables(regex: Regex, printOnly: Boolean): Unit = {
    val statement = conn.createStatement()

    statement.execute("show tables")
    val resultset = statement.getResultSet
    while (resultset.next()) {
      val tableName = resultset.getString(2)
      tableName match {
        case regex() =>
          if (printOnly) {
            // scalastyle:off println
            println(s"will drop table: $tableName")
            // scalastyle:on println
          } else {
            jdbcUpdate(s"drop table $tableName")
          }
        case _ => None
      }
    }
  }
}
