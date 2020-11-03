/*
 * Copyright 2015-2020 Snowflake Computing
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

import java.sql.{Connection, DriverManager}
import java.time.Instant
import java.util.Properties

import com.bettercloud.vault.{Vault, VaultConfig}
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import net.snowflake.spark.snowflake.ClusterTest.log
import net.snowflake.spark.snowflake.Parameters.MergedParameters

import scala.collection.mutable
import scala.io.Source

object TestUtils {
  lazy val githubRunId: String = {
    val jobTime = System.getenv(TestUtils.GITHUB_RUN_ID)
    if (jobTime == null) {
      throw new Exception(s"env variable ${TestUtils.GITHUB_RUN_ID} needs to be set")
    }
    jobTime
  }
  // Load sfOptions from config file and env.
  lazy val sfOptions: Map[String, String] = {
    var configFile = System.getenv(SNOWFLAKE_TEST_CONFIG)
    if (configFile == null) {
      configFile = "snowflake.travis.json"
    }
    overWriteLoginOptionsWithVault(overWriteOptionsWithEnv(loadJsonConfig(configFile))).get
  }
  // Load sfOptions from config file and env, but exclude "table"
  lazy val sfOptionsNoTable: Map[String, String] = {
    var resultOptions = new mutable.HashMap[String, String]
    for ((key, value) <- sfOptions) {
      if (key != "dbtable") {
        resultOptions += (key -> value)
      }
    }
    resultOptions.toMap
  }
  // parameters for connection
  lazy val param: MergedParameters = Parameters.mergeParameters(sfOptions)
  val SNOWFLAKE_TEST_ACCOUNT = "SNOWFLAKE_TEST_ACCOUNT"
  val SNOWFLAKE_TEST_CONFIG = "SNOWFLAKE_TEST_CONFIG"
  val GITHUB_TEST_RESULTS_TABLE = "CLUSTER_TEST_RESULT_TABLE"
  val GITHUB_SHA = "GITHUB_SHA"
  val GITHUB_RUN_ID = "GITHUB_RUN_ID"
  val STRESS_TEST_RESULTS_TABLE = "STRESS_TEST_RESULTS_TABLE"
  val STRESS_TEST_DETAILED_RESULTS_TABLE = "STRESS_TEST_DETAILED_RESULTS_TABLE"
  // Unlike in the Github environment, where we have unique commit ids,
  // stress test runs need to have a unique id in a schema, for which
  // we will use a Sequence object
  val STRESS_TEST_SEQ_NAME = "STRESS_TEST_SEQ"
  val SNOWFLAKE_NAME = "net.snowflake.spark.snowflake"
  val JDBC_DRIVER = "net.snowflake.client.jdbc.SnowflakeDriver"
  // Test case result status
  val TEST_RESULT_STATUS_NOT_STARTED = "NotStarted"
  val TEST_RESULT_STATUS_INIT = "Initialized"
  val TEST_RESULT_STATUS_START = "Started"
  val TEST_RESULT_STATUS_SUCCESS = "Success"
  val TEST_RESULT_STATUS_FAIL = "Fail"
  val TEST_RESULT_STATUS_EXCEPTION = "Exception"
  // Reason strings
  val TEST_RESULT_REASON_NO_REASON = "no reason"
  // For Performance and Stress Testing
  val STRESS_TEST_SOURCES = "STRESS_TEST_SOURCES"
  // Vault configuration variables
  protected val CONFIG_VAULT_URL_ENV_VAR = "VAULT_URL"
  protected val VAULT_KEY_ENV_VAR = "VAULT_KEY"

  /**
   * Used to format a string for writing into a test table the time elapsed for a test
   */
  def formatTimeElapsed(context: TestStatus): String = {
    val usedTime = context.taskEndTime - context.taskStartTime
    if (usedTime < 0) {
      s"Wrong time: Start ${context.taskStartTime} end: ${context.taskEndTime}"
    } else if (usedTime < 1000) {
      s"$usedTime ms"
    } else if (usedTime < 1000 * 60) {
      "%.2f seconds".format(usedTime.toDouble / 1000)
    } else {
      "%.2f minutes".format(usedTime.toDouble / 1000 / 60)
    }
  }

  /**
   * Generate a timestamp string formatted, from a millis timestamp
   */
  def formatTimestamp(timestampMillis: Long): String = {
    Instant.ofEpochMilli(timestampMillis).toString
  }

  /**
   * read sfOptions from json config e.g. snowflake.travis.json
   */
  def loadJsonConfig(configFile: String): Option[Map[String, String]] = {

    var result: Map[String, String] = Map()

    def read(node: JsonNode): Unit = {
      val itr = node.fields()
      while (itr.hasNext) {
        val entry = itr.next()
        result = result + (entry.getKey.toLowerCase -> entry.getValue.asText())
      }
    }

    try {
      val jsonConfigFile = Source.fromFile(configFile)
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
        ).head)

      log.info(s"load config from $configFile")
      jsonConfigFile.close()
      Some(result)
    } catch {
      case e: Throwable =>
        log.info(s"Can't read $configFile, reason: ${e.getMessage}")
        None
    }
  }

  // If use of Hashicorp Vault is enabled, use it to retrieve login credentials
  def overWriteLoginOptionsWithVault(
      sfOptions: Option[Map[String, String]]): Option[Map[String, String]] = {
    if (sfOptions.isDefined) {
      val vaultURL = System.getenv(CONFIG_VAULT_URL_ENV_VAR)
      val vaultKey = System.getenv(VAULT_KEY_ENV_VAR)

      // Don't touch this if either of the two necessary env variables are not set
      if (vaultURL == null || vaultKey == null) {
        sfOptions
      } else {
        var resultOptions = new mutable.HashMap[String, String]
        val loginValues = Set(
          Parameters.PARAM_SF_URL,
          Parameters.PARAM_SF_ACCOUNT,
          Parameters.PARAM_SF_USER,
          Parameters.PARAM_SF_PASSWORD,
          Parameters.PARAM_PEM_PRIVATE_KEY)

        // Remove login info
        resultOptions = resultOptions ++ sfOptions.get.filterKeys(param =>
          !loginValues.contains(param.toLowerCase))

        val vault = new Vault(new VaultConfig().address(vaultURL).build())

        for (key <- loginValues) {
          val value = vault.logical().read(vaultKey).getData.get(key)

          if (value != null) {
            resultOptions += (key -> value)
          }
        }
        Some(resultOptions.toMap)
      }
    } else {
      None
    }
  }

  // Overwite options with environment variable settings.
  def overWriteOptionsWithEnv(
      sfOptions: Option[Map[String, String]]): Option[Map[String, String]] = {
    if (sfOptions.isDefined) {
      var resultOptions = new mutable.HashMap[String, String]
      // Retrieve all options from Environment variables
      Parameters.KNOWN_PARAMETERS foreach { param =>
        val opt = readConfigValueFromEnv(param)
        if (opt.isDefined) {
          log.info(s"Get config from env: $param")
          resultOptions += (param -> opt.get)
        }
      }
      // Merge the options that are not set Env
      for ((key, value) <- sfOptions.get) {
        if (resultOptions.get(key).isEmpty) {
          resultOptions += (key -> value)
        }
      }
      Some(resultOptions.toMap)
    } else {
      None
    }
  }

  // Used for internal integration testing in SF env.
  def readConfigValueFromEnv(name: String): Option[String] = {
    scala.util.Properties.envOrNone(s"SPARK_CONN_ENV_${name.toUpperCase}")
  }

  /**
   * Get a connection based on the provided parameters
   */
  def getJDBCConnection(params: MergedParameters): Connection = {
    // Derive class name
    try Class.forName("com.snowflake.client.jdbc.SnowflakeDriver")
    catch {
      case _: ClassNotFoundException =>
        System.err.println("Driver not found")
    }

    val sfURL = params.sfURL
    val jdbcURL = s"""jdbc:snowflake://$sfURL"""

    val jdbcProperties = new Properties()

    // Obligatory properties
    jdbcProperties.put("db", params.sfDatabase)
    jdbcProperties.put("schema", params.sfSchema) // Has a default
    jdbcProperties.put("user", params.sfUser)

    params.privateKey match {
      case Some(privateKey) =>
        jdbcProperties.put("privateKey", privateKey)
      case None =>
        // Adding OAuth Token parameter
        params.sfToken match {
          case Some(value) =>
            jdbcProperties.put("token", value)
          case None => jdbcProperties.put("password", params.sfPassword)
        }
    }
    jdbcProperties.put("ssl", params.sfSSL) // Has a default
    // Optional properties
    if (params.sfAccount.isDefined) {
      jdbcProperties.put("account", params.sfAccount.get)
    }
    if (params.sfWarehouse.isDefined) {
      jdbcProperties.put("warehouse", params.sfWarehouse.get)
    }
    if (params.sfRole.isDefined) {
      jdbcProperties.put("role", params.sfRole.get)
    }
    params.getTimeOutputFormat match {
      case Some(value) =>
        jdbcProperties.put(Parameters.PARAM_TIME_OUTPUT_FORMAT, value)
      case _ => // No default value for it.
    }
    params.getQueryResultFormat match {
      case Some(value) =>
        jdbcProperties.put(Parameters.PARAM_JDBC_QUERY_RESULT_FORMAT, value)
      case _ => // No default value for it.
    }

    // Set up proxy info if it is configured.
    params.setJDBCProxyIfNecessary(jdbcProperties)

    // Adding Authenticator parameter
    params.sfAuthenticator match {
      case Some(value) =>
        jdbcProperties.put("authenticator", value)
      case _ => // No default value for it.
    }

    // Always set CLIENT_SESSION_KEEP_ALIVE.
    // Note, can be overridden with options
    jdbcProperties.put("client_session_keep_alive", "true")

    // Force DECIMAL for NUMBER (SNOW-33227)
    jdbcProperties.put("JDBC_TREAT_DECIMAL_AS_INT", "false")

    // Add extra properties from sfOptions
    val extraOptions = params.sfExtraOptions
    for ((k: String, v: Object) <- extraOptions) {
      jdbcProperties.put(k.toLowerCase, v.toString)
    }

    DriverManager.getConnection(jdbcURL, jdbcProperties)
  }

}
