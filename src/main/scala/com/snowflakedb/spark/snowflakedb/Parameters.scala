/*
 * Copyright 2015-2016 Snowflake Computing
 * Copyright 2015 TouchType Ltd
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

package com.snowflakedb.spark.snowflakedb

import com.amazonaws.auth.{AWSCredentials, BasicSessionCredentials}

/**
 * All user-specifiable parameters for spark-snowflake, along with their validation rules and
 * defaults.
 */
object Parameters {

  val PARAM_S3_MAX_FILE_SIZE = "s3maxfilesize"
  val DEFAULT_S3_MAX_FILE_SIZE = (10*1000*1000).toString
  val MIN_S3_MAX_FILE_SIZE = 1000000

  val PARAM_SF_TIMEZONE = "sftimezone"
  val TZ_SPARK1 = ""
  val TZ_SPARK2 = "spark"
  val TZ_SF1 = "snowflake"
  val TZ_SF2 = "sf_current"
  val TZ_SF_DEFAULT = "sf_default"

  val PARAM_TEMP_KEY_ID = "temporary_aws_access_key_id"
  val PARAM_TEMP_KEY_SECRET = "temporary_aws_secret_access_key"
  val PARAM_TEMP_SESSION_TOKEN = "temporary_aws_session_token"

  val PARAM_CHECK_BUCKET_CONFIGURATION = "check_bucket_configuration"

  val PARAM_PREACTIONS = "preactions"
  val PARAM_POSTACTIONS = "postactions"

  // List of values that mean "yes" when considered to be Boolean
  val BOOLEAN_VALUES_TRUE  = Set( "on", "yes",  "true", "1",  "enabled")
  val BOOLEAN_VALUES_FALSE = Set("off",  "no", "false", "0", "disabled")

  val DEFAULT_PARAMETERS: Map[String, String] = Map(
    // Notes:
    // * tempdir, dbtable and url have no default and they *must* be provided
    "overwrite" -> "false",
    "diststyle" -> "EVEN",
    "usestagingtable" -> "true",
    PARAM_PREACTIONS -> "",
    PARAM_POSTACTIONS -> ""
  )

  private val KNOWN_PARAMETERS = Set(
    PARAM_S3_MAX_FILE_SIZE,
    PARAM_CHECK_BUCKET_CONFIGURATION,
    PARAM_SF_TIMEZONE,
    PARAM_TEMP_KEY_ID,
    PARAM_TEMP_KEY_SECRET,
    PARAM_TEMP_SESSION_TOKEN,
    PARAM_PREACTIONS,
    PARAM_POSTACTIONS,
    "sfurl",
    "sfuser",
    "sfpassword",
    "sfurl",
    "sfdatabase",
    "sfschema",
    "sfrole",
    "sfcompress",
    "sfssl",
    "tempdir",
    "dbtable",
    "query"
  )

  /**
   * Merge user parameters with the defaults, preferring user parameters if specified
   */
  def mergeParameters(userParameters: Map[String, String]): MergedParameters = {
    if (!userParameters.contains("tempdir")) {
      throw new IllegalArgumentException("'tempdir' is required for all Snowflake loads and saves")
    }
    // Snowflake-todo Add more parameter checking
    if (!userParameters.contains("sfurl")) {
      throw new IllegalArgumentException("A snowflake URL must be provided with 'sfurl' parameter, e.g. 'accountname.snowflakecomputing.com:443'")
    }
    if (!userParameters.contains("sfuser")) {
      throw new IllegalArgumentException("A snowflake user must be provided with 'sfuser' parameter, e.g. 'user1'")
    }
    if (!userParameters.contains("sfpassword")) {
      throw new IllegalArgumentException("A snowflake passsword must be provided with 'sfpassword' parameter, e.g. 'password'")
    }
    if (!userParameters.contains("dbtable") && !userParameters.contains("query")) {
      throw new IllegalArgumentException(
        "You must specify a Snowflake table name with the 'dbtable' parameter or a query with the " +
        "'query' parameter.")
    }
    if (userParameters.contains("dbtable") && userParameters.contains("query")) {
      throw new IllegalArgumentException(
        "You cannot specify both the 'dbtable' and 'query' parameters at the same time.")
    }

    // Check temp keys
    var tempParams = 0
    if (userParameters.contains(PARAM_TEMP_KEY_ID)) tempParams += 1
    if (userParameters.contains(PARAM_TEMP_KEY_SECRET)) tempParams += 1
    if (userParameters.contains(PARAM_TEMP_SESSION_TOKEN)) tempParams += 1
    if (tempParams != 0 && tempParams != 3) {
      throw new IllegalArgumentException(
        s"""If you specify one of
           |${PARAM_TEMP_KEY_ID}, ${PARAM_TEMP_KEY_SECRET} and
           |${PARAM_TEMP_SESSION_TOKEN},
           |you must specify all 3 of them.""".stripMargin.replace("\n", " "))
    }

    val s3maxfilesizeStr = userParameters.get(PARAM_S3_MAX_FILE_SIZE)
    if (s3maxfilesizeStr.isDefined) {
      def toInt(s: String): Option[Int] = {
        try {
          Some(s.toInt)
        } catch {
          case e: Exception => None
        }
      }
      val s3maxfilesize = toInt(s3maxfilesizeStr.get)
      if(s3maxfilesize.isEmpty) {
        throw new IllegalArgumentException(
          s"Cannot parse $PARAM_S3_MAX_FILE_SIZE=${s3maxfilesizeStr.get} as a number")
      }
      if (s3maxfilesize.get < MIN_S3_MAX_FILE_SIZE) {
        throw new IllegalArgumentException(
          s"Specified $PARAM_S3_MAX_FILE_SIZE=${s3maxfilesizeStr.get} too small")
      }
    }

    MergedParameters(DEFAULT_PARAMETERS ++ userParameters)
  }

  /**
   * Adds validators and accessors to string map
   */
  case class MergedParameters(parameters: Map[String, String]) {

    // Simple conversion from string to an object of type
    // String, Boolean or Integer, depending on the value
    private def stringToObject(s: String) : Object = {
      try {
        return new Integer(s)
      } catch {
        case t: Throwable => {}
      }
      if (s.equalsIgnoreCase("true")) {
        return new java.lang.Boolean(true)
      }
      if (s.equalsIgnoreCase("false")) {
        return new java.lang.Boolean(false)
      }
      return s
    }

    private lazy val extraParams : Map[String, Object] = {
      // Find all entries that are in parameters and are not known
      val unknownParamNames = parameters.keySet -- KNOWN_PARAMETERS
      var res = Map[String, Object]()
      unknownParamNames.map(v => res += ( v -> stringToObject(parameters(v))))
      res
    }

    /**
     * A root directory to be used for intermediate data exchange,
     * expected to be on S3, or somewhere that can be written to
     * and read from by Snowflake.
     * Make sure that AWS credentials are available for S3.
     */
    def rootTempDir: String = parameters("tempdir")

    /**
     * Creates a per-query subdirectory in the [[rootTempDir]], with a random UUID.
     */
    def createPerQueryTempDir(): String = Utils.makeTempPath(rootTempDir)

    /**
     * The Snowflake table to be used as the target when loading or writing data.
     */
    def table: Option[TableName] = parameters.get("dbtable").map(_.trim).flatMap { dbtable =>
      // We technically allow queries to be passed using `dbtable` as long as they are wrapped
      // in parentheses. Valid SQL identifiers may contain parentheses but cannot begin with them,
      // so there is no ambiguity in ignoring subqeries here and leaving their handling up to
      // the `query` function defined below.
      if (dbtable.startsWith("(") && dbtable.endsWith(")")) {
        None
      } else {
        Some(TableName(dbtable))
      }
    }

    /**
     * The Snowflake query to be used as the target when loading data.
     */
    def query: Option[String] = {
      var res = parameters.get("query")
      if (res.isDefined) {
        // Remove trailing semicolon if present
        var str : String = res.get.trim
        if (str.charAt(str.length - 1) == ';')
          str = str.substring(0, str.length - 1)
        Some(str)
      } else {
        parameters.get("dbtable")
          .map(_.trim)
          .filter(t => t.startsWith("(") && t.endsWith(")"))
          .map(t => t.drop(1).dropRight(1))
      }
    }

    /**
     * ----------------------------------------- Database connection specification
     */

    /**
     * URL pointing to the snowflake database, simply
     *   host:port
     */
    def sfURL: String = parameters("sfurl")

    /**
     * Snowflake database name
     */
    def sfDatabase: String = parameters("sfdatabase")
    /**
     * Snowflake schema
     */
    def sfSchema: String = parameters.getOrElse("sfschema", "public")
    /**
     * Snowflake warehouse
     */
    def sfWarehouse: Option[String] = parameters.get("sfwarehouse")
    /**
     * Snowflake user
     */
    def sfUser: String = parameters("sfuser")
    /**
     * Snowflake password
     */
    def sfPassword: String = parameters("sfpassword")
    /**
     * Snowflake account - optional
     */
    def sfAccount: Option[String] = parameters.get("sfaccount")
    /**
     * Snowflake SSL on/off - "on" by default
     */
    def sfSSL: String = parameters.getOrElse("sfssl", "on")
    /**
     * Snowflake use compression on/off - "on" by default
     */
    def sfCompress: String = parameters.getOrElse("sfcompress", "on")
    /**
     * Snowflake role - optional
     */
    def sfRole: Option[String] = parameters.get("sfrole")
    /**
     * Snowflake timezone- optional
     */
    def sfTimezone: Option[String] = parameters.get(PARAM_SF_TIMEZONE)
    /**
     * The JDBC driver class name. This is used to make sure the driver is registered before
     * connecting over JDBC.
     */
    def jdbcDriver: Option[String] = parameters.get("jdbcdriver")

    /**
     * Returns a map of options that are not known to the connector,
     * and are passed verbosely to the JDBC driver
     */
    def sfExtraOptions: Map[String, Object] = extraParams

    /** Returns true if bucket lifecycle configuration should be checked */
    def checkBucketConfiguration: Boolean = BOOLEAN_VALUES_TRUE contains
      parameters.getOrElse(PARAM_CHECK_BUCKET_CONFIGURATION, "on").toLowerCase()

    /**
     * Max file size used to move data out from Snowflake
     */
    def s3maxfilesize: String = parameters.getOrElse(PARAM_S3_MAX_FILE_SIZE,
                                                     DEFAULT_S3_MAX_FILE_SIZE)

    def isTimezoneSpark : Boolean = {
      val tz = sfTimezone.getOrElse(TZ_SPARK1)
      (tz.equalsIgnoreCase(TZ_SPARK1)
        || tz.equalsIgnoreCase(TZ_SPARK2))
    }
    def isTimezoneSnowflake : Boolean = {
      val tz = sfTimezone.getOrElse("")
      (tz.equalsIgnoreCase(TZ_SF1)
        || tz.equalsIgnoreCase(TZ_SF2))
    }
    def isTimezoneSnowflakeDefault : Boolean = {
      val tz = sfTimezone.getOrElse("")
      tz.equalsIgnoreCase(TZ_SF_DEFAULT)
    }

    /**
     * -----------------------------------------Various other options
     */

    /**
     * If true, when writing, replace any existing data. When false, append to the table instead.
     * Note that the table schema will need to be compatible with whatever you have in the DataFrame
     * you're writing. spark-snowflake makes no attempt to enforce that - you'll just see Snowflake
     * errors if they don't match.
     *
     * Defaults to false.
     */
    @deprecated("Use SaveMode instead", "0.5.0")
    def overwrite: Boolean = parameters("overwrite").toBoolean

    /**
     * When true, data is always loaded into a new temporary table when performing an overwrite.
     * This is to ensure that the whole load process succeeds before dropping any data from
     * Snowflake, which can be useful if, in the event of failures, stale data is better than no data
     * for your systems.
     *
     * Defaults to true.
     */
    def useStagingTable: Boolean = parameters("usestagingtable").toBoolean

    /**
     * Extra options to append to the Snowflake COPY command (e.g. "MAXERROR 100").
     */
    def extraCopyOptions: String = parameters.get("extracopyoptions").getOrElse("")

    /**
     * List of semi-colon separated SQL statements to run before write operations.
     * This can be useful for running DELETE operations to clean up data
     *
     * If the action string contains %s, the table name will be substituted in, in case a staging
     * table is being used.
     *
     * Defaults to empty.
     */
    def preActions: Array[String] = parameters("preactions").split(";")

    /**
     * List of semi-colon separated SQL statements to run after successful write operations.
     * This can be useful for running GRANT operations to make your new tables readable to other
     * users and groups.
     *
     * If the action string contains %s, the table name will be substituted in, in case a staging
     * table is being used.
     *
     * Defaults to empty.
     */
    def postActions: Array[String] = parameters("postactions").split(";")

    /**
     * Temporary AWS credentials which are passed to Snowflake. These only need to be supplied by
     * the user when Hadoop is configured to authenticate to S3 via IAM roles assigned to EC2
     * instances.
     */
    def temporaryAWSCredentials: Option[AWSCredentials] = {
      for (
        accessKey <- parameters.get(PARAM_TEMP_KEY_ID);
        secretAccessKey <- parameters.get(PARAM_TEMP_KEY_SECRET);
        sessionToken <- parameters.get(PARAM_TEMP_SESSION_TOKEN)
      ) yield new BasicSessionCredentials(accessKey, secretAccessKey, sessionToken)
    }
  }
}
