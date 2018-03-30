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

package net.snowflake.spark.snowflake

import com.amazonaws.auth.{AWSCredentials, BasicSessionCredentials}
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature
import net.snowflake.spark.snowflake.FSType.FSType
import org.slf4j.LoggerFactory

/**
  * All user-specifiable parameters for spark-snowflake, along with their validation rules and
  * defaults.
  */
object Parameters {

  val log = LoggerFactory.getLogger(getClass)

  private[snowflake] val KNOWN_PARAMETERS = new scala.collection.mutable.HashSet[String]()

  private def knownParam(param: String): String = {
    KNOWN_PARAMETERS += param
    param
  }

  // List of known parameters
  val PARAM_S3_MAX_FILE_SIZE   = knownParam("s3maxfilesize")
  val PARAM_SF_ACCOUNT         = knownParam("sfaccount")
  val PARAM_SF_URL             = knownParam("sfurl")
  val PARAM_SF_USER            = knownParam("sfuser")
  val PARAM_SF_PASSWORD        = knownParam("sfpassword")
  val PARAM_SF_DATABASE        = knownParam("sfdatabase")
  val PARAM_SF_SCHEMA          = knownParam("sfschema")
  val PARAM_SF_ROLE            = knownParam("sfrole")
  val PARAM_SF_COMPRESS        = knownParam("sfcompress")
  val PARAM_SF_SSL             = knownParam("sfssl")
  val PARAM_TEMPDIR            = knownParam("tempdir")
  val PARAM_SF_DBTABLE         = knownParam("dbtable")
  val PARAM_SF_QUERY           = knownParam("query")
  val PARAM_SF_TIMEZONE        = knownParam("sftimezone")
  val PARAM_SF_WAREHOUSE       = knownParam("sfwarehouse")
  val PARAM_JDBC_DRIVER        = knownParam("jdbcdriver")
  val PARAM_TEMP_KEY_ID        = knownParam("temporary_aws_access_key_id")
  val PARAM_TEMP_KEY_SECRET    = knownParam("temporary_aws_secret_access_key")
  val PARAM_TEMP_SESSION_TOKEN = knownParam("temporary_aws_session_token")
  val PARAM_CHECK_BUCKET_CONFIGURATION = knownParam(
    "check_bucket_configuration")
  val PARAM_TEMP_SAS_TOKEN     = knownParam("temporary_azure_sas_token")
  val PARAM_PARALLELISM        = knownParam("parallelism")
  val PARAM_PREACTIONS         = knownParam("preactions")
  val PARAM_POSTACTIONS        = knownParam("postactions")
  val PARAM_AWS_SECRET_KEY     = knownParam("awssecretkey")
  val PARAM_AWS_ACCESS_KEY     = knownParam("awsaccesskey")
  val PARAM_USE_STAGING_TABLE  = knownParam("usestagingtable")
  val PARAM_EXTRA_COPY_OPTIONS = knownParam("extracopyoptions")
  val PARAM_AUTO_PUSHDOWN      = knownParam("autopushdown")
  val PARAM_COLUMN_MAP         = knownParam("columnmap")



  val DEFAULT_S3_MAX_FILE_SIZE = (10 * 1000 * 1000).toString
  val MIN_S3_MAX_FILE_SIZE     = 1000000

  val TZ_SPARK1     = ""
  val TZ_SPARK2     = "spark"
  val TZ_SF1        = "snowflake"
  val TZ_SF2        = "sf_current"
  val TZ_SF_DEFAULT = "sf_default"

  // List of values that mean "yes" when considered to be Boolean
  // scalastyle:off
  val BOOLEAN_VALUES_TRUE  = Set("on", "yes", "true", "1", "enabled")
  val BOOLEAN_VALUES_FALSE = Set("off", "no", "false", "0", "disabled")
  // scalastyle: on

  /**
    * Helper method to check if a given string represents some form
    * of "true" value, see BOOLEAN_VALUES_TRUE
    */
  def isTrue(string: String): Boolean = {
    BOOLEAN_VALUES_TRUE contains string.toLowerCase
  }

  val DEFAULT_PARAMETERS: Map[String, String] = Map(
    // Notes:
    // * tempdir, dbtable and url have no default and they *must* be provided
    "diststyle"       -> "EVEN",
    "usestagingtable" -> "true",
    PARAM_PREACTIONS  -> "",
    PARAM_POSTACTIONS -> "",
    PARAM_AUTO_PUSHDOWN -> "on"

  )

  /**
    * Merge user parameters with the defaults, preferring user parameters if specified
    */
  def mergeParameters(params: Map[String, String]): MergedParameters = {

    val userParameters = params.map {
      case (key, value) => (key.toLowerCase, value)
    }

    if (userParameters.contains(PARAM_TEMPDIR)) {
      log.warn(
        "Use of an external S3 bucket for staging is deprecated and will be removed in a future version. " +
          "Unset your 'tempDir' parameter to use the Snowflake internal stage instead.")
    }
    // Snowflake-todo Add more parameter checking
    if (!userParameters.contains(PARAM_SF_URL)) {
      throw new IllegalArgumentException(
        "A snowflake URL must be provided with '" + PARAM_SF_URL + "' parameter, e.g. 'accountname.snowflakecomputing.com:443'")
    }
    if (!userParameters.contains(PARAM_SF_USER)) {
      throw new IllegalArgumentException(
        "A snowflake user must be provided with '" + PARAM_SF_USER + "' parameter, e.g. 'user1'")
    }
    if (!userParameters.contains(PARAM_SF_PASSWORD)) {
      throw new IllegalArgumentException(
        "A snowflake passsword must be provided with '" + PARAM_SF_PASSWORD + "' parameter, e.g. 'password'")
    }
    if (!userParameters.contains(PARAM_SF_DBTABLE) && !userParameters.contains(
          PARAM_SF_QUERY)) {
      throw new IllegalArgumentException(
        "You must specify a Snowflake table name with the '" + PARAM_SF_DBTABLE + "' parameter or a query" + " with the " +
          "'" + PARAM_SF_QUERY + "' parameter.")
    }
    if (userParameters.contains(PARAM_SF_DBTABLE) && userParameters.contains(
          PARAM_SF_QUERY)) {
      throw new IllegalArgumentException(
        "You cannot specify both the '" + PARAM_SF_DBTABLE + "' and '" + PARAM_SF_QUERY + "' parameters at the same time.")
    }

    // Check temp keys
    var tempParams = 0
    if (userParameters.contains(PARAM_TEMP_KEY_ID)) tempParams += 1
    if (userParameters.contains(PARAM_TEMP_KEY_SECRET)) tempParams += 1
    if (userParameters.contains(PARAM_TEMP_SESSION_TOKEN)) tempParams += 1
    if (tempParams != 0 && tempParams != 3) {
      throw new IllegalArgumentException(s"""If you specify one of
           |$PARAM_TEMP_KEY_ID, $PARAM_TEMP_KEY_SECRET and
           |$PARAM_TEMP_SESSION_TOKEN,
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
      if (s3maxfilesize.isEmpty) {
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

    override def toString: String = {
      "Snowflake Data Source"
    }

    // Simple conversion from string to an object of type
    // String, Boolean or Integer, depending on the value
    private def stringToObject(s: String): Object = {
      try {
        return new Integer(s)
      } catch {
        case t: Throwable =>
      }
      if (s.equalsIgnoreCase("true")) {
        return new java.lang.Boolean(true)
      }
      if (s.equalsIgnoreCase("false")) {
        return new java.lang.Boolean(false)
      }
      s
    }

    private lazy val extraParams: Map[String, Object] = {
      // Find all entries that are in parameters and are not known
      val unknownParamNames = parameters.keySet -- KNOWN_PARAMETERS
      var res               = Map[String, Object]()
      unknownParamNames.foreach(v =>
        res += (v -> stringToObject(parameters(v))))
      res
    }

    lazy val usingExternalStage: Boolean = !rootTempDir.isEmpty

    lazy val rootTempDirStorageType: FSType = {
      val tempDir = Option(parameters.getOrElse(PARAM_TEMPDIR, "")).
        getOrElse("")

      if (tempDir.isEmpty) {
        FSType.Unknown
      } else {
        val lsTempDir = tempDir.toLowerCase

        if (lsTempDir.startsWith("file://")) {
          FSType.LocalFile
        } else if (lsTempDir.startsWith("wasb://") ||
                   lsTempDir.startsWith("wasbs://")) {
          FSType.Azure
        } else if (lsTempDir.startsWith("s3")) {
          FSType.S3
        } else {
          throw new SnowflakeConnectorException("Parameter 'tempDir' must have a " +
            "file, wasb, wasbs, s3a, or s3n schema.")
        }
      }
    }

    /**
      * Number of threads used for PUT/GET.
      */
    lazy val parallelism: Option[Int] = parameters.get(PARAM_PARALLELISM).map(p => p.toInt)

    /**
      * A root directory to be used for intermediate data exchange,
      * expected to be on cloud storage (S3 or Azure storage), or somewhere
      * that can be written to and read from by Snowflake.
      * Make sure that credentials are available for this cloud provider.
      */
    lazy val rootTempDir: String = {
      rootTempDirStorageType
      Option(parameters.getOrElse(PARAM_TEMPDIR, "")).getOrElse("")
    }

    /**
      * Creates a per-query subdirectory in the [[rootTempDir]], with a random UUID.
      */
    def createPerQueryTempDir(): String = Utils.makeTempPath(rootTempDir)

    /**
      * The Snowflake table to be used as the target when loading or writing data.
      */
    def table: Option[TableName] =
      parameters.get(PARAM_SF_DBTABLE).map(_.trim).flatMap { dbtable =>
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
      val res = parameters.get(PARAM_SF_QUERY)
      if (res.isDefined) {
        // Remove trailing semicolon if present
        var str: String = res.get.trim
        if (str.charAt(str.length - 1) == ';')
          str = str.substring(0, str.length - 1)
        Some(str)
      } else {
        parameters
          .get(PARAM_SF_DBTABLE)
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
    def sfURL: String = parameters(PARAM_SF_URL)

    /**
      * Snowflake database name
      */
    def sfDatabase: String = parameters(PARAM_SF_DATABASE)

    /**
      * Snowflake schema
      */
    def sfSchema: String = parameters.getOrElse(PARAM_SF_SCHEMA, "public")

    /**
      * Snowflake warehouse
      */
    def sfWarehouse: Option[String] = {
      parameters.get(PARAM_SF_WAREHOUSE)
    }

    /**
      * Snowflake user
      */
    def sfUser: String = parameters(PARAM_SF_USER)

    /**
      * Snowflake password
      */
    def sfPassword: String = parameters(PARAM_SF_PASSWORD)

    /**
      * Snowflake account - optional
      */
    def sfAccount: Option[String] = {
      parameters.get(PARAM_SF_ACCOUNT)
    }

    /**
      * Snowflake SSL on/off - "on" by default
      */
    def sfSSL: String = parameters.getOrElse(PARAM_SF_SSL, "on")

    /**
      * Snowflake use compression on/off - "on" by default
      */
    def sfCompress: Boolean =
      isTrue(parameters.getOrElse(PARAM_SF_COMPRESS, "on"))

    /**
      * Snowflake automatically enable/disable pushdown function
      */
    def autoPushdown: Boolean = isTrue(parameters.getOrElse(PARAM_AUTO_PUSHDOWN, "on"))


    /**
      * Snowflake role - optional
      */
    def sfRole: Option[String] = parameters.get(PARAM_SF_ROLE)

    /**
      * Snowflake timezone- optional
      */
    def sfTimezone: Option[String] = parameters.get(PARAM_SF_TIMEZONE)


    /**
      * Retrieve Column mapping data.
      * None if empty
      */
    def columnMap: Option[Map[String,String]] = {
      if(parameters.get(PARAM_COLUMN_MAP).isDefined && parameters.get(PARAM_TEMPDIR).isDefined){
        throw new IllegalArgumentException("Column Mapping function only supports internal stage")
      }
      parameters.get(PARAM_COLUMN_MAP) match {
        case None => None
        case Some(source: String) => Some(Utils.parseMap(source))
      }
    }


    /**
      * The JDBC driver class name. This is used to make sure the driver is registered before
      * connecting over JDBC.
      */
    def jdbcDriver: Option[String] = {
      parameters.get(PARAM_JDBC_DRIVER)
    }

    /**
      * Returns a map of options that are not known to the connector,
      * and are passed verbosely to the JDBC driver
      */
    def sfExtraOptions: Map[String, Object] = extraParams

    /** Returns true if bucket lifecycle configuration should be checked */
    def checkBucketConfiguration: Boolean =
      isTrue(parameters.getOrElse(PARAM_CHECK_BUCKET_CONFIGURATION, "off"))

    /**
      * Max file size used to move data out from Snowflake
      */
    def s3maxfilesize: String =
      parameters.getOrElse(PARAM_S3_MAX_FILE_SIZE, DEFAULT_S3_MAX_FILE_SIZE)

    def isTimezoneSpark: Boolean = {
      val tz = sfTimezone.getOrElse(TZ_SPARK1)
      (tz.equalsIgnoreCase(TZ_SPARK1)
      || tz.equalsIgnoreCase(TZ_SPARK2))
    }
    def isTimezoneSnowflake: Boolean = {
      val tz = sfTimezone.getOrElse("")
      (tz.equalsIgnoreCase(TZ_SF1)
      || tz.equalsIgnoreCase(TZ_SF2))
    }
    def isTimezoneSnowflakeDefault: Boolean = {
      val tz = sfTimezone.getOrElse("")
      tz.equalsIgnoreCase(TZ_SF_DEFAULT)
    }

    /**
      * -----------------------------------------Various other options
      */
    /**
      * When true, data is always loaded into a new temporary table when performing an overwrite.
      * This is to ensure that the whole load process succeeds before dropping any data from
      * Snowflake, which can be useful if, in the event of failures, stale data is better than no data
      * for your systems.
      *
      * Defaults to true.
      */
    def useStagingTable: Boolean =
      parameters(PARAM_USE_STAGING_TABLE).toBoolean

    /**
      * Extra options to append to the Snowflake COPY command (e.g. "MAXERROR 100").
      */
    def extraCopyOptions: String =
      parameters.getOrElse(PARAM_EXTRA_COPY_OPTIONS, "")

    /**
      * List of semi-colon separated SQL statements to run before write operations.
      * This can be useful for running DELETE operations to clean up data
      *
      * If the action string contains %s, the table name will be substituted in, in case a staging
      * table is being used.
      *
      * Defaults to empty.
      */
    def preActions: Array[String] = parameters(PARAM_PREACTIONS).split(";")

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
    def postActions: Array[String] = parameters(PARAM_POSTACTIONS).split(";")

    /**
      * Temporary AWS credentials which are passed to Snowflake. These only need to be supplied by
      * the user when Hadoop is configured to authenticate to S3 via IAM roles assigned to EC2
      * instances.
      */
    def temporaryAWSCredentials: Option[AWSCredentials] = {
      for (accessKey       <- parameters.get(PARAM_TEMP_KEY_ID);
           secretAccessKey <- parameters.get(PARAM_TEMP_KEY_SECRET);
           sessionToken    <- parameters.get(PARAM_TEMP_SESSION_TOKEN))
        yield
          new BasicSessionCredentials(accessKey, secretAccessKey, sessionToken)
    }

    /**
      * SAS Token to be passed to Snowflake to access data in Azure storage.
      * We currently don't support full storage account key so this has to be
      * provided if customer would like to load data through their storage
      * account directly.
      */
    def temporaryAzureStorageCredentials: Option[StorageCredentialsSharedAccessSignature] = {
      for (sas <- parameters.get(PARAM_TEMP_SAS_TOKEN))
        yield
          new StorageCredentialsSharedAccessSignature(sas)
    }
  }
}

object FSType extends Enumeration {
  type FSType = Value
  val LocalFile, S3, Azure, Unknown = Value
}