/*
 * Copyright 2015-2018 Snowflake Computing
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

import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, PrivateKey}
import java.util.Properties

import net.snowflake.client.jdbc.internal.amazonaws.auth.{AWSCredentials, BasicSessionCredentials}
import net.snowflake.client.jdbc.internal.microsoft.azure.storage.StorageCredentialsSharedAccessSignature
import net.snowflake.spark.snowflake.FSType.FSType
import org.apache.commons.codec.binary.Base64
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
  * All user-specifiable parameters for spark-snowflake, along with their validation rules and
  * defaults.
  */
object Parameters {

  val log = new LoggerWithTelemetry(LoggerFactory.getLogger(getClass))

  private[snowflake] val KNOWN_PARAMETERS =
    new scala.collection.mutable.HashSet[String]()

  private def knownParam(param: String) = {
    KNOWN_PARAMETERS += param
    param
  }

  // List of known parameters
  val PARAM_S3_MAX_FILE_SIZE: String = knownParam("s3maxfilesize")
  val PARAM_SF_ACCOUNT: String = knownParam("sfaccount")
  val PARAM_SF_URL: String = knownParam("sfurl")
  val PARAM_SF_USER: String = knownParam("sfuser")
  val PARAM_SF_PASSWORD: String = knownParam("sfpassword")
  val PARAM_SF_DATABASE: String = knownParam("sfdatabase")
  val PARAM_SF_SCHEMA: String = knownParam("sfschema")
  val PARAM_SF_ROLE: String = knownParam("sfrole")
  val PARAM_SF_COMPRESS: String = knownParam("sfcompress")
  val PARAM_SF_SSL: String = knownParam("sfssl")
  val PARAM_TEMPDIR: String = knownParam("tempdir")
  val PARAM_SF_DBTABLE: String = knownParam("dbtable")
  val PARAM_SF_QUERY: String = knownParam("query")
  val PARAM_SF_TIMEZONE: String = knownParam("sftimezone")
  val PARAM_SF_WAREHOUSE: String = knownParam("sfwarehouse")
  val PARAM_TEMP_KEY_ID: String = knownParam("temporary_aws_access_key_id")
  val PARAM_TEMP_KEY_SECRET: String = knownParam(
    "temporary_aws_secret_access_key"
  )
  val PARAM_TEMP_SESSION_TOKEN: String = knownParam(
    "temporary_aws_session_token"
  )
  val PARAM_CHECK_BUCKET_CONFIGURATION: String = knownParam(
    "check_bucket_configuration"
  )
  val PARAM_TEMP_SAS_TOKEN: String = knownParam("temporary_azure_sas_token")
  val PARAM_PARALLELISM: String = knownParam("parallelism")
  val PARAM_PREACTIONS: String = knownParam("preactions")
  val PARAM_POSTACTIONS: String = knownParam("postactions")
  val PARAM_AWS_SECRET_KEY: String = knownParam("awssecretkey")
  val PARAM_AWS_ACCESS_KEY: String = knownParam("awsaccesskey")
  val PARAM_USE_STAGING_TABLE: String = knownParam("usestagingtable")
  val PARAM_EXTRA_COPY_OPTIONS: String = knownParam("extracopyoptions")
  val PARAM_AUTO_PUSHDOWN: String = knownParam("autopushdown")
  val PARAM_COLUMN_MAP: String = knownParam("columnmap")
  val PARAM_TRUNCATE_COLUMNS: String = knownParam("truncate_columns")
  val PARAM_PURGE: String = knownParam("purge")

  val PARAM_TRUNCATE_TABLE: String = knownParam("truncate_table")
  val PARAM_CONTINUE_ON_ERROR: String = knownParam("continue_on_error")
  val PARAM_STREAMING_STAGE: String = knownParam("streaming_stage")
  val PARAM_PEM_PRIVATE_KEY: String = knownParam("pem_private_key")
  val PARAM_KEEP_COLUMN_CASE: String = knownParam("keep_column_case")

  val PARAM_COLUMN_MAPPING: String = knownParam("column_mapping")
  val PARAM_COLUMN_MISMATCH_BEHAVIOR: String = knownParam(
    "column_mismatch_behavior"
  )

  // Add Oauth params
  val PARAM_AUTHENTICATOR: String = knownParam("sfauthenticator")
  val PARAM_OAUTH_TOKEN: String = knownParam("sftoken")

  // Internal use only?
  val PARAM_BIND_VARIABLE: String = knownParam("bind_variable")

  // Force to use COPY UNLOAD when reading data from Snowflake
  val PARAM_USE_COPY_UNLOAD: String = knownParam("use_copy_unload")
  // Expected partition size in MB when SELECT is used when reading data from Snowflake
  val PARAM_EXPECTED_PARTITION_SIZE_IN_MB: String = knownParam(
    "partition_size_in_mb"
  )
  val PARAM_TIME_OUTPUT_FORMAT: String = knownParam("time_output_format")
  val PARAM_JDBC_QUERY_RESULT_FORMAT: String = knownParam(
    "jdbc_query_result_format"
  )

  // Proxy related info
  val PARAM_USE_PROXY: String = knownParam("use_proxy")
  val PARAM_PROXY_HOST: String = knownParam("proxy_host")
  val PARAM_PROXY_PORT: String = knownParam("proxy_port")
  val PARAM_PROXY_USER: String = knownParam("proxy_user")
  val PARAM_PROXY_PASSWORD: String = knownParam("proxy_password")
  val PARAM_NON_PROXY_HOSTS: String = knownParam("non_proxy_hosts")

  val PARAM_EXPECTED_PARTITION_COUNT: String = knownParam(
    "expected_partition_count"
  )
  val PARAM_MAX_RETRY_COUNT: String = knownParam("max_retry_count")
  // When a spark task fails because of a throttling issue,
  // the task should use exponential backoff for next retry.
  val PARAM_USE_EXPONENTIAL_BACKOFF: String = knownParam(
    "use_exponential_backoff"
  )
  // Internal option to remove (") in stage table name.
  // This option may be removed without any notice in any time.
  val PARAM_INTERNAL_STAGING_TABLE_NAME_REMOVE_QUOTES_ONLY: String = knownParam(
    "internal_staging_table_name_remove_quotes_only"
  )

  val DEFAULT_S3_MAX_FILE_SIZE: String = (10 * 1000 * 1000).toString
  val MIN_S3_MAX_FILE_SIZE = 1000000

  val TZ_SPARK1 = ""
  val TZ_SPARK2 = "spark"
  val TZ_SF1 = "snowflake"
  val TZ_SF2 = "sf_current"
  val TZ_SF_DEFAULT = "sf_default"

  // List of values that mean "yes" when considered to be Boolean
  val BOOLEAN_VALUES_TRUE: Set[String] =
    Set("on", "yes", "true", "1", "enabled")
  val BOOLEAN_VALUES_FALSE: Set[String] =
    Set("off", "no", "false", "0", "disabled")

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
    "diststyle" -> "EVEN",
    PARAM_USE_STAGING_TABLE -> "true",
    PARAM_CONTINUE_ON_ERROR -> "off",
    PARAM_TRUNCATE_TABLE -> "off",
    PARAM_PREACTIONS -> "",
    PARAM_POSTACTIONS -> "",
    PARAM_AUTO_PUSHDOWN -> "on",
    PARAM_SF_SSL -> "on",
    PARAM_KEEP_COLUMN_CASE -> "off",
    PARAM_BIND_VARIABLE -> "on",
    PARAM_COLUMN_MAPPING -> "order",
    PARAM_COLUMN_MISMATCH_BEHAVIOR -> "error",
    PARAM_EXPECTED_PARTITION_SIZE_IN_MB -> "100",
    PARAM_USE_COPY_UNLOAD -> "false",
    PARAM_USE_PROXY -> "false",
    PARAM_EXPECTED_PARTITION_COUNT -> "1000",
    PARAM_MAX_RETRY_COUNT -> "10",
    PARAM_USE_EXPONENTIAL_BACKOFF -> "off"
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
        "Use of an external S3/Azure storage for staging is deprecated" +
          " and will be removed in a future version. " +
          "Unset your 'tempDir' parameter to use the Snowflake internal stage instead."
      )
    }
    // Snowflake-todo Add more parameter checking
    if (!userParameters.contains(PARAM_SF_URL)) {
      throw new IllegalArgumentException(
        "A snowflake URL must be provided with '" + PARAM_SF_URL +
          "' parameter, e.g. 'accountname.snowflakecomputing.com:443'"
      )
    }
    if (!userParameters.contains(PARAM_SF_USER)) {
      throw new IllegalArgumentException(
        "A snowflake user must be provided with '" + PARAM_SF_USER + "' parameter, e.g. 'user1'"
      )
    }
    val tokenVal = userParameters.get(PARAM_OAUTH_TOKEN)
    if ((!userParameters.contains(PARAM_SF_PASSWORD)) &&
      (!userParameters.contains(PARAM_PEM_PRIVATE_KEY)) &&
      //  if OAuth token not provided
      (tokenVal.isEmpty)) {
      throw new IllegalArgumentException(
        "A snowflake password or private key path or OAuth token must be provided with '" +
          PARAM_SF_PASSWORD + " or " + PARAM_PEM_PRIVATE_KEY + "' or '" +
          PARAM_OAUTH_TOKEN + "' parameter, e.g. 'password'"
      )
    }
    //  ensure OAuth token  provided if PARAM_AUTHENTICATOR = OAuth
    val authenticatorVal = userParameters.get(PARAM_AUTHENTICATOR)
    if ((authenticatorVal.contains("oauth")) &&
      (tokenVal.isEmpty)) {
      throw new IllegalArgumentException(
        "An OAuth token is required if the authenticator mode is '" +
          PARAM_AUTHENTICATOR + "'"
      )
    }
    //  PARAM_AUTHENTICATOR must be OAuth if OAuth token is specified
    if (!(authenticatorVal.contains("oauth")) &&
      (!tokenVal.isEmpty)) {
      throw new IllegalArgumentException(
        "Invalid authenticator mode passed '" + PARAM_AUTHENTICATOR +
          ", the authentication mode must be 'oauth' when specifying OAuth token"
      )
    }
    if (!userParameters.contains(PARAM_SF_DBTABLE) && !userParameters.contains(
          PARAM_SF_QUERY
        )) {
      throw new IllegalArgumentException(
        "You must specify a Snowflake table name with the '" + PARAM_SF_DBTABLE +
          "' parameter or a query with the '" + PARAM_SF_QUERY + "' parameter."
      )
    }
    if (userParameters.contains(PARAM_SF_DBTABLE) && userParameters.contains(
          PARAM_SF_QUERY
        )) {
      throw new IllegalArgumentException(
        "You cannot specify both the '" + PARAM_SF_DBTABLE + "' and '" + PARAM_SF_QUERY +
          "' parameters at the same time."
      )
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
          case _: Exception => None
        }
      }
      val s3maxfilesize = toInt(s3maxfilesizeStr.get)
      if (s3maxfilesize.isEmpty) {
        throw new IllegalArgumentException(
          s"Cannot parse $PARAM_S3_MAX_FILE_SIZE=${s3maxfilesizeStr.get} as a number"
        )
      }
      if (s3maxfilesize.get < MIN_S3_MAX_FILE_SIZE) {
        throw new IllegalArgumentException(
          s"Specified $PARAM_S3_MAX_FILE_SIZE=${s3maxfilesizeStr.get} too small"
        )
      }
    }

    MergedParameters(DEFAULT_PARAMETERS ++ userParameters)
  }

  /**
    * Adds validators and accessors to string map
    */
  case class MergedParameters(parameters: Map[String, String]) {

    private var generatedColumnMap: Option[Map[String, String]] = None

    override def toString: String = {
      "Snowflake Data Source"
    }

    // Simple conversion from string to an object of type
    // String, Boolean or Integer, depending on the value
    private def stringToObject(s: String): Object = {
      try {
        return new Integer(s)
      } catch {
        case _: Throwable =>
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
      var res = Map[String, Object]()
      unknownParamNames.foreach(
        v => res += (v -> stringToObject(parameters(v)))
      )
      res
    }

    lazy val usingExternalStage: Boolean = !rootTempDir.isEmpty

    lazy val rootTempDirStorageType: FSType = {
      val tempDir = parameters.getOrElse(PARAM_TEMPDIR, "")

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
          throw new SnowflakeConnectorException(
            "Parameter 'tempDir' must have a " +
              "file, wasb, wasbs, s3a, or s3n schema."
          )
        }
      }
    }

    /**
      * Number of threads used for PUT/GET.
      */
    lazy val parallelism: Option[Int] =
      parameters.get(PARAM_PARALLELISM).map(p => p.toInt)

    /**
      * A root directory to be used for intermediate data exchange,
      * expected to be on cloud storage (S3 or Azure storage), or somewhere
      * that can be written to and read from by Snowflake.
      * Make sure that credentials are available for this cloud provider.
      */
    lazy val rootTempDir: String = {
      rootTempDirStorageType
      parameters.getOrElse(PARAM_TEMPDIR, "")
    }

    lazy val proxyInfo: Option[ProxyInfo] = {
      if (this.useProxy) {
        Some(
          new ProxyInfo(
            proxyHost,
            proxyPort,
            proxyUser,
            proxyPassword,
            nonProxyHosts
          )
        )
      } else {
        None
      }
    }

    /**
      * Utility function to set proxy properties for JDBC if necessary
      */
    private[snowflake] def setJDBCProxyIfNecessary(jdbcProperties: Properties)
    : Unit = {
      proxyInfo match {
        case Some(proxyInfoValue) =>
          proxyInfoValue.setProxyForJDBC(jdbcProperties)
        case None =>
      }
    }

    /**
      * Whether or not to have TRUNCATE_COLUMNS in the COPY statement
      * generated by the Spark connector.
      */
    def truncateColumns(): Boolean = {
      isTrue(parameters.getOrElse(PARAM_TRUNCATE_COLUMNS, "off"))
    }

    /**
      * Whether or not to have PURGE in the COPY statement generated
      * by the Spark connector
      * @return
      */
    def purge(): Boolean = {
      isTrue(parameters.getOrElse(PARAM_PURGE, "off"))
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
        if (str.charAt(str.length - 1) == ';') {
          str = str.substring(0, str.length - 1)
        }
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
    def sfURL: String = {
      val origUrl = parameters(PARAM_SF_URL)
      val urlPattern = """(https?://)?(.*)""".r
      origUrl match {
        case urlPattern(_, url) => url
        case _ => throw new IllegalArgumentException(
          s"Input sfURL is invalid: $origUrl"
        )
      }
    }

    /**
      * URL pointing to the snowflake database including protocol.
      * for example, https://host:port
      */
    def sfFullURL: String = s"${if (isSslON) "https://" else "http://"}$sfURL"

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
    def autoPushdown: Boolean =
      isTrue(parameters.getOrElse(PARAM_AUTO_PUSHDOWN, "on"))

    /**
      * Snowflake role - optional
      */
    def sfRole: Option[String] = parameters.get(PARAM_SF_ROLE)

    /**
      * Snowflake timezone- optional
      */
    def sfTimezone: Option[String] = parameters.get(PARAM_SF_TIMEZONE)

    /**
      * Proxy related parameters.
      */
    def useProxy: Boolean = {
      isTrue(parameters.getOrElse(PARAM_USE_PROXY, "false"))
    }
    def proxyHost: Option[String] = {
      parameters.get(PARAM_PROXY_HOST)
    }
    def proxyPort: Option[String] = {
      parameters.get(PARAM_PROXY_PORT)
    }
    def proxyUser: Option[String] = {
      parameters.get(PARAM_PROXY_USER)
    }
    def proxyPassword: Option[String] = {
      parameters.get(PARAM_PROXY_PASSWORD)
    }
    def nonProxyHosts: Option[String] = {
      parameters.get(PARAM_NON_PROXY_HOSTS)
    }

    /**
      *  Mapping OAuth and authenticator values
      */
    def sfAuthenticator: Option[String] = parameters.get(PARAM_AUTHENTICATOR)
    def sfToken: Option[String] = parameters.get(PARAM_OAUTH_TOKEN)

    def expectedPartitionCount: Int = {
      parameters.getOrElse(PARAM_EXPECTED_PARTITION_COUNT, "1000").toInt
    }

    def maxRetryCount: Int = {
      parameters.getOrElse(PARAM_MAX_RETRY_COUNT, "10").toInt
    }
    def useExponentialBackoff: Boolean = {
      isTrue(parameters.getOrElse(PARAM_USE_EXPONENTIAL_BACKOFF, "false"))
    }
    def stagingTableNameRemoveQuotesOnly: Boolean = {
      isTrue(parameters.getOrElse(PARAM_INTERNAL_STAGING_TABLE_NAME_REMOVE_QUOTES_ONLY, "false"))
    }

    /**
      * set column map
      */
    def setColumnMap(fromSchema: Option[StructType],
                     toSchema: Option[StructType]): Unit = {
      if (parameters.get(PARAM_COLUMN_MAP).isDefined) {
        throw new Exception("Column map is already declared")
      }
      generatedColumnMap =
        if (columnMapping == "name" && fromSchema.isDefined && toSchema.isDefined) {
          val map = Utils.generateColumnMap(
            fromSchema.get,
            toSchema.get,
            columnMismatchBehavior.equals("error")
          )
          if (map.isEmpty) throw new UnsupportedOperationException(s"""
             |No column name matched between Snowflake Table and Spark Dataframe.
             |Please check the column names or manually assign the ColumnMap
         """.stripMargin)
          Some(map)
        } else None
    }

    /**
      * Retrieve Column mapping data.
      * None if empty
      */
    def columnMap: Option[Map[String, String]] = {
      parameters.get(PARAM_COLUMN_MAP) match {
        case None => generatedColumnMap
        case Some(source: String) => Some(Utils.parseMap(source))
      }
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
      * Snowflake, which can be useful if, in the event of failures, stale data is better than
      * no data for your systems.
      *
      * Defaults to true.
      */
    def useStagingTable: Boolean =
      isTrue(parameters(PARAM_USE_STAGING_TABLE))

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
      for (accessKey <- parameters.get(PARAM_TEMP_KEY_ID);
           secretAccessKey <- parameters.get(PARAM_TEMP_KEY_SECRET);
           sessionToken <- parameters.get(PARAM_TEMP_SESSION_TOKEN))
        yield
          new BasicSessionCredentials(accessKey, secretAccessKey, sessionToken)
    }

    /**
      * SAS Token to be passed to Snowflake to access data in Azure storage.
      * We currently don't support full storage account key so this has to be
      * provided if customer would like to load data through their storage
      * account directly.
      */
    def temporaryAzureStorageCredentials
      : Option[StorageCredentialsSharedAccessSignature] = {
      for (sas <- parameters.get(PARAM_TEMP_SAS_TOKEN))
        yield new StorageCredentialsSharedAccessSignature(sas)
    }

    /**
      * Truncate table when overwriting.
      * Keep the table schema
      */
    def truncateTable: Boolean = isTrue(parameters(PARAM_TRUNCATE_TABLE))

    /**
      * Set on_error parameter to continue in COPY command
      * todo: create data validation function in spark side instead of using COPY COMMAND
      */
    def continueOnError: Boolean = isTrue(parameters(PARAM_CONTINUE_ON_ERROR))

    def azureSAS: Option[String] = parameters.get(PARAM_TEMP_SAS_TOKEN)

    def awsAccessKey: Option[String] = parameters.get(PARAM_AWS_ACCESS_KEY)

    def awsSecretKey: Option[String] = parameters.get(PARAM_AWS_SECRET_KEY)

    def isSslON: Boolean = isTrue(sfSSL)

    def keepOriginalColumnNameCase: Boolean =
      isTrue(parameters(PARAM_KEEP_COLUMN_CASE))

    def bindVariableEnabled: Boolean = isTrue(parameters(PARAM_BIND_VARIABLE))

    def useCopyUnload: Boolean = isTrue(parameters(PARAM_USE_COPY_UNLOAD))

    def expectedPartitionSize: Long = {
      try {
        (parameters(PARAM_EXPECTED_PARTITION_SIZE_IN_MB).toDouble * 1024 * 1024).toLong
      } catch {
        case _: Exception =>
          throw new IllegalArgumentException(
            "Input expected partition size is invalid"
          )
      }
    }

    /**
      * Snowflake time output format
      */
    def getTimeOutputFormat: Option[String] = {
      parameters.get(PARAM_TIME_OUTPUT_FORMAT)
    }

    /**
      * Snowflake query result format
      */
    def getQueryResultFormat: Option[String] = {
      parameters.get(PARAM_JDBC_QUERY_RESULT_FORMAT)
    }

    /**
      * Generate private key form pem key value
      * @return private key object
      */
    def privateKey: Option[PrivateKey] =
      parameters
        .get(PARAM_PEM_PRIVATE_KEY)
        .map(key => {
          // scalastyle:off
          java.security.Security.addProvider(
            new net.snowflake.client.jdbc.internal.org.bouncycastle.jce.provider.BouncyCastleProvider
          )
          // scalastyle:on
          try {
            val encoded = Base64.decodeBase64(key)
            val kf = KeyFactory.getInstance("RSA")
            val keySpec = new PKCS8EncodedKeySpec(encoded)
            kf.generatePrivate(keySpec)
          } catch {
            case _: Exception =>
              throw new IllegalArgumentException(
                "Input PEM private key is invalid"
              )
          }

        })

    def columnMapping: String = {
      parameters(PARAM_COLUMN_MAPPING).toLowerCase match {
        case "name"  => "name"
        case "order" => "order"
        case value =>
          log.error(s"""
              |wrong input value of column_mapping parameter: $value,
              |it only supports "name" and "order",
              |using default setting "order"
              |""".stripMargin)
          "order"
      }
    }

    def columnMismatchBehavior: String = {
      parameters(PARAM_COLUMN_MISMATCH_BEHAVIOR).toLowerCase() match {
        case "error"  => "error"
        case "ignore" => "ignore"
        case value =>
          log.error(s"""
               |wrong input values of column_mismatch_behavior parameter: $value
               |it only supports "error" and "ignore",
               |using default setting "error"
               """.stripMargin)
          "ignore"
      }
    }

    def streamingStage: Option[String] = parameters.get(PARAM_STREAMING_STAGE)

    def storagePath: Option[String] = {
      val azure_url = "wasbs?://([^@]+)@([^.]+)\\.([^/]+)/(.*)".r
      val s3_url = "s3[an]://([^/]+)/(.*)".r
      parameters.get(PARAM_TEMPDIR) match {
        case Some(azure_url(container, account, endpoint, path)) =>
          Some(s"azure://$account.$endpoint/$container/$path")
        case Some(s3_url(bucket, prefix)) =>
          Some(s"s3://$bucket/$prefix")
        case None => None
        case Some(str) =>
          throw new UnsupportedOperationException(
            s"Only Support azure or s3 storage: $str"
          )
      }
    }
  }
}

object FSType extends Enumeration {
  type FSType = Value
  val LocalFile, S3, Azure, Unknown = Value
}
