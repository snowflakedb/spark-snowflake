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

import java.net.URI
import java.sql.{Connection, ResultSet}
import java.util.{Properties, UUID}

import net.snowflake.client.jdbc.{SnowflakeDriver, SnowflakeResultSet, SnowflakeResultSetSerializable}
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.spark.{SPARK_VERSION, SparkContext, SparkEnv}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.postfixOps
import scala.util.control.NonFatal
import scala.io._
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.{AmazonS3Client, AmazonS3URI}
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.BucketLifecycleConfiguration
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.spark.snowflake.FSType.FSType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.LoggerFactory

/**
  * Various arbitrary helper functions
  */
object Utils {

  /**
    * Literal to be used with the Spark DataFrame's .format method
    */
  val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

  /**
    * Short literal name of SNOWFLAKE_SOURCE_NAME
    */
  val SNOWFLAKE_SOURCE_SHORT_NAME = "snowflake"

  val VERSION = "2.14.0"

  /**
    * The certified JDBC version to work with this spark connector version.
    */
  val CERTIFIED_JDBC_VERSION = "3.14.4"

  /**
    * Important:
    * Never change the value of PROPERTY_NAME_OF_CONNECTOR_VERSION.
    * Changing it will cause spark connector doesn't work in some cases.
    */
  val PROPERTY_NAME_OF_CONNECTOR_VERSION = "spark.snowflakedb.version"

  /**
    * Client info related variables
    */
  private[snowflake] lazy val sparkAppName = if (SparkEnv.get != null) {
    SparkEnv.get.conf.get("spark.app.name", "")
  } else {
    ""
  }
  private[snowflake] lazy val scalaVersion =
    util.Properties.versionNumberString
  private[snowflake] lazy val javaVersion =
    System.getProperty("java.version", "UNKNOWN")
  private[snowflake] lazy val jdbcVersion =
    SnowflakeDriver.implementVersion
  private val mapper = new ObjectMapper()

  private[snowflake] val JDBC_DRIVER =
    "net.snowflake.client.jdbc.SnowflakeDriver"

  private val log = LoggerFactory.getLogger(getClass)

  def classForName(className: String): Class[_] = {
    val classLoader =
      Option(Thread.currentThread().getContextClassLoader)
        .getOrElse(this.getClass.getClassLoader)
    // scalastyle:off
    Class.forName(className, true, classLoader)
    // scalastyle:on
  }

  /**
    * Joins prefix URL a to path suffix b, and appends a trailing /, in order to create
    * a temp directory path for S3.
    */
  def joinUrls(a: String, b: String): String = {
    a.stripSuffix("/") + "/" + b.stripPrefix("/").stripSuffix("/") + "/"
  }

  /**
    * Snowflake COPY and UNLOAD commands don't support s3n or s3a, but users may wish to use them
    * for data loads. This function converts the URL back to the s3:// format.
    */
  def fixS3Url(url: String): String = {
    url.replaceAll("s3[an]://", "s3://")
  }

  /**
    * Converts url for the copy command. For S3, convert s3a|s3n to s3. For
    * Azure, convert the wasb: url to azure: url.
    *
    * @param url the url to be used in hadoop/spark
    * @return the url to be used in Snowflake
    */
  def fixUrlForCopyCommand(url: String): String = {
    if (url.startsWith("wasb://") ||
        url.startsWith("wasbs://")) {
      // in spark, the azure storage path is defined as
      // wasb://CONTAINER@STORAGE_ACCOUNT.HOSTNAME/PATH
      // while in snowflake, the path will be
      // azure://STORAGE_ACCOUNT.HOSTNAME/CONTAINER/PATH

      val pathUri = URI.create(url)

      "azure://" + pathUri.getHost + "/" + pathUri.getUserInfo + pathUri.getPath
    } else {
      fixS3Url(url)
    }
  }

  /**
    * Returns a copy of the given URI with the user credentials removed.
    */
  def removeCredentialsFromURI(uri: URI): URI = {
    new URI(
      uri.getScheme,
      null, // no user info
      uri.getHost,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment
    )
  }

  /**
    * Creates a randomly named temp directory path for intermediate data
    */
  def makeTempPath(tempRoot: String): String =
    Utils.joinUrls(tempRoot, UUID.randomUUID().toString)

  /**
    * Checks whether the S3 bucket for the given UI has an object lifecycle configuration to
    * ensure cleanup of temporary files. If no applicable configuration is found, this method logs
    * a helpful warning for the user.
    */
  def checkThatBucketHasObjectLifecycleConfiguration(
    tempDir: String,
    tempDirStorageType: FSType,
    s3Client: AmazonS3Client
  ): Unit = {

    tempDirStorageType match {
      case FSType.S3 =>
        try {
          val s3URI = new AmazonS3URI(Utils.fixS3Url(tempDir))
          val bucket = s3URI.getBucket
          val bucketLifecycleConfiguration =
            s3Client.getBucketLifecycleConfiguration(bucket)
          val key = Option(s3URI.getKey).getOrElse("")
          val someRuleMatchesTempDir =
            bucketLifecycleConfiguration.getRules.asScala.exists { rule =>
              // Note: this only checks that there is an active rule which matches
              // the temp directory; it does not actually check that the rule will
              // delete the files. This check is still better than nothing, though,
              // and we can always improve it later.
              rule.getStatus == BucketLifecycleConfiguration.ENABLED && key
                .startsWith(rule.getPrefix)
            }
          if (!someRuleMatchesTempDir) {
            log.warn(
              s"The S3 bucket $bucket does not have an object lifecycle configuration to " +
                "ensure cleanup of temporary files. Consider configuring `tempdir` " +
                "to point to a bucket with an object lifecycle policy that automatically " +
                "deletes files after an expiration period. For more information, see " +
                "https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lifecycle-mgmt.html"
            )
          }
        } catch {
          case NonFatal(e) =>
            log.warn(
              "An error occurred while trying to read the S3 bucket lifecycle configuration",
              e
            )
        }
      case _ =>
    }
  }

  /**
    * Given a URI, verify that the Hadoop FileSystem for that URI is not the S3 block FileSystem.
    * `spark-snowflakedb` cannot use this FileSystem because the files written to it will not be
    * readable by Snowflake (and vice versa).
    */
  def checkFileSystem(uri: URI, hadoopConfig: Configuration): Unit = {
    val fs = FileSystem.get(uri, hadoopConfig)

    // Note that we do not want to use isInstanceOf here, since we're only interested in detecting
    // exact matches. We compare the class names as strings in order to avoid introducing a binary
    // dependency on classes which belong to the `hadoop-aws` JAR, as that artifact is not present
    // in some environments (such as EMR). See #92 for details.
    if (fs.getClass.getCanonicalName == "org.apache.hadoop.fs.s3.S3FileSystem") {
      throw new IllegalArgumentException(
        "spark-snowflakedb does not support the S3 Block FileSystem. " +
          "Please reconfigure `tempdir` to use a s3n:// or s3a:// scheme."
      )
    }
  }

  // Reads a Map from a file using the format
  //     key = value
  // Snowflake-todo Use some standard config library
  def readMapFromFile(sc: SparkContext, file: String): Map[String, String] = {
    val fs = FileSystem.get(URI.create(file), sc.hadoopConfiguration)
    val is = fs.open(Path.getPathWithoutSchemeAndAuthority(new Path(file)))
    val src = scala.io.Source.fromInputStream(is)

    mapFromSource(src)

  }

  /**
    * Same as readMapFromFile, but accepts the file content as an argument
    */
  def readMapFromString(string: String): Map[String, String] = {
    val src = scala.io.Source.fromString(string)

    mapFromSource(src)
  }

  private def mapFromSource(src: Source): Map[String, String] = {
    var map = new mutable.HashMap[String, String]
    for (line <- src.getLines()) {
      val index = line.indexOf('=')
      if (index < 1) {
        throw new Exception(
          s"Can't parse the line of $line. The index of '=' is $index")
      }
      val key = line.substring(0, index).trim.toLowerCase
      val value = line.substring(index + 1).trim
      if (!key.startsWith("#")) {
        map += (key -> value)
      }
    }
    map.toMap
  }

  private[snowflake] def getMergedParameters(params: Map[String, String])
  : MergedParameters = {
    val lcParams = params.map { case (key, value) => (key.toLowerCase, value) }
    MergedParameters(Parameters.DEFAULT_PARAMETERS ++ lcParams)
  }

  def getJDBCConnection(params: Map[String, String]): Connection = {
    val mergedParams = getMergedParameters(params)
    ServerConnection.getServerConnection(mergedParams, false).jdbcConnection
  }

  def getJDBCConnection(params: java.util.Map[String, String]): Connection = {
    val m2: Map[String, String] = params.asScala.toMap
    getJDBCConnection(m2)
  }

  // Stores the last generated Select query
  private var lastCopyUnload: String = _
  private var lastCopyLoad: String = _
  private var lastCopyLoadQueryId: String = _
  private var lastSelect: String = _
  private var lastSelectQueryId: String = _
  private var lastPutCommand: String = _
  private var lastGetCommand: String = _

  private[snowflake] def setLastCopyUnload(select: String): Unit = {
    lastCopyUnload = select
  }

  def getLastCopyUnload: String = {
    lastCopyUnload
  }

  private[snowflake] def setLastSelect(select: String): Unit = {
    lastSelect = select
  }

  def getLastSelect: String = lastSelect

  private[snowflake] def setLastSelectQueryId(queryId: String): Unit =
    lastSelectQueryId = queryId

  /** Get the query ID for the last query. */
  def getLastSelectQueryId: String = lastSelectQueryId

  private[snowflake] def setLastCopyLoad(select: String): Unit = {
    lastCopyLoad = select
  }

  def getLastCopyLoad: String = {
    lastCopyLoad
  }

  private[snowflake] def setLastCopyLoadQueryId(queryId: String): Unit =
    lastCopyLoadQueryId = queryId

  /** Get the query ID for the last Copy Into Table command. */
  def getLastCopyLoadQueryId: String = lastCopyLoadQueryId

  private[snowflake] def setLastPutCommand(set: String): Unit = {
    lastPutCommand = set
  }

  def getLastPutCommand: String = {
    lastPutCommand
  }

  private[snowflake] def setLastGetCommand(get: String): Unit = {
    lastGetCommand = get
  }

  def getLastGetCommand: String = {
    lastGetCommand
  }

  // Issues a set of configuration changes at the beginning of the session.
  // Note, we are changing session parameters to have the exact
  // date/timestamp formats we want.
  // Since we want to distinguish TIMESTAMP_NTZ from others, we can't use
  // the TIMESTAMP_FORMAT option of COPY.
  private[snowflake] def genPrologueSql(
    params: MergedParameters
  ): Option[SnowflakeSQLStatement] = {
    // Determine the timezone we want to use
    val tz = params.sfTimezone
    var timezoneSetString = ""
    if (params.isTimezoneSpark) {
      // Use the Spark-level one
      val tzStr = java.util.TimeZone.getDefault.getID
      timezoneSetString = s"timezone = '$tzStr'"
    } else if (params.isTimezoneSnowflake) {
      // Do nothing
    } else if (params.isTimezoneSnowflakeDefault) {
      // Set it to the Snowflake default
      timezoneSetString = "timezone = default"
    } else {
      // Otherwise, use the specified value
      timezoneSetString = s"timezone = '${tz.get}'"
    }
    log.debug(s"sfTimezone: '$tz'   timezoneSetString '$timezoneSetString'")

    val timestampFormats = Seq(
      ("timestamp_ntz_output_format", params.sfTimestampNTZOutputFormat.get),
      ("timestamp_ltz_output_format", params.sfTimestampLTZOutputFormat.get),
      ("timestamp_tz_output_format", params.sfTimestampTZOutputFormat.get))
    val timestampSettings = timestampFormats.filter(x => !params.isTimestampSnowflake(x._2))
    val timestampSetString = timestampSettings.map(x => s"${x._1} = '${x._2}'").mkString(", ")
    if (timezoneSetString.isEmpty && timestampSetString.isEmpty) {
      log.info("Timezone and timestamp output formats are sf_current, so skip setting them.")
      None
    } else if (timezoneSetString.isEmpty) {
      Some(ConstantString(s"alter session set $timestampSetString ;")!)
    } else if (timestampSettings.isEmpty) {
      Some(ConstantString(s"alter session set $timezoneSetString ;")!)
    } else {
      Some(ConstantString(s"alter session set $timezoneSetString , $timestampSetString ;")!)
    }
  }
  // Issue a set of changes reverting genPrologueSql
  private[snowflake] def genEpilogueSql(
    params: MergedParameters
  ): SnowflakeSQLStatement = {
    var timezoneUnsetString = ""
    // For all cases except for "snowflake", we unset the timezone
    if (!params.isTimezoneSnowflake) {
      timezoneUnsetString = "timezone,"
    }

    ConstantString("alter session unset") + timezoneUnsetString +
      ConstantString(s"""
         |date_output_format,
         |timestamp_ntz_output_format,
         |timestamp_ltz_output_format,
         |timestamp_tz_output_format;
       """.stripMargin)
  }

  private[snowflake] def executePreActions(jdbcWrapper: JDBCWrapper,
                                           conn: ServerConnection,
                                           params: MergedParameters,
                                           table: Option[TableName]): Unit = {
    // Execute preActions
    params.preActions.foreach { action =>
      if (action != null && !action.trim.isEmpty) {
        val actionSql =
          if (action.contains("%s")) action.format(table.get) else action
        log.info("Executing preAction: " + actionSql)
        jdbcWrapper.executePreparedInterruptibly(
          conn.prepareStatement(actionSql)
        )
      }
    }
  }

  private[snowflake] def executePostActions(jdbcWrapper: JDBCWrapper,
                                            conn: ServerConnection,
                                            params: MergedParameters,
                                            table: Option[TableName]): Unit = {
    // Execute preActions
    params.postActions.foreach { action =>
      if (action != null && !action.trim.isEmpty) {
        val actionSql =
          if (action.contains("%s")) action.format(table.get) else action
        log.info("Executing postAction: " + actionSql)
        jdbcWrapper.executePreparedInterruptibly(
          conn.prepareStatement(actionSql)
        )
      }
    }
  }

  def runQuery(params: Map[String, String], query: String): ResultSet = {
    val conn = getJDBCConnection(params)
    val resultSerializables = try {
      conn.createStatement()
        .executeQuery(query)
        .asInstanceOf[SnowflakeResultSet]
        .getResultSetSerializables(Long.MaxValue)
    } finally {
      conn.close()
    }

    // Long.MaxValue is passed to getResultSetSerializables(),
    // so it is sure there is only one object in resultSerializables
    val mergedParams = getMergedParameters(params)
    val jdbcProperties = new Properties()
    mergedParams.setJDBCProxyIfNecessary(jdbcProperties)
    resultSerializables.get(0).getResultSet(
      SnowflakeResultSetSerializable
        .ResultSetRetrieveConfig
        .Builder
        .newInstance()
        .setProxyProperties(jdbcProperties)
        .setSfFullURL(mergedParams.sfFullURL)
        .build()
    )
  }

  // Java version
  def runQuery(params: java.util.Map[String, String],
               query: String): ResultSet = {
    runQuery(params.asScala.toMap, query)
  }

  // Helper function for testing
  def printQuery(params: Map[String, String], query: String): Unit = {
    // scalastyle:off println
    System.out.println(s"Running: $query")
    val res = runQuery(params, query)
    val columnCount = res.getMetaData.getColumnCount
    var rowCnt = 0
    while (res.next()) {
      rowCnt += 1
      val s = StringBuilder.newBuilder
      s.append("| ")
      for (i <- 1 to columnCount) {
        if (i > 1) {
          s.append(" | ")
        }
        s.append(res.getString(i))
      }
      s.append(" |")
      System.out.println(s)
    }
    System.out.println(s"TOTAL: $rowCnt rows")
    // scalastyle:on println
  }

  /** Removes (hopefully :)) sensitive content from a query string */
  def sanitizeQueryText(q: String): String = {
    // scalastyle:off
    "<SANITIZED> " + q
      .replaceAll(
        "(AWS_KEY_ID|AWS_SECRET_KEY|AZURE_SAS_TOKEN)='[^']+'",
        "$1='❄☃❄☺❄☃❄'"
      )
      .replaceAll(
        "(sfaccount|sfurl|sfuser|sfpassword|sfwarehouse|sfdatabase|sfschema|sfrole|awsaccesskey|awssecretkey) \"[^\"]+\"",
        "$1 \"❄☃❄☺❄☃❄\""
      )
    // scalastyle:on
  }

  /**
    * create a map from string for column mapping
    */
  def parseMap(source: String): Map[String, String] = {
    if (source == null || source.length < 5 ||
        !(source.startsWith("Map(") && source.endsWith(")"))) {
      throw new UnsupportedOperationException("input map format is incorrect!")
    }
    source
      .substring(4, source.length - 1)
      .split(",")
      .map(x => {
        val names = x.split("->").map(_.trim)
        names(0) -> names(1)
      })
      .toMap
  }

  /**
    * ensure a name wrapped with double quotes
    */
  def ensureQuoted(name: String): String =
    if (isQuoted(name)) name else quotedName(name)

  /**
    * check whether a name is quoted
    */
  def isQuoted(name: String): Boolean = {
    name.startsWith("\"") && name.endsWith("\"")
  }

  /**
    * wrap a name with double quotes
    */
  def quotedName(name: String): String = {
    // Name legality check going from spark => SF.
    // If the input identifier is legal, uppercase before wrapping it with double quotes.
    if (name.matches("[_a-zA-Z]([_0-9a-zA-Z])*")) {
      "\"" + name.toUpperCase + "\""
    } else {
      "\"" + name + "\""
    }
  }

  /**
    * wrap a name with double quotes without capitalize letters
    */
  def quotedNameIgnoreCase(name: String): String =
    if (isQuoted(name)) name else s""""$name""""

  /**
    * Check whether the giving DataFrame contains variant type or not
    */
  def containVariant(schema: StructType): Boolean =
    schema.fields.map(DefaultJDBCWrapper.schemaConversion).contains("VARIANT")

  private[snowflake] def generateColumnMap(
    from: StructType,
    to: StructType,
    reportError: Boolean
  ): Map[String, String] = {

    def containsColumn(name: String, list: StructType): Boolean =
      list.exists(_.name.equalsIgnoreCase(name))

    val result = mutable.HashMap[String, String]()
    val fromNameMap = mutable.HashMap[String, String]()
    val toNameMap = mutable.HashMap[String, String]()

    // check duplicated name after to lower
    from.foreach(
      field =>
        fromNameMap.put(field.name.toLowerCase, field.name) match {
          case Some(_) =>
            // if Snowflake table doesn't contain this column, ignore it
            if (containsColumn(field.name, to)) {
              throw new UnsupportedOperationException(
                s"""
                   |Duplicated column names in Spark DataFrame: ${fromNameMap(
                     field.name.toLowerCase
                   )}, ${field.name}
             """.stripMargin
              )
            }
          case _ => // nothing
      }
    )

    to.foreach(
      field =>
        toNameMap.put(field.name.toLowerCase, field.name) match {
          case Some(_) =>
            // if Spark DataFrame doesn't contain this column, ignore it
            if (containsColumn(field.name, from)) {
              throw new UnsupportedOperationException(
                s"""
                   |Duplicated column names in Snowflake table: ${toNameMap(
                     field.name.toLowerCase
                   )}, ${field.name}
             """.stripMargin
              )
            }
          case _ => // nothing
      }
    )

    // check mismatch
    if (reportError)
      if (fromNameMap.size != toNameMap.size) {
        throw new UnsupportedOperationException(s"""
             |column number of Spark Dataframe (${fromNameMap.size})
             | doesn't match column number of Snowflake Table (${toNameMap.size})
           """.stripMargin)
      }

    fromNameMap.foreach {
      case (index, name) =>
        if (toNameMap.contains(index)) result.put(name, toNameMap(index))
        else if (reportError) throw new UnsupportedOperationException(s"""
             |can't find column $name in Snowflake Table
           """.stripMargin)
    }

    result.toMap
  }
  private[snowflake] def removeQuote(schema: StructType): StructType =
    new StructType(
      schema
        .map(
          field =>
            StructField(
              if (field.name.startsWith("\"") && field.name.endsWith("\"")) {
                field.name.substring(1, field.name.length - 1)
              } else {
                field.name
              },
              field.dataType,
              field.nullable
          )
        )
        .toArray
    )

  // Get pretty size String
  def getSizeString(size: Long): String = {
    if (size < 1024.toLong) {
      s"$size Bytes"
    } else if (size <  1024.toLong * 1024) {
      "%.2f KB".format(size.toDouble / 1024)
    } else if (size <  1024.toLong * 1024 * 1024) {
      "%.2f MB".format(size.toDouble / 1024 / 1024)
    } else if (size < 1024.toLong * 1024 * 1024 * 1024) {
      "%.2f GB".format(size.toDouble / 1024 / 1024 / 1024)
    } else {
      "%.2f TB".format(size.toDouble / 1024 / 1024 / 1024 / 1024)
    }
  }

  // Get pretty time String
  def getTimeString(milliSeconds: Long): String = {
    if (milliSeconds <  1000) {
      s"$milliSeconds ms"
    } else if (milliSeconds <  1000 * 60) {
      "%.2f seconds".format(milliSeconds.toDouble / 1000)
    } else if (milliSeconds <  1000 * 60 * 60) {
      "%.2f minutes".format(milliSeconds.toDouble / 1000 / 60)
    } else {
      "%.2f hours".format(milliSeconds.toDouble / 1000 / 60 / 60)
    }
  }

  // Very simple escaping
  private def esc(s: String): String = {
    s.replace("\"", "").replace("\\", "")
  }

  def getClientInfoString(): String = {
    val snowflakeClientInfo =
      s""" {
         | "spark.version" : "${esc(SPARK_VERSION)}",
         | "$PROPERTY_NAME_OF_CONNECTOR_VERSION" : "${esc(Utils.VERSION)}",
         | "spark.app.name" : "${esc(sparkAppName)}",
         | "scala.version" : "${esc(scalaVersion)}",
         | "java.version" : "${esc(javaVersion)}",
         | "snowflakedb.jdbc.version" : "${esc(jdbcVersion)}"
         |}""".stripMargin

    snowflakeClientInfo
  }

  def getClientInfoJson(): ObjectNode = {
    SnowflakeTelemetry.getClientConfig()
  }

  private[snowflake] def addVersionInfo(metric: ObjectNode): ObjectNode = {
    metric.put(TelemetryClientInfoFields.SPARK_CONNECTOR_VERSION, esc(VERSION))
    metric.put(TelemetryClientInfoFields.SPARK_VERSION, esc(SPARK_VERSION))
    metric.put(TelemetryClientInfoFields.APPLICATION_NAME, esc(sparkAppName))
    metric.put(TelemetryClientInfoFields.SCALA_VERSION, esc(scalaVersion))
    metric.put(TelemetryClientInfoFields.JAVA_VERSION, esc(javaVersion))
    metric.put(TelemetryClientInfoFields.JDBC_VERSION, esc(jdbcVersion))
    metric.put(TelemetryClientInfoFields.CERTIFIED_JDBC_VERSION,
      esc(CERTIFIED_JDBC_VERSION))

    metric
  }

  /**
    * Print JDBC ResultSet for debugging purpose
    */
  private[snowflake] def printResultSet(rs: ResultSet): Unit = {
    // scalastyle:off println
    try {
      val columnCount = rs.getMetaData.getColumnCount
      val sb = new StringBuilder
      for (i <- 1 to columnCount) {
        sb.append(s"${rs.getMetaData.getColumnName(i)}(${rs.getMetaData.getColumnTypeName(i)}) | ")
      }
      println(sb.toString())
      while (rs.next) {
        sb.clear()
        for (i <- 1 to columnCount) {
          sb.append(rs.getString(i)).append(" | ")
        }
        println(sb.toString())
      }
    } catch {
      case th: Throwable =>
        println(s"Fail to print result set: ${th.getMessage}")
    }
    // scalastyle:off println
  }

  // Adjust table name by adding database or schema name for table existence check.
  private[snowflake] def getTableNameForExistenceCheck(database: String,
                                                       schema: String,
                                                       name: String): String = {
    val unQuotedIdPattern = """([a-zA-Z_][\w$]*)"""
    val quotedIdPattern = """("([^"]|"")+")"""
    val idPattern = s"($unQuotedIdPattern|$quotedIdPattern)"

    // scalastyle:off
    // Only add the database/schema name for <id> and <id>.<id>
    // If the name is fully qualified name or invalid name, don't need to adjust it.
    // NOTE: <id>..<id> is equals to <id>.PUBLIC.<id> which is a fully qualified name.
    // refer to https://docs.snowflake.com/en/sql-reference/name-resolution.html#resolution-when-schema-omitted-double-dot-notation
    // scalastyle:off
    if (name.matches(idPattern)) {
      // Add database and schema name
      s"${ensureQuoted(database)}.${ensureQuoted(schema)}.$name"
    } else if (name.matches(s"$idPattern\\.$idPattern")) {
      // Add database name
      s"${ensureQuoted(database)}.$name"
    } else {
      // For all the other cases, don't adjust it
      name
    }
  }

}
