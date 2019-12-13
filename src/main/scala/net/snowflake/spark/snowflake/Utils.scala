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
import java.util.UUID

import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.spark.SparkContext

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.io._
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.{
  AmazonS3Client,
  AmazonS3URI
}
import net.snowflake.client.jdbc.internal.amazonaws.services.s3.model.BucketLifecycleConfiguration
import net.snowflake.spark.snowflake.FSType.FSType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
  * Various arbitrary helper functions
  */
object Utils {

  /**
    * Literal to be used with the Spark DataFrame's .format method
    */
  val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

  val VERSION = "2.5.6"

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
        Unit
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
      assert(index > 0, "Can't parse this line: " + line)
      val key = line.substring(0, index).trim.toLowerCase
      val value = line.substring(index + 1).trim
      if (!key.startsWith("#")) {
        map += (key -> value)
      }
    }
    map.toMap
  }

  def getJDBCConnection(params: Map[String, String]): Connection = {
    val wrapper = new JDBCWrapper()
    val lcParams = params.map { case (key, value) => (key.toLowerCase, value) }
    val mergedParams = MergedParameters(
      Parameters.DEFAULT_PARAMETERS ++ lcParams
    )
    wrapper.getConnector(mergedParams)
  }

  def getJDBCConnection(params: java.util.Map[String, String]): Connection = {
    val m2: Map[String, String] = params.asScala.toMap
    getJDBCConnection(m2)
  }

  // Stores the last generated Select query
  private var lastCopyUnload: String = _
  private var lastCopyLoad: String = _
  private var lastSelect: String = _
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

  private[snowflake] def setLastCopyLoad(select: String): Unit = {
    lastCopyLoad = select
  }

  def getLastCopyLoad: String = {
    lastCopyLoad
  }

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
  ): SnowflakeSQLStatement = {
    // Determine the timezone we want to use
    val tz = params.sfTimezone
    var timezoneSetString = ""
    if (params.isTimezoneSpark) {
      // Use the Spark-level one
      val tzStr = java.util.TimeZone.getDefault.getID
      timezoneSetString = s"timezone = '$tzStr',"
    } else if (params.isTimezoneSnowflake) {
      // Do nothing
    } else if (params.isTimezoneSnowflakeDefault) {
      // Set it to the Snowflake default
      timezoneSetString = "timezone = default,"
    } else {
      // Otherwise, use the specified value
      timezoneSetString = s"timezone = '${tz.get}',"
    }
    log.debug(s"sfTimezone: '$tz'   timezoneSetString '$timezoneSetString'")

    ConstantString("alter session set") + timezoneSetString +
      ConstantString(s"""
         |timestamp_ntz_output_format = 'YYYY-MM-DD HH24:MI:SS.FF3',
         |timestamp_ltz_output_format = 'TZHTZM YYYY-MM-DD HH24:MI:SS.FF3',
         |timestamp_tz_output_format = 'TZHTZM YYYY-MM-DD HH24:MI:SS.FF3';
       """.stripMargin)
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
                                           conn: Connection,
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
                                            conn: Connection,
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
    conn.createStatement().executeQuery(query)
  }

  // Java version
  def runQuery(params: java.util.Map[String, String],
               query: String): ResultSet = {
    val conn = getJDBCConnection(params)
    conn.createStatement().executeQuery(query)
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

}
