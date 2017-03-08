/*
 * Copyright 2015-2016 Snowflake Computing
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

package net.snowflake.spark.snowflake.benchmarks

import java.io.{BufferedWriter, File, FileWriter, Writer}
import java.util.Properties

import net.snowflake.spark.snowflake.pushdowns.SnowflakeStrategy
import net.snowflake.spark.snowflake.{
  IntegrationSuiteBase,
  SnowflakeConnectorUtils
}
import org.apache.spark.sql.DataFrame
import org.scalatest.exceptions.TestFailedException

import scala.collection.mutable

trait PerformanceSuite extends IntegrationSuiteBase {

  protected final val runOptionAccepted = Set[String](
    "all",
    "jdbc-source",
    "s3-all",
    "s3-parquet",
    "s3-csv",
    "snowflake-all",
    "snowflake-with-pushdown",
    "snowflake-partial-pushdown")

  protected final var fullPushdown: Boolean    = false
  protected final var partialPushdown: Boolean = false
  protected final var s3CSV: Boolean           = false
  protected final var s3Parquet: Boolean       = false
  protected final var jdbcSource: Boolean      = false

  protected final val outputFormatAccepted =
    Set[String]("csv", "print", "both")
  protected var dataSources: mutable.LinkedHashMap[String,
                                                   Map[String, DataFrame]]
  protected final var headersWritten: Boolean    = false
  protected final var fileWriter: Option[Writer] = None

  protected final var currentSource: String = ""

  // For implementing classes to add their own required config params
  protected var requiredParams: mutable.LinkedHashMap[String, String]
  protected var acceptedArguments: mutable.LinkedHashMap[String, Set[String]]

  /** Configuration string for run mode for benchmarks */
  protected final var runOption: String = ""

  /** Configuration string for output: simple print or CSV */
  protected final var outputFormat: String = ""

  protected final var runTests: Boolean = true

  // Maintain session state to make sure it is restored upon finishing of suite
  protected final var sessionStatus: Boolean = false

  protected final var jdbcProperties: Properties = new Properties
  protected final var jdbcURL: String            = ""

  override def beforeAll(): Unit = {
    super.beforeAll()

    sessionStatus = sparkSession.experimental.extraStrategies.exists(s =>
      s.isInstanceOf[SnowflakeStrategy])

    try {
      runOption = getConfigValue("runOption")
      outputFormat = getConfigValue("outputFormat")

      for ((k, v) <- requiredParams)
        requiredParams.put(k, getConfigValue(k).toLowerCase)

    } catch {
      case t: TestFailedException =>
        if (t.getMessage contains "Config file needs to contain") {
          runTests = false
          val reqParams = requiredParams.keySet.mkString(", ")
          println(
            s"""One or more required parameters for running the benchmark suite was missing: " +
              "runOption, outputFormat, $reqParams. Skipping ${getClass.getSimpleName}.""")
        } else throw t

      case e: Exception => throw e
    }

    if (runTests) {
      verifyParams()
      if (outputFormat == "csv" || outputFormat == "both") prepareCSV()
    }

    partialPushdown = Set("all", "snowflake-all", "snowflake-partial-pushdown") contains runOption
    fullPushdown = Set("all", "snowflake-all", "snowflake-with-pushdown") contains runOption
    jdbcSource = Set("all", "jdbc-source") contains runOption
    s3Parquet = Set("all", "s3-all", "s3-parquet") contains runOption
    s3CSV = Set("all", "s3-all", "s3-csv") contains runOption

    jdbcURL = s"""jdbc:snowflake://${params.sfURL}"""

    jdbcProperties.put("db", params.sfDatabase)
    jdbcProperties.put("schema", params.sfSchema) // Has a default
    jdbcProperties.put("user", params.sfUser)
    jdbcProperties.put("password", params.sfPassword)
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
  }

  protected final def verifyParams(): Unit = {

    val argCheckMap = mutable.LinkedHashMap() ++= acceptedArguments
    argCheckMap.put("runOption", runOptionAccepted)
    argCheckMap.put("outputFormat", outputFormatAccepted)

    val fullParams = mutable.LinkedHashMap() ++= requiredParams
    fullParams.put("runOption", runOption)
    fullParams.put("outputFormat", outputFormat)

    for ((param, acceptedSet) <- argCheckMap) {
      val argValue = fullParams
        .getOrElse(param, fail(s"Required parameter $param missing."))
      if (!acceptedSet.contains(argValue) && !acceptedSet.contains("*"))
        fail(
          s"""Value $argValue not accepted for parameter $param. Accepted values are: ${acceptedSet
            .mkString(", ")} """)
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SnowflakeConnectorUtils.setPushdownSession(sparkSession, sessionStatus)

    if (fileWriter.isDefined) {
      fileWriter.get.close()
    }
  }

  /**
    * Run the query using the given runtime configurations (S3-direct, with pushdown, without pushdown, etc.,
    * outputting using also the provided config value.
    */
  protected final def testQuery(query: String,
                                name: String = "unnamed"): Unit = {

    val failedMessage = s"""Failed: $name. No result generated."""

    // Skip if invalid configs
    if (!runTests) return

    var columnWriters =
      new mutable.ListBuffer[((String, String) => Option[String])]

    var outputHeaders =
      new mutable.ListBuffer[String]

    if (partialPushdown) {
      columnWriters += runWithSnowflake(pushdown = false)
      outputHeaders += s"""Only filter/proj pushdowns"""
    }

    if (fullPushdown) {
      columnWriters += runWithSnowflake(pushdown = true)
      outputHeaders += s"""With full pushdowns"""
    }

    if (jdbcSource) {
      columnWriters += runWithoutSnowflake(format = "jdbc")
      outputHeaders += s"""Using Spark JDBC Source"""
    }

    if (s3Parquet) {
      columnWriters += runWithoutSnowflake(format = "parquet")
      outputHeaders += s"""Direct from S3 Parquet"""
    }

    if (s3CSV) {
      columnWriters += runWithoutSnowflake(format = "csv")
      outputHeaders += s"""Direct from S3 CSV"""
    }

    val results =
      columnWriters.map(f => f(query, name).getOrElse(failedMessage))

    if (outputFormat == "print" || outputFormat == "both") {
      outputHeaders
        .zip(results)
        .foreach(x => println(name + ", " + x._1 + ": " + x._2))
    }

    if (outputFormat == "csv" || outputFormat == "both") {
      writeToCSV(outputHeaders, results, name)
    }
  }

  protected final def writeToCSV(headers: Seq[String],
                                 results: Seq[String],
                                 name: String): Unit = {

    val writer = {
      if (fileWriter.isEmpty) prepareCSV()
      fileWriter.get
    }

    if (!headersWritten) {
      headersWritten = true
      writer.write("Name, " + headers.mkString(", ") + "\n")
    }

    writer.write(name + ", " + results.mkString(", ") + "\n")
  }

  protected final def prepareCSV(): Unit = {

    if (fileWriter.isEmpty) {
      try {
        val outputFile = new File(getConfigValue("outputFile"))
        fileWriter = Some(
          new BufferedWriter(new FileWriter(outputFile, false)))
      } catch {
        case e: Exception =>
          if (fileWriter.isDefined) {
            fileWriter.get.close()
          }
          throw e
      }
    }
  }

  protected final def runWithSnowflake(
      pushdown: Boolean)(sql: String, name: String): Option[String] = {

    if (currentSource != "snowflake") {
      dataSources.foreach {
        case (tableName: String, sources: Map[String, DataFrame]) => {
          val df: DataFrame = sources.getOrElse(
            "snowflake",
            fail(
              "Snowflake datasource missing for snowflake performance test."))
          df.createOrReplaceTempView(tableName)
        }
      }
      currentSource = "snowflake"
    }

    val state = sessionStatus
    SnowflakeConnectorUtils.setPushdownSession(sparkSession, pushdown)
    val result = executeSqlBenchmarkStatement(sql, name)
    SnowflakeConnectorUtils.setPushdownSession(sparkSession, state)
    result
  }

  private def executeSqlBenchmarkStatement(sql: String,
                                           name: String): Option[String] = {
    try {
      val t1 = System.nanoTime()
      sparkSession.sql(sql).collect()
      Some(((System.nanoTime() - t1) / 1e9d).toString)
    } catch {
      case _: Exception =>
        println(s"""Query $name failed.""")
        None
    }
  }

  /* Used for running direct from S3, or JDBC Source */
  protected final def runWithoutSnowflake(
      format: String)(sql: String, name: String): Option[String] = {

    if (currentSource != format) {
      dataSources.foreach {
        case (tableName: String, sources: Map[String, DataFrame]) => {
          val df: DataFrame = sources.getOrElse(
            format,
            fail(
              s"$format datasource missing for snowflake performance test."))
          df.createOrReplaceTempView(tableName)
        }
      }
      currentSource = format
    }

    executeSqlBenchmarkStatement(sql, name)
  }
}
