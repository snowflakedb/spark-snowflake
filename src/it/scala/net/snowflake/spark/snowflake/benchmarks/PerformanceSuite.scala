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

import net.snowflake.spark.snowflake.pushdowns.SnowflakeStrategy
import net.snowflake.spark.snowflake.{
  IntegrationSuiteBase,
  SnowflakeConnectorUtils
}
import org.scalatest.exceptions.TestFailedException

import scala.collection.mutable

trait PerformanceSuite extends IntegrationSuiteBase {

  protected final val runOptionAccepted    = Set[String]("both", "full-only", "partial-only")
  protected final val outputFormatAccepted = Set[String]("csv", "print")

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

  override def beforeAll(): Unit = {
    super.beforeAll()

    sessionStatus = getSessionStatus()

    try {
      runOption = getConfigValue("runOption")
      outputFormat = getConfigValue("outputFormat")

      for ((k, v) <- requiredParams)
        requiredParams.put(k, getConfigValue(k).toLowerCase)

    } catch {
      case t: TestFailedException => {
        if (t.getMessage contains "Config file needs to contain") {
          runTests = false
          val reqParams = requiredParams.keySet.mkString(", ")
          println(
            s"""One or more required parameters for running the benchmark suite was missing: " +
              "runOption, outputFormat, $reqParams. Skipping ${getClass.getSimpleName}.""")
        } else throw t

      }
      case e: Exception => throw e
    }

    if (runTests)
      verifyParams()
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

    var result: String = ""

    if (runOption == "both") {
      result = runWithSnowflake(query, name, false).getOrElse(failedMessage)
      outputResult(
        s"""${name.toUpperCase}, with only filter/proj pushdowns: """ + result)

      result = runWithSnowflake(query, name, true).getOrElse(failedMessage)
      outputResult(s"""${name.toUpperCase}, with full pushdowns: """ + result)

    } else if (runOption == "full-only") {
      result = runWithSnowflake(query, name, true).getOrElse(failedMessage)
      outputResult(s"""${name.toUpperCase}, with full pushdowns: """ + result)
    } else {
      result = runWithSnowflake(query, name, false).getOrElse(failedMessage)
      outputResult(
        s"""${name.toUpperCase}, with only filter/proj pushdowns: """ + result)
    }

    // runWithoutSnowflake(query)
  }

  protected def outputResult(result: String): Unit = {
    if (outputFormat == "print") {
      println(result)
    }
  }

  protected final def runWithSnowflake(
      sql: String,
      name: String,
      pushdown: Boolean = true): Option[String] = {

    val state = getSessionStatus()
    SnowflakeConnectorUtils.setPushdownSession(sparkSession, pushdown)

    try {
      val t1 = System.nanoTime()
      sparkSession.sql(sql).collect()
      Some(((System.nanoTime() - t1) / 1e9d).toString)
    } catch {
      case e: Exception => {
        println(s"""Query $name failed.""")
        throw(e)
        None
      }
    } finally {
      SnowflakeConnectorUtils.setPushdownSession(sparkSession, state)
    }

  }

  protected final def getSessionStatus(): Boolean = {
    !(sparkSession.experimental.extraStrategies
      .find(s => s.isInstanceOf[SnowflakeStrategy])
      .isEmpty)
  }
}
