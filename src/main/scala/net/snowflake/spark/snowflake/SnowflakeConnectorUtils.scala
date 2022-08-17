/*
 * Copyright 2015-2018 Snowflake Computing
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.snowflake.spark.snowflake

import java.nio.file.Paths
import java.security.InvalidKeyException

import net.snowflake.spark.snowflake.pushdowns.SnowflakeStrategy
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

/** Connector utils, including what needs to be invoked to enable pushdowns. */
object SnowflakeConnectorUtils {

  @transient lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  /**
    * Check Spark version, if Spark version matches SUPPORT_SPARK_VERSION enable PushDown,
    * otherwise disable it.
    */
  val SUPPORT_SPARK_VERSION = "3.3"

  def checkVersionAndEnablePushdown(session: SparkSession): Boolean =
    if (session.version.startsWith(SUPPORT_SPARK_VERSION)) {
      enablePushdownSession(session)
      true
    } else {
      log.warn("Query pushdown is not supported because you are using " +
        s"Spark ${session.version} with a connector designed to support Spark " +
        s"$SUPPORT_SPARK_VERSION. Either use the version of Spark supported by " +
        s"the connector or install a version of the connector that supports " +
        s"your version of Spark.")
      disablePushdownSession(session)
      false
    }

  /** Enable more advanced query pushdowns to Snowflake.
    *
    * @param session The SparkSession for which pushdowns are to be enabled.
    */
  def enablePushdownSession(session: SparkSession): Unit = {
    if (!session.experimental.extraStrategies.exists(
          s => s.isInstanceOf[SnowflakeStrategy]
        )) {
      session.experimental.extraStrategies ++= Seq(new SnowflakeStrategy)
    }
  }

  /** Disable more advanced query pushdowns to Snowflake.
    *
    * @param session The SparkSession for which pushdowns are to be disabled.
    */
  def disablePushdownSession(session: SparkSession): Unit = {
    session.experimental.extraStrategies = session.experimental.extraStrategies
      .filterNot(strategy => strategy.isInstanceOf[SnowflakeStrategy])
  }

  def setPushdownSession(session: SparkSession, enabled: Boolean): Unit = {
    if (enabled) {
      enablePushdownSession(session)
    } else {
      disablePushdownSession(session)
    }
  }

  // TODO: Improve error handling with retries, etc.

  @throws[SnowflakeConnectorException]
  def handleS3Exception(ex: Exception): Unit = {
    if (ex.getCause.isInstanceOf[InvalidKeyException]) {
      // Most likely cause: Unlimited strength policy files not installed
      var msg: String = "Strong encryption with Java JRE requires JCE " +
        "Unlimited Strength Jurisdiction Policy " +
        "files. " +
        "Follow JDBC client installation instructions " +
        "provided by Snowflake or contact Snowflake " +
        "Support. This needs to be installed in the Java runtime for all Spark executor nodes."

      log.error(
        "JCE Unlimited Strength policy files missing: {}. {}.",
        ex.getMessage: Any,
        ex.getCause.getMessage: Any
      )

      val bootLib: String =
        java.lang.System.getProperty("sun.boot.library.path")

      if (bootLib != null) {
        msg += " The target directory on your system is: " + Paths
          .get(bootLib, "security")
          .toString
        log.error(msg)
      }

      throw new SnowflakeConnectorException(msg)
    } else {
      throw ex
    }
  }
}

class SnowflakeConnectorException(message: String) extends Exception(message)
class SnowflakePushdownException(message: String)
  extends SnowflakeConnectorException(message)
class SnowflakeConnectorFeatureNotSupportException(message: String)
  extends Exception(message)

object SnowflakeFailMessage {
  // Note: don't change the message context except necessary
  final val FAIL_PUSHDOWN_STATEMENT = "pushdown failed"
  final val FAIL_PUSHDOWN_GENERATE_QUERY = "pushdown failed in generateQueries"
  final val FAIL_PUSHDOWN_SET_TO_EXPR = "pushdown failed in setToExpr"
  final val FAIL_PUSHDOWN_AGGREGATE_EXPRESSION = "pushdown failed for aggregate expression"
  final val FAIL_PUSHDOWN_UNSUPPORTED_CONVERSION = "pushdown failed for unsupported conversion"
  final val FAIL_PUSHDOWN_UNSUPPORTED_UNION = "pushdown failed for Spark feature: UNION by name"
}

class SnowflakePushdownUnsupportedException(message: String,
                                            val unsupportedOperation: String,
                                            val details: String,
                                            val isKnownUnsupportedOperation: Boolean)
  extends Exception(message)
