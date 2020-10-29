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

import org.slf4j.helpers.MessageFormatter
import org.slf4j.{Logger, LoggerFactory}

/**
  * Logger Wrapper class.
  *
  * The logger wrapper may do some special handling for the log message,
  * for example, sending telemetry messages. And then pass through the logging
  * request to internal logger object.
  *
  * NOTE: LoggerWrapper only implements the functions which are used by Spark
  *       Connector. If other functions in <code>org.slf4j.Logger</code> need
  *       to be used, they need to be implemented in this class.
  */
private[snowflake] class LoggerWrapper(logger: Logger) {

  // Do NOT throw exception when sending log entry
  private def sendLogTelemetryIfEnabled(level: String, msg: String): Unit = {
    try {
      // Send the message as telemetry
      TelemetryReporter.getTelemetryReporter.sendLogTelemetry(level, msg)
    } catch {
      case th: Throwable => {
        logger.warn(s"Fail to send log entry as telemetry message: ${th.getMessage}")
      }
    }
  }

  /**
    * Log a message at the TRACE level.
    *
    * @param msg the message string to be logged
    */
  def trace(msg: String): Unit = {
    logger.trace(msg)
    sendLogTelemetryIfEnabled(LoggerWrapper.TRACE_LEVEL, msg)
  }

  /**
    * Log a message at the DEBUG level.
    *
    * @param msg the message string to be logged
    */
  def debug(msg: String): Unit = {
    logger.debug(msg)
    sendLogTelemetryIfEnabled(LoggerWrapper.DEBUG_LEVEL, msg)
  }

  /**
    * Log a message at the INFO level.
    *
    * @param msg the message string to be logged
    */
  def info(msg: String): Unit = {
    logger.info(msg)
    sendLogTelemetryIfEnabled(LoggerWrapper.INFO_LEVEL, msg)
  }

  /**
    * Log a message at the WARN level.
    *
    * @param msg the message string to be logged
    */
  def warn(msg: String): Unit = {
    logger.warn(msg)
    sendLogTelemetryIfEnabled(LoggerWrapper.WARN_LEVEL, msg)
  }

  /**
    * Log a message at the WARN level.
    *
    * @param msg the message string to be logged
    * @param t   the exception (throwable) to log
    */
  def warn(msg: String, t: Throwable): Unit = {
    logger.warn(msg, t)
    sendLogTelemetryIfEnabled(LoggerWrapper.WARN_LEVEL, s"$msg: ${t.getMessage}")
  }

  /**
    * Log a message at the ERROR level.
    *
    * @param msg the message string to be logged
    */
  def error(msg: String): Unit = {
    logger.error(msg)
    sendLogTelemetryIfEnabled(LoggerWrapper.ERROR_LEVEL, msg)
  }

  /**
    * Log an exception (throwable) at the ERROR level with an
    * accompanying message.
    *
    * @param msg the message accompanying the exception
    * @param t   the exception (throwable) to log
    */
  def error(msg: String, t: Throwable): Unit = {
    logger.error(msg, t)
    sendLogTelemetryIfEnabled(LoggerWrapper.ERROR_LEVEL, s"$msg: ${t.getMessage}")
  }

  /**
    * Log a message at the ERROR level according to the specified format
    * and arguments.
    * <p/>
    * <p>This form avoids superfluous object creation when the logger
    * is disabled for the ERROR level. </p>
    *
    * @param format the format string
    * @param arg1   the first argument
    * @param arg2   the second argument
    */
  def error(format: String, arg1: Any, arg2: Any): Unit = {
    logger.error(format, arg1, arg2)
    val logEntry = try {
      // Never throw exception
      MessageFormatter.format(format, arg1, arg2).getMessage
    } catch {
      case _: Throwable => {
        s"$format, ${arg1.toString}, ${arg2.toString}"
      }
    }
    sendLogTelemetryIfEnabled(LoggerWrapper.ERROR_LEVEL, logEntry)
  }
}

private[snowflake] object LoggerWrapper {
  // Log levels
  val TRACE_LEVEL = "TRACE"
  val DEBUG_LEVEL = "DEBUG"
  val INFO_LEVEL = "INFO"
  val WARN_LEVEL = "WARN"
  val ERROR_LEVEL = "ERROR"
}

/**
  * The <code>LoggerWrapperFactory</code> is a utility class producing a
  * <code>LoggerWrapper</code> object.
  */
private[snowflake] object LoggerWrapperFactory {
  private[snowflake] def getLoggerWrapper(clazz: Class[_]): LoggerWrapper = {
    new LoggerWrapper(LoggerFactory.getLogger(clazz))
  }

  def getLoggerWrapper(name: String): LoggerWrapper = {
    new LoggerWrapper(LoggerFactory.getLogger(name))
  }
}
