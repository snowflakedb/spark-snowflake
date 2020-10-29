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

private[snowflake] object TelemetryReporter {

  private var driverTelemetryReporter: TelemetryReporter = new NoopTelemetryReporter()

  private[snowflake] def setDriverTelemetryReporter(tr: TelemetryReporter): Unit = {
    driverTelemetryReporter = tr
  }

  private[snowflake] def resetDriverTelemetryReporter(): Unit = {
    driverTelemetryReporter = new NoopTelemetryReporter()
  }

  private val executorTelemetryReporters = new ThreadLocal[TelemetryReporter]() {
    override protected def initialValue = new NoopTelemetryReporter()
  }

  private val isExecutorThread = new ThreadLocal[Boolean]() {
    override protected def initialValue = false
  }

  private[snowflake] def setExecutorTelemetryReporter(tr: TelemetryReporter): Unit = {
    executorTelemetryReporters.set(tr)
    isExecutorThread.set(true)
  }

  private[snowflake] def resetExecutorTelemetryReporter(): Unit = {
    executorTelemetryReporters.set(new NoopTelemetryReporter())
    isExecutorThread.set(false)
  }

  private[snowflake] def getTelemetryReporter(): TelemetryReporter = {
    if (isExecutorThread.get()) {
      executorTelemetryReporters.get()
    } else {
      driverTelemetryReporter
    }
  }

}

private[snowflake] trait TelemetryReporter {
  private[snowflake] def sendLogTelemetry(level: String, msg: String): Unit
}

private[snowflake] class NoopTelemetryReporter extends TelemetryReporter{
  private[snowflake] override def sendLogTelemetry(level: String, msg: String): Unit = {}
}

private[snowflake] class DriverTelemetryReporter extends TelemetryReporter{
  private[snowflake] override def sendLogTelemetry(level: String, msg: String): Unit = {
    // TODO: Sending telemetry message on driver in next step
    // println(s"Driver logging: $level: $msg")
  }
}

private[snowflake] class ExecutorTelemetryReporter extends TelemetryReporter{
  private[snowflake] override def sendLogTelemetry(level: String, msg: String): Unit = {
    // TODO: Sending telemetry message on executor with partition info in next step
    // println(s"Executor logging: $level: $msg")
  }
}

