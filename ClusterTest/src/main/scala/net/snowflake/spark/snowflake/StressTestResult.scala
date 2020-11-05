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

import javax.mail.{Message, Session, Transport}
import javax.mail.internet.{InternetAddress, MimeMessage}
import org.slf4j.{Logger, LoggerFactory}
import net.snowflake.spark.snowflake.StressTestResult.{ErrorCaseReport, tryToSendEmail}

import scala.collection.mutable.ListBuffer

private[snowflake] object StressTestResult {

  /**
    * Class representing an error for either a suite or a subtask.
    */
  case class ErrorCaseReport(testName: String, status: String, message: String)

  val log: Logger = LoggerFactory.getLogger(getClass)

  def tryToSendEmail(emailTo: String, emailSubject: String, msg: String): Unit = {
    val host = System.getenv(TestUtils.STRESS_TEST_EMAIL_HOST)
    val emailFrom = System.getenv(TestUtils.STRESS_TEST_EMAIL_FROM)

    if (host == null || emailFrom == null) {
      return
    }

    val properties = System.getProperties
    properties.setProperty("mail.smtp.host", host)

    val session = Session.getDefaultInstance(properties)

    try {
      val message = new MimeMessage(session)
      message.setFrom(new InternetAddress(emailFrom))
      message.addRecipient(Message.RecipientType.TO, new InternetAddress(emailTo))
      message.setSubject(emailSubject)
      message.setText(msg)

      Transport.send(message)
    } catch {
      // Log, but don't crash if the email failed to send.
      case mex: Exception => {
        log.info(s"Failed to send failure notice email to $emailTo. stack trace: ${mex.getMessage}")
        mex.printStackTrace()
      }
    }
  }
}

private[snowflake] class StressTestResult(builder: StressTestResultBuilder, email: Option[String]) extends ClusterTestResult {
  val testSuiteName: String = builder.overallTestStatus.testName
  val testStatus: String = builder.overallTestStatus.testStatus
  val startTime: String = TestUtils.formatTimestamp(builder.overallTestStatus.taskStartTime)
  val testRunTime: String = TestUtils.formatTimeElapsed(builder.overallTestStatus)
  val reason: String =
    builder.overallTestStatus.reason.getOrElse(TestUtils.TEST_RESULT_REASON_NO_REASON)

  def writeToSnowflake(): Unit = {
    val connection = DefaultJDBCWrapper.getConnector(TestUtils.param)

    // Create the sequence for id generation, if it does not exist
    DefaultJDBCWrapper.executeInterruptibly(
      connection,
      s"create sequence if not exists ${TestUtils.STRESS_TEST_SEQ_NAME}")

    // Create the overall test-result table if it doesn't exist.
    if (!DefaultJDBCWrapper.tableExists(connection, TestUtils.STRESS_TEST_RESULTS_TABLE)) {
      DefaultJDBCWrapper.executeInterruptibly(
        connection,
        s"""create table ${TestUtils.STRESS_TEST_RESULTS_TABLE} (
           | testRevision Integer,
           | runId Integer,
           | testName String,
           | testStatus String,
           | startTime String,
           | testRunTime String,
           | reason String )
           |""".stripMargin)
    }

    // Get the next runID
    val res = DefaultJDBCWrapper
      .executeQueryInterruptibly(connection, s"select ${TestUtils.STRESS_TEST_SEQ_NAME}.nextVal")
    res.next()

    val runId = res.getLong(1)

    // Create the detailed test-result table if it doesn't exist.
    if (!DefaultJDBCWrapper.tableExists(connection, TestUtils.STRESS_TEST_DETAILED_RESULTS_TABLE)) {
      // Create the subtask-result table for this run
      DefaultJDBCWrapper.executeInterruptibly(
        connection,
        s"""create table ${TestUtils.STRESS_TEST_DETAILED_RESULTS_TABLE} (
           | revisionNumber Integer,
           | runId Integer,
           | testSuiteName String,
           | testName String,
           | testStatus String,
           | startTime String,
           | testRunTime String,
           | reason String )
           |""".stripMargin)
    }

    // Write test result into the main result table
    DefaultJDBCWrapper.executeInterruptibly(
      connection,
      s"""insert into ${TestUtils.STRESS_TEST_RESULTS_TABLE} values (
         |  ${builder.testRevisionNumber},
         |  $runId,
         | '$testSuiteName' ,
         | '$testStatus' ,
         | '$startTime' ,
         | '$testRunTime' ,
         | '$reason'
         | ) """.stripMargin)

    var errorsEncountered: ListBuffer[ErrorCaseReport] = ListBuffer()

    if (testStatus != TestUtils.TEST_RESULT_STATUS_SUCCESS) {
      val err = ErrorCaseReport(testSuiteName, testStatus,
        s"Test suite was not successful, reason: $reason")
      errorsEncountered.append(err)
    }

    // Now write the results of the individual subtasks.
    builder.subTaskResults.foreach(subTask => {
      if (subTask.testStatus != TestUtils.TEST_RESULT_STATUS_SUCCESS) {
        val err = ErrorCaseReport(subTask.testName, subTask.testStatus,
          s"Subtask ${subTask.testName} in $testSuiteName was not successful, reason: $reason")
        errorsEncountered.append(err)
      }

      DefaultJDBCWrapper.executeInterruptibly(
        connection,
        s"""insert into ${TestUtils.STRESS_TEST_DETAILED_RESULTS_TABLE} values (
           | ${builder.testRevisionNumber},
           | $runId,
           | '$testSuiteName',
           | '${subTask.testName}' ,
           | '${subTask.testStatus}' ,
           | '${TestUtils.formatTimestamp(subTask.taskStartTime)}' ,
           | '${TestUtils.formatTimeElapsed(subTask)}' ,
           | '${subTask.reason.getOrElse(TestUtils.TEST_RESULT_REASON_NO_REASON)}'
           | ) """.stripMargin)
    })

    connection.close()

    // If a notification email on failure was defined, email that address
    if (email.isDefined && errorsEncountered.nonEmpty) {
      val emailContent = errorsEncountered.mkString("\n")
      tryToSendEmail(email.get, "Spark Connector Stress-Test Failure Notice", emailContent)
    }
  }
}
