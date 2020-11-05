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

import scala.collection.mutable.ListBuffer

private[snowflake] class StressTestResultBuilder(email: Option[String] = None) extends BaseTestResultBuilder {

  protected[snowflake] var testRevisionNumber: Int = _
  protected[snowflake] var subTaskResults: ListBuffer[TestStatus] = ListBuffer()

  override def build(): ClusterTestResult = {
    new StressTestResult(this, email)
  }

  override def withStartTimeInMill(startTimeInMillis: Long): StressTestResultBuilder = {
    this.overallTestStatus.taskStartTime = startTimeInMillis
    this
  }

  override def withEndTimeInMill(endTimeInMillis: Long): StressTestResultBuilder = {
    this.overallTestStatus.taskEndTime = endTimeInMillis
    this
  }
  override def withTestCaseName(testCaseName: String): StressTestResultBuilder = {
    this.overallTestStatus.testName = testCaseName
    this
  }
  override def withTestStatus(testStatus: String): StressTestResultBuilder = {
    this.overallTestStatus.testStatus = testStatus
    this
  }
  override def withReason(reason: Option[String]): StressTestResultBuilder = {
    this.overallTestStatus.reason = reason
    this
  }

  def withTestRevision(revision: Int): StressTestResultBuilder = {
    this.testRevisionNumber = revision
    this
  }
  override def withNewSubTaskResult(subTaskResult: TestStatus): StressTestResultBuilder = {
    subTaskResults.append(subTaskResult)
    this
  }
}
