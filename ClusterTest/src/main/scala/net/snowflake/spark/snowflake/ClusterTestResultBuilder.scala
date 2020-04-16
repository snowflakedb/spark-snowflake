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

class ClusterTestResultBuilder() {
  private[snowflake] var testType = "Scala" // There are Scala test and Python test.
  private[snowflake] var commitID: String = _
  private[snowflake] var githubRunId: String = _
  private[snowflake] var startTimeInMillis: Long = 0
  private[snowflake] var endTimeInMillis: Long = 0
  private[snowflake] var testCaseName: String = _
  private[snowflake] var testStatus: String = "NotStarted"
  private[snowflake] var reason: String = "no reason"

  def build(): ClusterTestResult = {
    new ClusterTestResult(this)
  }

  def withTestType(testType: String): ClusterTestResultBuilder = {
    this.testType = testType
    this
  }
  def withGithubRunId(jobStartTime: String): ClusterTestResultBuilder = {
    this.githubRunId = jobStartTime
    this
  }
  def withCommitID(commitID: String): ClusterTestResultBuilder = {
    this.commitID = commitID
    this
  }
  def withStartTimeInMill(startTimeInMillis: Long): ClusterTestResultBuilder = {
    this.startTimeInMillis = startTimeInMillis
    this
  }
  def withEndTimeInMill(endTimeInMillis: Long): ClusterTestResultBuilder = {
    this.endTimeInMillis = endTimeInMillis
    this
  }
  def withTestCaseName(testCaseName: String): ClusterTestResultBuilder = {
    this.testCaseName = testCaseName
    this
  }
  def withTestStatus(testStatus: String): ClusterTestResultBuilder = {
    this.testStatus = testStatus
    this
  }
  def withReason(reason: String): ClusterTestResultBuilder = {
    this.reason = reason
    this
  }
}
