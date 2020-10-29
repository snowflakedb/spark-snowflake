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

class GithubActionsClusterTestResultBuilder() extends BaseClusterTestResultBuilder {

  protected[snowflake] var testType = "Scala" // There are Scala test and Python test.
  protected[snowflake] var commitID: String = _
  protected[snowflake] var githubRunId: String = _

  override def build(): ClusterTestResult = {
    new GithubActionsClusterTestResult(this)
  }

  override def withStartTimeInMill(
      startTimeInMillis: Long): GithubActionsClusterTestResultBuilder = {
    this.startTimeInMillis = startTimeInMillis
    this
  }

  override def withEndTimeInMill(endTimeInMillis: Long): GithubActionsClusterTestResultBuilder = {
    this.endTimeInMillis = endTimeInMillis
    this
  }
  override def withTestCaseName(testCaseName: String): GithubActionsClusterTestResultBuilder = {
    this.testCaseName = testCaseName
    this
  }
  override def withTestStatus(testStatus: String): GithubActionsClusterTestResultBuilder = {
    this.testStatus = testStatus
    this
  }
  override def withReason(reason: String): GithubActionsClusterTestResultBuilder = {
    this.reason = reason
    this
  }

  def withTestType(testType: String): GithubActionsClusterTestResultBuilder = {
    this.testType = testType
    this
  }
  def withGithubRunId(jobStartTime: String): GithubActionsClusterTestResultBuilder = {
    this.githubRunId = jobStartTime
    this
  }
  def withCommitID(commitID: String): GithubActionsClusterTestResultBuilder = {
    this.commitID = commitID
    this
  }
  // This does nothing for now when running on Github.
  override def withNewSubTestResult(
      startTime: Long,
      endTime: Long,
      status: String,
      reason: String): GithubActionsClusterTestResultBuilder = this
}
