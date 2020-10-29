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

abstract class BaseClusterTestResultBuilder() {

  protected[snowflake] var startTimeInMillis: Long = 0
  protected[snowflake] var endTimeInMillis: Long = 0
  protected[snowflake] var testCaseName: String = _
  protected[snowflake] var testStatus: String = "NotStarted"
  protected[snowflake] var reason: String = "no reason"

  def build(): ClusterTestResult;

  def withStartTimeInMill(startTimeInMillis: Long): BaseClusterTestResultBuilder
  def withEndTimeInMill(endTimeInMillis: Long): BaseClusterTestResultBuilder
  def withTestCaseName(testCaseName: String): BaseClusterTestResultBuilder
  def withTestStatus(testStatus: String): BaseClusterTestResultBuilder
  def withReason(reason: String): BaseClusterTestResultBuilder
  def withNewSubTestResult(
      startTime: Long,
      endTime: Long,
      status: String,
      reason: String): BaseClusterTestResultBuilder
}
