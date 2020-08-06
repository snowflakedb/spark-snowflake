/*
 * Copyright 2020 Snowflake Computing
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

package net.snowflake.spark.snowflake.pushdowns.querygeneration

import java.net.URI

import net.snowflake.spark.snowflake.Utils
import org.scalatest.{FunSuite, Matchers}
import net.snowflake.spark.snowflake.Utils
import net.snowflake.spark.snowflake.pushdowns.querygeneration._

/**
  * Unit tests for helper functions
  */
class PushdownSuite01 extends FunSuite with Matchers {

  test("test DateStatement.getSnowflakeTimestampFormat") {
    assert(DateStatement.getSnowflakeTimestampFormat(
      "yyyy-MM-dd HH:mm:ss").get.equals("yyyy-MM-dd HH:MI:ss"))
    assert(DateStatement.getSnowflakeTimestampFormat(
      "yyyy/MM/dd HH:mm:ss.SSS").get.equals("yyyy/MM/dd HH:MI:ss.FF3"))
    assert(DateStatement.getSnowflakeTimestampFormat(
      "yyyy-MM-dd'T'HH:mm:ss'Z'").get.equals("yyyy-MM-dd\"T\"HH:MI:ss\"Z\""))
    assert(DateStatement.getSnowflakeTimestampFormat(
      "'year: 'yyyy'month: 'MM'day'dd").get.equals("\"year: \"yyyy\"month: \"MM\"day\"dd"))

    // negative test
    assert(DateStatement.getSnowflakeTimestampFormat(
      "G yyyy-MM-dd HH:mm:ss").isEmpty)
    assert(DateStatement.getSnowflakeTimestampFormat(
      "yyyy-MM-dd'T'HH:mm:ss'Z").isEmpty)
  }
}

