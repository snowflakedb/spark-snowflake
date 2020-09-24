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

import net.snowflake.spark.snowflake.{ConstantString, SnowflakeSQLStatement, Utils}
import org.scalatest.{FunSuite, Matchers}
import net.snowflake.spark.snowflake.pushdowns.querygeneration._
import org.apache.spark.unsafe.types.CalendarInterval

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

  private def isEqualSkipWhite(left: String, right: String): Boolean = {
    left.replaceAll("\\s+", "").equalsIgnoreCase(
      right.replaceAll("\\s+", "")
    )
  }

  test("test DateStatement.generateDateAddStatement") {
    val childStmt = new SnowflakeSQLStatement() + ConstantString("TEST_COLUMN_1")

    val interval1 = new CalendarInterval(1, 2, 123456)
    assert(
      isEqualSkipWhite(
        DateStatement.generateDateAddStatement(true, interval1, childStmt).toString,
        "DATEADD ( 'MONTH', (0 - (1)), DATEADD ( 'DAY', (0 - (2)), DATEADD ( 'MICROSECOND', (0 - (123456)), TEST_COLUMN_1 ) ) )"
      )
    )
    assert(
      isEqualSkipWhite(
        DateStatement.generateDateAddStatement(false, interval1, childStmt).toString,
        "DATEADD ( 'MONTH', 1, DATEADD ( 'DAY', 2, DATEADD ( 'MICROSECOND', 123456, TEST_COLUMN_1 ) ) )"
      )
    )

    val interval2 = new CalendarInterval(1, 0, 123456)
    assert(
      isEqualSkipWhite(
        DateStatement.generateDateAddStatement(true, interval2, childStmt).toString,
        "DATEADD ( 'MONTH', (0 - (1)), DATEADD ( 'MICROSECOND', (0 - (123456)), TEST_COLUMN_1 ) )"
      )
    )
    assert(
      isEqualSkipWhite(
        DateStatement.generateDateAddStatement(false, interval2, childStmt).toString,
        "DATEADD ( 'MONTH', 1, DATEADD ( 'MICROSECOND', 123456, TEST_COLUMN_1 ) )"
      )
    )
  }
}

