/*
 * Copyright 2015-2016 Snowflake Computing
 * Copyright 2015 Databricks
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

import org.scalatest.FunSuite
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import net.snowflake.spark.snowflake.FilterPushdown._

class FilterPushdownSuite extends FunSuite {

  // Fake filter class to detect fallback logic
  test("buildWhereClause with empty list of filters") {
    assert(buildWhereClause(StructType(Nil), Seq.empty) === "")
  }

  test("buildWhereClause with string literals that contain Unicode characters") {
    // scalastyle:off
    val whereClause = buildWhereClause(testSchema, Seq(EqualTo("test_string", "Unicode's樂趣")))
    // Here, the apostrophe in the string needs to be replaced with two single quotes, ''
    assert(whereClause === """WHERE "test_string" = 'Unicode''s樂趣'""")
    // scalastyle:on
  }

  test("buildWhereClause with multiple filters") {
    val filters = Seq(
      EqualTo("test_bool", true),
      // scalastyle:off
      EqualTo("test_string", "Unicode是樂趣"),
      // scalastyle:on
      GreaterThan("test_double", 1000.0),
      LessThan("test_double", Double.MaxValue),
      GreaterThanOrEqual("test_float", 1.0f),
      LessThanOrEqual("test_int", 43),
      StringStartsWith("test_string", "prefix"),
      StringEndsWith("test_string", "suffix"),
      StringContains("test_string", "infix"),
      In("test_int", Array(2,3,4))
    )
    val whereClause = buildWhereClause(testSchema, filters)
    // scalastyle:off
    val expectedWhereClause =
      """
        |WHERE "test_bool" = true
        |AND "test_string" = 'Unicode是樂趣'
        |AND "test_double" > 1000.0
        |AND "test_double" < 1.7976931348623157E308
        |AND "test_float" >= 1.0
        |AND "test_int" <= 43
        |AND STARTSWITH("test_string", 'prefix')
        |AND ENDSWITH("test_string", 'suffix')
        |AND CONTAINS("test_string", 'infix')
        |AND ("test_int" IN (2, 3, 4))
        |""".stripMargin.lines.mkString(" ").trim
    // scalastyle:on
    assert(whereClause === expectedWhereClause)
  }

  test("buildWhereClause for different datatypes") {
    val filters = Seq(
      EqualTo("test_bool", true),
      // scalastyle:off
      EqualTo("test_string", "Unicode是樂趣"),
      // scalastyle:on
      EqualTo("test_double", 1000.0),
      EqualTo("test_float", 1.0f),
      EqualTo("test_int", 43),
      EqualTo("test_date", TestUtils.toDate(2013, 3, 5)),
      EqualTo("test_timestamp", TestUtils.toTimestamp(2013, 3, 5, 12, 1, 2, 987)),
      StringStartsWith("test_timestamp", "2013-04-05")
    )
    val whereClause = buildWhereClause(testSchema, filters)
    // scalastyle:off
    val expectedWhereClause =
    """
      |WHERE "test_bool" = true
      |AND "test_string" = 'Unicode是樂趣'
      |AND "test_double" = 1000.0
      |AND "test_float" = 1.0
      |AND "test_int" = 43
      |AND "test_date" = '2013-04-05'::DATE
      |AND "test_timestamp" = '2013-04-05 12:01:02.987'::TIMESTAMP(3)
      |AND STARTSWITH("test_timestamp", '2013-04-05')
    """.stripMargin.lines.mkString(" ").trim
    // scalastyle:on
    assert(whereClause === expectedWhereClause)
  }

  test("buildWhereClause for unknown attributes") {
    val filters = Seq(
      EqualTo("test_bool", true),
      EqualTo("test_foo", true),  // should be removed
      EqualTo("test_int", 7)
    )
    val whereClause = buildWhereClause(testSchema, filters)
    // scalastyle:off
    val expectedWhereClause =
    """
      |WHERE "test_bool" = true
      |AND "test_int" = 7
    """.stripMargin.lines.mkString(" ").trim
    // scalastyle:on
    assert(whereClause === expectedWhereClause)
  }

  test("buildWhereClause for logical constructs") {
    val filters = Seq(
      Or(
        EqualTo("test_bool", true),
        EqualTo("test_int", 7)),
      Or(
        Not(IsNull("test_int")),
        IsNotNull("test_float")),
      Or(
        EqualTo("test_bool", true),
        And(
          EqualTo("test_float", 3.13),
          Not(EqualTo("test_int", 7))))
    )
    val whereClause = buildWhereClause(testSchema, filters)
    // scalastyle:off
    val expectedWhereClause =
    """
      |WHERE (("test_bool" = true) OR ("test_int" = 7))
      |AND (((NOT (("test_int" IS NULL)))) OR (("test_float" IS NOT NULL)))
      |AND (("test_bool" = true) OR ((("test_float" = 3.13) AND ((NOT ("test_int" = 7))))))
    """.stripMargin.lines.mkString(" ").trim
    // scalastyle:on
    assert(whereClause === expectedWhereClause)
  }

  private val testSchema: StructType = StructType(Seq(
    StructField("test_byte", ByteType),
    StructField("test_bool", BooleanType),
    StructField("test_date", DateType),
    StructField("test_double", DoubleType),
    StructField("test_float", FloatType),
    StructField("test_int", IntegerType),
    StructField("test_long", LongType),
    StructField("test_short", ShortType),
    StructField("test_string", StringType),
    StructField("test_timestamp", TimestampType)))
}