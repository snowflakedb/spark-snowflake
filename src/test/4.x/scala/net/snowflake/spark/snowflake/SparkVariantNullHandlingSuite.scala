/*
 * Copyright 2025 Snowflake Computing
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

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  ArrayType,
  MapType,
  StringType,
  StructField,
  StructType,
  VariantType
}
import org.scalatest.funsuite.AnyFunSuite

/** Spark 4.x only: unit coverage for VARIANT null/blank handling in [[SparkVariantSupport]] and [[Conversions]]. */
class SparkVariantNullHandlingSuite extends AnyFunSuite {

  private val mapper = new ObjectMapper()

  test("materializeJdbcVariant: null input yields null") {
    assert(SparkVariantSupport.materializeNonNullJdbcVariant(null, false, true) == null)
  }

  test("materializeJdbcVariant: blank string is SQL null when nullable") {
    assert(SparkVariantSupport.materializeNonNullJdbcVariant("   ", false, true) == null)
    assert(SparkVariantSupport.materializeNonNullJdbcVariant("", false, true) == null)
  }

  test("materializeJdbcVariant: blank string throws when not nullable") {
    intercept[IllegalArgumentException] {
      SparkVariantSupport.materializeNonNullJdbcVariant("", false, false)
    }
  }

  test("materializeJdbcVariant: JSON text null is a variant value, not SQL null") {
    val v = SparkVariantSupport.materializeNonNullJdbcVariant("null", false, true)
    assert(v != null)
  }

  test("jsonStringToRow: Json null for nullable VariantType field yields SQL null") {
    val schema = StructType(
      Seq(StructField("v", VariantType, nullable = true))
    )
    val tree = mapper.readTree("""{"v":null}""")
    val row = Conversions.jsonStringToRow[Row](tree, schema).asInstanceOf[Row]
    assert(row.isNullAt(0))
  }

  test("jsonStringToRow: Json null for non-nullable VariantType throws") {
    val schema = StructType(
      Seq(StructField("v", VariantType, nullable = false))
    )
    val tree = mapper.readTree("""{"v":null}""")
    intercept[IllegalArgumentException] {
      Conversions.jsonStringToRow[Row](tree, schema)
    }
  }

  test("jsonStringToRow: map with null variant value when valueContainsNull") {
    val schema = StructType(
      Seq(
        StructField(
          "m",
          MapType(StringType, VariantType, valueContainsNull = true),
          nullable = false
        )
      )
    )
    val tree = mapper.readTree("""{"m":{"x":1,"y":null}}""")
    val row = Conversions.jsonStringToRow[Row](tree, schema).asInstanceOf[Row]
    val m = row.getMap[String, Any](0)
    assert(m("x") != null)
    assert(m("y") == null)
  }

  test("jsonStringToRow: array of variants with null element when containsNull") {
    val schema = StructType(
      Seq(StructField("a", ArrayType(VariantType, containsNull = true), nullable = false))
    )
    val tree = mapper.readTree("""{"a":[1, null, 3]}""")
    val row = Conversions.jsonStringToRow[Row](tree, schema).asInstanceOf[Row]
    val seq = row.getSeq[Any](0)
    assert(seq.length === 3)
    assert(seq(0) != null)
    assert(seq(1) == null)
    assert(seq(2) != null)
  }
}
