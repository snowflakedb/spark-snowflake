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

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.variant.VariantExpressionEvalUtils
import org.apache.spark.sql.types.{DataType, VariantType}
import org.apache.spark.unsafe.types.UTF8String

/** Spark 4.x: map Snowflake VARIANT columns to Spark [[VariantType]] and JSON text to variant values. */
private[snowflake] object SparkVariantSupport {

  def isSparkVariantType(dt: DataType): Boolean = dt == VariantType

  def catalystTypeForSnowflakeJdbcColumn(
      _sqlType: Int,
      columnTypeName: String
  ): Option[DataType] = {
    if (columnTypeName == null) None
    else {
      val tn = columnTypeName.trim
      if (tn.equalsIgnoreCase("VARIANT")) Some(VariantType)
      else None
    }
  }

  /**
    * Turn a JDBC/unload string for a Snowflake VARIANT column into a Spark [[VariantType]] value.
    *
    * @param jdbcValue   value from `ResultSet#getObject` or a CSV unload cell (JSON text). May be
    *                    Java `null` defensively even when `wasNull` was false.
    * @param nullable    when true, blank/whitespace-only strings are treated as SQL `NULL` (no
    *                    value). JSON text `"null"` is still parsed to a variant JSON-null value.
    */
  def materializeNonNullJdbcVariant(
      jdbcValue: AnyRef,
      isInternalRow: Boolean,
      nullable: Boolean = true
  ): AnyRef = {
    if (jdbcValue == null) return null
    val raw = jdbcValue match {
      case s: String => s
      case other => String.valueOf(other)
    }
    val json = raw.trim
    if (json.isEmpty) {
      if (nullable) return null
      else
        throw new IllegalArgumentException(
          "Empty or whitespace-only string cannot be converted to VARIANT for a non-nullable column"
        )
    }
    val vv =
      VariantExpressionEvalUtils.parseJson(UTF8String.fromString(json), false, true)
    if (isInternalRow) vv
    else CatalystTypeConverters.convertToScala(vv, VariantType).asInstanceOf[AnyRef]
  }
}
