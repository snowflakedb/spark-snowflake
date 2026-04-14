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

import java.time.ZoneId

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.expressions.variant.VariantExpressionEvalUtils
import org.apache.spark.sql.types.{DataType, VariantType}
import org.apache.spark.types.variant.{Variant, VariantUtil}
import org.apache.spark.unsafe.types.{UTF8String, VariantVal}

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
    val vv0 =
      VariantExpressionEvalUtils.parseJson(UTF8String.fromString(json), false, true)
    val vv = maybeUnwrapSnowflakeStringVariant(vv0)
    if (isInternalRow) vv
    else CatalystTypeConverters.convertToScala(vv, VariantType).asInstanceOf[AnyRef]
  }

  /**
    * Snowflake can materialize VARIANT cells loaded from staged JSON/Parquet as a STRING-typed
    * variant whose string payload is itself JSON (`{...}` / `[...]`). Spark `parseJson` on the
    * outer JDBC text then yields STRING; unwrap one level so object/array fields resolve.
    */
  private def maybeUnwrapSnowflakeStringVariant(vv: VariantVal): VariantVal = {
    val v = new Variant(vv.getValue, vv.getMetadata)
    if (v.getType != VariantUtil.Type.STRING) return vv
    val inner = v.getString.trim
    if (!looksLikeJsonObjectOrArray(inner)) return vv
    try {
      VariantExpressionEvalUtils.parseJson(UTF8String.fromString(inner), false, true)
    } catch {
      case NonFatal(_) => vv
    }
  }

  private def looksLikeJsonObjectOrArray(s: String): Boolean =
    s.nonEmpty && {
      val c = s.charAt(0)
      (c == '{' && s.endsWith("}")) || (c == '[' && s.endsWith("]"))
    }

  /**
    * Serialize a Spark [[VariantType]] cell to JSON text for Parquet/Avro string columns (Snowflake
    * COPY loads VARIANT from JSON strings in staged files).
    */
  def variantToParquetJson(value: AnyRef, zoneId: ZoneId): String = {
    if (value == null) return null
    val variant = value match {
      case v: Variant => v
      case vv: VariantVal => new Variant(vv.getValue, vv.getMetadata)
      case other =>
        throw new IllegalArgumentException(
          s"Expected Variant or VariantVal for VariantType column, got ${Option(other).map(_.getClass).orNull}"
        )
    }
    variant.toJson(zoneId)
  }
}
