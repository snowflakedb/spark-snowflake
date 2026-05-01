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

import org.apache.spark.sql.types.DataType

/** Spark 3.5 build: Spark SQL has no native Variant type; JDBC variant mapping is disabled. */
private[snowflake] object SparkVariantSupport {

  def isSparkVariantType(dt: DataType): Boolean = false

  def catalystTypeForSnowflakeJdbcColumn(
      _sqlType: Int,
      _columnTypeName: String
  ): Option[DataType] = None

  def materializeNonNullJdbcVariant(
      jdbcValue: AnyRef,
      isInternalRow: Boolean,
      nullable: Boolean = true
  ): AnyRef =
    throw new UnsupportedOperationException(
      "Native Spark VariantType is not available in this Spark version"
    )
}
