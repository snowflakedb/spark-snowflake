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

import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.types.DataType

/**
  * Spark 4.0+ checks [[CreatableRelationProvider.supportsDataType]] before save; the default
  * implementation rejects [[org.apache.spark.sql.types.VariantType]]. Snowflake accepts VARIANT
  * columns via the existing JSON (and compatible) write paths.
  */
private[snowflake] trait CreatableRelationVariantSupport extends CreatableRelationProvider {

  abstract override def supportsDataType(dt: DataType): Boolean =
    SparkVariantSupport.isSparkVariantType(dt) || super.supportsDataType(dt)
}
