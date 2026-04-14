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

package net.snowflake.spark.snowflake.io

import scala.collection.JavaConverters._

import net.snowflake.spark.snowflake.{Parameters, SparkVariantSupport}
import org.apache.avro.Schema
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType, VariantType}
import org.scalatest.funsuite.AnyFunSuite

/** Spark 4.x: [[ParquetUtils]] maps [[VariantType]] to Avro string + JSON cell text. */
class ParquetVariantWriteSuite extends AnyFunSuite {

  test("convertStructToAvro maps VariantType to string") {
    val schema = StructType(Seq(StructField("v", VariantType, nullable = true)))
    val avro = ParquetUtils.convertStructToAvro(schema)
    val fieldSchema = avro.getField("v").schema()
    val nonNull =
      if (fieldSchema.getType == Schema.Type.UNION)
        fieldSchema.getTypes.asScala.find(_.getType != Schema.Type.NULL).get
      else fieldSchema
    assert(nonNull.getType === Schema.Type.STRING)
  }

  test("rowToAvroRecord writes Variant column as JSON string") {
    val schema = StructType(Seq(StructField("v", VariantType, nullable = false)))
    val avro = ParquetUtils.convertStructToAvro(schema)
    val params = Parameters.mergeParameters(
      Map(
        Parameters.PARAM_SF_URL -> "test.snowflakecomputing.com:443",
        Parameters.PARAM_SF_USER -> "user",
        Parameters.PARAM_SF_PASSWORD -> "p",
        Parameters.PARAM_SF_DBTABLE -> "t"
      )
    )
    val variantCell =
      SparkVariantSupport
        .materializeNonNullJdbcVariant("""{"x":99}""", isInternalRow = false, nullable = false)
        .asInstanceOf[AnyRef]
    val row = Row(variantCell)
    val rec = ParquetUtils.rowToAvroRecord(row, avro, schema, params)
    val json = rec.get("v").asInstanceOf[String]
    assert(json.contains("\"x\"") && json.contains("99"))
  }
}
