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

import java.util.Arrays

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{StructField, StructType, VariantType}
import org.apache.spark.types.variant.Variant
import org.apache.spark.unsafe.types.VariantVal

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

/**
  * Null / missing semantics for Spark 4.x [[VariantType]] against Snowflake VARIANT:
  * SQL NULL vs JSON `null`, nested object fields, array elements, JDBC vs COPY unload.
  * Compiled only for Spark 4+ (see src/it/4.x).
  */
class NativeSparkVariantNullHandlingITSuite extends IntegrationSuiteBase {

  private val table = s"SPARK_IT_NATIVE_VARIANT_NULL_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()
    jdbcUpdate(
      s"""create or replace table $table (
         |  id integer,
         |  v_sql_null variant,
         |  v_json_null variant,
         |  v_nested variant,
         |  v_scalar variant,
         |  arr variant
         |)""".stripMargin
    )
    jdbcUpdate(
      s"""insert into $table (id, v_sql_null, v_json_null, v_nested, v_scalar, arr) select
         |  1,
         |  null,
         |  parse_json('null'),
         |  parse_json('{"a":null,"b":2}'),
         |  parse_json('"ok"'),
         |  parse_json('[10, null, 30]')""".stripMargin
    )
    jdbcUpdate(
      s"""insert into $table (id, v_sql_null, v_json_null, v_nested, v_scalar, arr) select
         |  2,
         |  parse_json('{"only":"row2"}'),
         |  null,
         |  null,
         |  null,
         |  parse_json('[]')""".stripMargin
    )
  }

  override def afterAll(): Unit = {
    jdbcUpdate(s"drop table if exists $table")
    super.afterAll()
  }

  private def readDf(extraOpts: Map[String, String] = Map.empty) =
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable ++ extraOpts)
      .option("dbtable", table)
      .load()

  private def fieldIndex(row: Row, name: String): Int = {
    val idx = row.schema.fieldNames.indexWhere(_.equalsIgnoreCase(name))
    assert(idx >= 0, s"column $name not in ${row.schema.fieldNames.mkString(",")}")
    idx
  }

  private def rowVariantAt(row: Row, idx: Int): Variant =
    row.get(idx) match {
      case v: Variant => v
      case vv: VariantVal => new Variant(vv.getValue, vv.getMetadata)
      case other =>
        fail(s"expected Variant or VariantVal at index $idx, got ${Option(other).map(_.getClass)}")
    }

  /** Spark [[Variant]] / [[VariantVal]] are not Java-serializable; avoid putting them in RDD rows. */
  private def variantPayload(any: Any): (Array[Byte], Array[Byte]) =
    any match {
      case vv: VariantVal => (vv.getValue, vv.getMetadata)
      case v: Variant => (v.getValue, v.getMetadata)
      case other =>
        fail(s"expected Variant or VariantVal, got ${Option(other).map(_.getClass)}")
    }

  /** Value buffer for Spark `parse_json('null')` (metadata can differ for the same token across sources). */
  private lazy val refJsonNullValueBytes: Array[Byte] = {
    val cell = sparkSession.sql("select parse_json('null') as v").collect()(0).get(0)
    variantPayload(cell)._1.clone()
  }

  /** Compare value payload to Spark SQL `parse_json('null')` (on-heap; no RDD serialization). */
  private def assertTopLevelJsonNull(v: Any): Unit = {
    val got = variantPayload(v)
    assert(
      Arrays.equals(got._1, refJsonNullValueBytes),
      "expected variant VALUE bytes to match Spark parse_json('null')"
    )
  }

  test("JDBC read: SQL NULL vs JSON null vs nested JSON null (inferred VariantType)") {
    // The COPY UNLOAD → JSON path cannot distinguish SQL NULL from VARIANT JSON `null`
    // (both collapse to JSON null in the unloaded file); there is a dedicated
    // "COPY unload" test above that covers that path. This test is JDBC-only.
    if (!params.useCopyUnload) {
      val rows = readDf().filter("id = 1").collect()
      assert(rows.length === 1)
      val r = rows(0)

      assert(r.isNullAt(fieldIndex(r, "V_SQL_NULL")))

      val vJsonNullIdx = fieldIndex(r, "V_JSON_NULL")
      assert(!r.isNullAt(vJsonNullIdx))
      assertTopLevelJsonNull(rowVariantAt(r, vJsonNullIdx))

      val nested = rowVariantAt(r, fieldIndex(r, "V_NESTED"))
      assert(nested.getFieldByKey("b").getLong === 2L)
      assertTopLevelJsonNull(nested.getFieldByKey("a"))

      assert(rowVariantAt(r, fieldIndex(r, "V_SCALAR")).getString === "ok")

      val arr = rowVariantAt(r, fieldIndex(r, "ARR"))
      assert(arr.arraySize() === 3)
      assert(arr.getElementAtIndex(0).getLong === 10L)
      assertTopLevelJsonNull(arr.getElementAtIndex(1))
      assert(arr.getElementAtIndex(2).getLong === 30L)
    }
  }

  test("JDBC read: row mixing non-null and null variant columns (id=2)") {
    val rows = readDf().filter("id = 2").collect()
    assert(rows.length === 1)
    val r = rows(0)

    assert(!r.isNullAt(fieldIndex(r, "V_SQL_NULL")))
    assert(rowVariantAt(r, fieldIndex(r, "V_SQL_NULL")).getFieldByKey("only").getString === "row2")

    assert(r.isNullAt(fieldIndex(r, "V_JSON_NULL")))
    assert(r.isNullAt(fieldIndex(r, "V_NESTED")))
    assert(r.isNullAt(fieldIndex(r, "V_SCALAR")))

    val arr = rowVariantAt(r, fieldIndex(r, "ARR"))
    assert(arr.arraySize() === 0)
  }

  test("COPY unload: nested JSON null and variant scalars when use_copy_unload is true") {
    // GCS does not support the COPY UNLOAD → client-download path used when
    // `use_copy_unload=true`; `InternalGcsStorage.download` throws. The CI matrix
    // excludes `use_copy_unload=true` on GCP globally, but this test forces it on,
    // so skip it when running against a GCP test account.
    if (!"gcp".equals(System.getenv(SNOWFLAKE_TEST_ACCOUNT))) {
      val rows =
        readDf(Map("use_copy_unload" -> "true"))
          .filter("id = 1")
          .collect()
      assert(rows.length === 1)
      val r = rows(0)

      assert(r.isNullAt(fieldIndex(r, "V_SQL_NULL")))
      val vJsonNullIdx = fieldIndex(r, "V_JSON_NULL")
      // Top-level VARIANT JSON `null` may round-trip as SQL NULL in COPY→JSON→parse; JDBC keeps it distinct.
      if (!r.isNullAt(vJsonNullIdx)) {
        assertTopLevelJsonNull(rowVariantAt(r, vJsonNullIdx))
      }

      val nested = rowVariantAt(r, fieldIndex(r, "V_NESTED"))
      assertTopLevelJsonNull(nested.getFieldByKey("a"))
      assert(nested.getFieldByKey("b").getLong === 2L)

      assert(rowVariantAt(r, fieldIndex(r, "V_SCALAR")).getString === "ok")
      val arr = rowVariantAt(r, fieldIndex(r, "ARR"))
      assert(arr.arraySize() === 3)
      assert(arr.getElementAtIndex(0).getLong === 10L)
      assertTopLevelJsonNull(arr.getElementAtIndex(1))
      assert(arr.getElementAtIndex(2).getLong === 30L)
    }
  }

  test("explicit schema: nullable VariantType columns accept SQL NULL") {
    // See note above: COPY UNLOAD collapses SQL NULL and VARIANT JSON `null` in the
    // unloaded JSON, so this assertion only applies to the JDBC result-set path.
    if (!params.useCopyUnload) {
      import org.apache.spark.sql.types.IntegerType

      val explicitSchema = StructType(
        Seq(
          StructField("ID", IntegerType, nullable = false),
          StructField("V_SQL_NULL", VariantType, nullable = true),
          StructField("V_JSON_NULL", VariantType, nullable = true),
          StructField("V_NESTED", VariantType, nullable = true),
          StructField("V_SCALAR", VariantType, nullable = true),
          StructField("ARR", VariantType, nullable = true)
        )
      )

      val r =
        sparkSession.read
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(connectorOptionsNoTable)
          .option("dbtable", table)
          .schema(explicitSchema)
          .load()
          .filter("id = 1")
          .collect()(0)
      assert(r.isNullAt(fieldIndex(r, "V_SQL_NULL")))
      assertTopLevelJsonNull(rowVariantAt(r, fieldIndex(r, "V_JSON_NULL")))
    }
  }

  test("round-trip write/read: SQL NULL and JSON null in VariantType columns") {
    // See note above: COPY UNLOAD collapses SQL NULL and VARIANT JSON `null` on the
    // unload side, so the read assertion can't distinguish them. JDBC-only.
    if (!params.useCopyUnload) {
      val tw = s"SPARK_IT_NATIVE_VARIANT_NULL_RT_$randomSuffix"
      try {
        val src = sparkSession.sql(
          """select 1 as id,
            |       cast(null as variant) as v_sql,
            |       parse_json('null') as v_json""".stripMargin
        )

        src.write
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(connectorOptionsNoTable)
          .option(Parameters.PARAM_USE_JSON_IN_STRUCTURED_DATA, "true")
          .option("dbtable", tw)
          .mode(SaveMode.Overwrite)
          .save()

        val back = sparkSession.read
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(connectorOptionsNoTable)
          .option("dbtable", tw)
          .load()
          .collect()(0)

        assert(back.isNullAt(fieldIndex(back, "V_SQL")))
        assertTopLevelJsonNull(rowVariantAt(back, fieldIndex(back, "V_JSON")))
      } finally {
        jdbcUpdate(s"drop table if exists $tw")
      }
    }
  }
}
