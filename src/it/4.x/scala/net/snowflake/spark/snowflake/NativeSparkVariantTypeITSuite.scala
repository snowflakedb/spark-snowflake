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

import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types._
import org.apache.spark.types.variant.Variant
import org.apache.spark.unsafe.types.VariantVal

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

/**
  * Integration coverage for Spark 4.x native [[VariantType]] when reading/writing Snowflake
  * VARIANT columns (JDBC metadata, JSON unload when enabled, and JSON write path).
  * Only compiled for Spark 4+ (see src/it/4.x).
  */
class NativeSparkVariantTypeITSuite extends IntegrationSuiteBase {

  private val tableRead = s"SPARK_IT_NATIVE_VARIANT_READ_$randomSuffix"
  private val tableWrite = s"SPARK_IT_NATIVE_VARIANT_WRITE_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()
    jdbcUpdate(
      s"""create or replace table $tableRead (
         |  v variant,
         |  arr variant,
         |  n integer
         |)""".stripMargin
    )
    jdbcUpdate(
      s"""insert into $tableRead (v, arr, n) select
         |  parse_json('{"x":42,"msg":"hi"}'),
         |  parse_json('[1,2,3]'),
         |  7""".stripMargin
    )
    jdbcUpdate(
      s"""insert into $tableRead (v, arr, n) select
         |  null,
         |  parse_json('[]'),
         |  0""".stripMargin
    )
  }

  override def afterAll(): Unit = {
    jdbcUpdate(s"drop table if exists $tableRead")
    jdbcUpdate(s"drop table if exists $tableWrite")
    super.afterAll()
  }

  test("inferred schema maps Snowflake VARIANT to Spark VariantType (JDBC read)") {
    val df =
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", tableRead)
        .load()

    assert(df.schema.map(_.dataType).contains(VariantType))
    val vField = df.schema.fields.find(_.dataType == VariantType).map(_.name.toUpperCase)
    assert(vField.contains("V"))
  }

  test("JDBC read returns Spark Variant values for VARIANT columns") {
    val rows =
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", tableRead)
        .load()
        .collect()

    assert(rows.length === 2)

    val v0 = rowVariantAt(rows(0), fieldIndex(rows(0), "V"))
    assert(v0.getFieldByKey("x").getLong === 42L)
    assert(v0.getFieldByKey("msg").getString === "hi")

    val arr0 = rowVariantAt(rows(0), fieldIndex(rows(0), "ARR"))
    assert(arr0.arraySize() === 3)
    assert(arr0.getElementAtIndex(0).getLong === 1L)
    assert(arr0.getElementAtIndex(2).getLong === 3L)

    assert(numericCellAsLong(rows(0), fieldIndex(rows(0), "N")) === 7L)

    assert(rows(1).isNullAt(fieldIndex(rows(1), "V")))
    assert(numericCellAsLong(rows(1), fieldIndex(rows(1), "N")) === 0L)
  }

  test("JSON unload path maps VARIANT to VariantType when use_copy_unload is true") {
    // GCS does not support the COPY UNLOAD → client-download path used when
    // `use_copy_unload=true`; `InternalGcsStorage.download` throws. The CI matrix
    // excludes `use_copy_unload=true` on GCP globally, but this test forces it on,
    // so skip it when running against a GCP test account.
    if (!"gcp".equals(System.getenv(SNOWFLAKE_TEST_ACCOUNT))) {
      val opts = connectorOptionsNoTable + ("use_copy_unload" -> "true")
      val df =
        sparkSession.read
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(opts)
          .option("dbtable", tableRead)
          .load()

      assert(df.schema.map(_.dataType).contains(VariantType))
      val v = rowVariantAt(df.select("V").first(), 0)
      assert(v.getFieldByKey("x").getLong === 42L)
    }
  }

  test("round-trip write DataFrame with VariantType column and read back") {
    val src =
      sparkSession.sql("""select 100 as id, parse_json('{"k":"hello","n":-1}') as v""")

    val vField = src.schema.fields.find(_.name.equalsIgnoreCase("v")).get
    assert(vField.dataType === VariantType)

    // Variant-containing saves default to Parquet unless JSON is enabled; ParquetUtils does not
    // yet map VariantType (see ParquetUtils.convertFieldToAvro).
    src.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_USE_JSON_IN_STRUCTURED_DATA, "true")
      .option("dbtable", tableWrite)
      .mode(SaveMode.Overwrite)
      .save()

    val back =
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", tableWrite)
        .load()

    assert(back.schema.map(_.dataType).contains(VariantType))

    val row = findRowWithId(back.collect(), 100)
    val vCol = rowVariantAt(row, fieldIndex(row, "v"))
    assert(vCol.getFieldByKey("k").getString === "hello")
    assert(vCol.getFieldByKey("n").getLong === -1L)
  }

  /**
    * Connector scans populate [[VariantVal]] in [[Row]]s; [[org.apache.spark.sql.catalyst.CatalystTypeConverters]]
    * still surfaces [[VariantVal]] for [[VariantType]], so wrap bytes as [[Variant]] for assertions.
    */
  private def rowVariantAt(row: Row, idx: Int): Variant =
    row.get(idx) match {
      case v: Variant => v
      case vv: VariantVal => new Variant(vv.getValue, vv.getMetadata)
      case other =>
        fail(s"expected Variant or VariantVal at index $idx, got ${Option(other).map(_.getClass)}")
    }

  private def fieldIndex(row: Row, name: String): Int = {
    val idx = row.schema.fieldNames.indexWhere(_.equalsIgnoreCase(name))
    assert(idx >= 0, s"column $name not in ${row.schema.fieldNames.mkString(",")}")
    idx
  }

  private def findRowWithId(rows: Array[Row], id: Int): Row = {
    val found = rows.find { r =>
      val i = r.schema.fieldNames.indexWhere(_.equalsIgnoreCase("id"))
      i >= 0 && numericCellAsLong(r, i) == id.toLong
    }
    assert(found.isDefined, s"no row with id=$id in ${rows.length} row(s)")
    found.get
  }

  /** Snowflake INTEGER/NUMBER may appear as int, long, or decimal in Spark. */
  private def numericCellAsLong(r: Row, idx: Int): Long =
    r.schema(idx).dataType match {
      case IntegerType => r.getInt(idx).toLong
      case LongType => r.getLong(idx)
      case ShortType => r.getShort(idx).toLong
      case _: DecimalType => r.getDecimal(idx).longValue()
      case ByteType => r.getByte(idx).toLong
      case _ =>
        r.get(idx) match {
          case x: java.lang.Integer => x.longValue()
          case x: java.lang.Long => x.longValue()
          case x: java.math.BigDecimal => x.longValue()
          case other =>
            fail(s"unsupported id column JVM type: ${other.getClass}")
        }
    }
}
