/*
 * Copyright 2015-2025 Snowflake Computing
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
package org.apache.spark.sql.snowflake.catalog

import java.nio.file.Files

import org.apache.iceberg.exceptions.ForbiddenException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, V1Table}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/** Time travel on a table that Horizon 403s (simulated by ForbiddingFallbackCatalog). */
class SnowflakeFallbackCatalogTimeTravelTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private var snapshotId: Long = _

  override def beforeAll(): Unit = {
    // spark.sql.extensions is static: stop any session leaked by an earlier suite in this JVM
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())
    val path = Files.createTempDirectory("fallback-time-travel").toUri.toString
    def fallback(b: SparkSession.Builder, name: String) = b
      .config(s"spark.sql.catalog.$name", classOf[ForbiddingFallbackCatalog].getName)
      .config(s"spark.sql.catalog.$name.hadoop-warehouse", path)
      .config(s"spark.sql.catalog.$name.forbidden-tables", "db.secret")
      .config(s"spark.sql.catalog.$name.cache-enabled", "false")
    val builder = SparkSession.builder()
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.artifact.isolation.enabled", "false")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.raw", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.raw.type", "hadoop")
      .config("spark.sql.catalog.raw.warehouse", path)
    spark = fallback(fallback(builder, "fb").config("spark.sql.catalog.fb.warehouse", "MYDB"),
      "fbnowh").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS raw.db")
    spark.sql("CREATE TABLE raw.db.secret (id INT) USING iceberg")
    spark.sql("INSERT INTO raw.db.secret VALUES (1)")
    spark.sql("ALTER TABLE raw.db.secret CREATE BRANCH x")
    spark.sql("CREATE TABLE raw.db.open (id INT) USING iceberg")
    snapshotId = spark.sql("SELECT snapshot_id FROM raw.db.secret.snapshots").head.getLong(0)
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def catalog(name: String = "fb"): TableCatalog =
    spark.sessionState.catalogManager.catalog(name).asInstanceOf[TableCatalog]

  private val secret = Identifier.of(Array("db"), "secret")

  private def snowflakeTable(table: AnyRef): V1Table = {
    assert(table.isInstanceOf[V1Table], s"expected a Snowflake V1Table, got $table")
    val v1 = table.asInstanceOf[V1Table]
    assert(v1.catalogTable.provider.contains(classOf[FallbackDefaultSource].getName))
    v1
  }

  private def queryOf(table: AnyRef): String = {
    val props = snowflakeTable(table).catalogTable.storage.properties
    assert(!props.contains("dbtable"))
    props("query")
  }

  test("plain load falls back to dbtable") {
    val props = snowflakeTable(catalog().loadTable(secret)).catalogTable.storage.properties
    assert(props.get("dbtable").contains("MYDB.db.secret"))
    assert(!props.contains("query"))
  }

  test("VERSION AS OF a snapshot ID falls back with AT(VERSION)") {
    assert(queryOf(catalog().loadTable(secret, snapshotId.toString)) ==
      s"SELECT * FROM MYDB.db.secret AT(VERSION => $snapshotId)")
  }

  test("VERSION AS OF a branch or tag falls back with AT(VERSION_REF)") {
    assert(queryOf(catalog().loadTable(secret, "x")) ==
      "SELECT * FROM MYDB.db.secret AT(VERSION_REF => 'x')")
  }

  test("VERSION AS OF 'main' and branch_main fall back to the current table") {
    for (table <- Seq(catalog().loadTable(secret, "main"),
        catalog().loadTable(Identifier.of(Array("db", "secret"), "branch_main")))) {
      val props = snowflakeTable(table).catalogTable.storage.properties
      assert(props.get("dbtable").contains("MYDB.db.secret"))
      assert(!props.contains("query"))
    }
  }

  test("TIMESTAMP AS OF falls back with AT(TIMESTAMP) in microseconds") {
    assert(queryOf(catalog().loadTable(secret, 1764547200000000L)) ==
      "SELECT * FROM MYDB.db.secret AT(TIMESTAMP => TO_TIMESTAMP_LTZ(1764547200000000, 6))")
  }

  test("time travel without a warehouse uses the namespace as is") {
    assert(queryOf(catalog("fbnowh").loadTable(secret, "x")) ==
      "SELECT * FROM db.secret AT(VERSION_REF => 'x')")
  }

  test("Iceberg selector names on a protected table fall back with the matching AT clause") {
    val cases = Seq(
      "branch_x" -> "AT(BRANCH => 'x')",
      "tag_t" -> "AT(VERSION_REF => 't')",
      s"snapshot_id_$snapshotId" -> s"AT(VERSION => $snapshotId)",
      "at_timestamp_1764547200000" -> "AT(TIMESTAMP => TO_TIMESTAMP_LTZ(1764547200000000, 6))")
    for ((name, clause) <- cases) {
      val table = catalog().loadTable(Identifier.of(Array("db", "secret"), name))
      assert(queryOf(table) == s"SELECT * FROM MYDB.db.secret $clause", name)
    }
  }

  test("selector names of readable or missing tables keep the delegate's error") {
    intercept[org.apache.iceberg.exceptions.ValidationException](
      catalog().loadTable(Identifier.of(Array("db", "open"), "branch_missing")))
    intercept[NoSuchTableException](
      catalog().loadTable(Identifier.of(Array("db", "missing"), "branch_x")))
  }

  test("the refs metadata table of a protected table is read from Snowflake") {
    for ((name, dbtable) <- Seq("fb" -> "MYDB.db.secret", "fbnowh" -> "db.secret")) {
      val table = catalog(name).loadTable(Identifier.of(Array("db", "secret"), "refs"))
      assert(queryOf(table) == IcebergTimeTravel.query(dbtable, IcebergTimeTravel.Refs), name)
    }
    // Its relation is cached apart from the table's
    val refs = catalog().loadTable(Identifier.of(Array("db", "secret"), "refs"))
    assert(snowflakeTable(refs).catalogTable.identifier.catalog != Some("fb"))
  }

  test("other metadata tables of a protected table keep the 403 instead of reading db.secret.x") {
    for (name <- Seq("fb", "fbnowh"); meta <- Seq("snapshots", "history", "entries")) {
      intercept[ForbiddenException](
        catalog(name).loadTable(Identifier.of(Array("db", "secret"), meta)))
    }
  }

  test("each time travel spec gets its own relation cache key") {
    def cacheCatalog(table: AnyRef) = snowflakeTable(table).catalogTable.identifier.catalog
    assert(cacheCatalog(catalog().loadTable(secret)).contains("fb"))
    val atBranch = cacheCatalog(catalog().loadTable(secret, "x"))
    val atSnapshot = cacheCatalog(catalog().loadTable(secret, snapshotId.toString))
    assert(Set(Some("fb"), atBranch, atSnapshot).size == 3)
  }

  test("a plain load drops the cached relation so new read options apply") {
    val key = QualifiedTableName("fb", "db", "secret")
    spark.sessionState.catalog.cacheTable(key, LocalRelation())
    catalog().loadTable(secret)
    assert(spark.sessionState.catalog.getCachedTable(key) == null)
  }
}
