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

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class SnowflakeFallbackCatalogIcebergTest extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    val warehouse = Files.createTempDirectory("fallback-iceberg").toUri.toString
    // spark.sql.extensions is static: stop any session leaked by an earlier suite in this JVM
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())
    spark = SparkSession.builder()
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      // Iceberg probes optional classes on executors; don't route misses to the artifact server
      .config("spark.sql.artifact.isolation.enabled", "false")
      // Only Iceberg's extensions, as for Iceberg's own SparkCatalog
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.fb", classOf[SnowflakeFallbackCatalog].getName)
      .config("spark.sql.catalog.fb.type", "hadoop")
      .config("spark.sql.catalog.fb.warehouse", warehouse)
      .getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS fb.db")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def count(sql: String): Long = spark.sql(sql).collect().head.getLong(0)

  private def refs(table: String): Set[String] =
    spark.sql(s"SELECT name FROM $table.refs").collect().map(_.getString(0)).toSet

  test("branch lifecycle with WAP write and fast_forward through the fallback catalog") {
    spark.sql("CREATE TABLE fb.db.wap (id INT) USING iceberg")
    spark.sql("INSERT INTO fb.db.wap VALUES (1), (2)")

    spark.sql("ALTER TABLE fb.db.wap CREATE BRANCH audit")
    assert(refs("fb.db.wap") == Set("main", "audit"))

    spark.sql("MERGE INTO fb.db.wap.branch_audit t USING (SELECT 3 AS id) s ON t.id = s.id " +
      "WHEN NOT MATCHED THEN INSERT *")
    assert(count("SELECT count(*) FROM fb.db.wap.branch_audit") == 3)
    assert(count("SELECT count(*) FROM fb.db.wap VERSION AS OF 'audit'") == 3)
    assert(count("SELECT count(*) FROM fb.db.wap") == 2)

    spark.sql("CALL fb.system.fast_forward('db.wap', 'main', 'audit')")
    assert(count("SELECT count(*) FROM fb.db.wap") == 3)

    spark.sql("ALTER TABLE fb.db.wap DROP BRANCH audit")
    assert(refs("fb.db.wap") == Set("main"))
  }

  test("tag lifecycle through the fallback catalog") {
    spark.sql("CREATE TABLE fb.db.tagged (id INT) USING iceberg")
    spark.sql("INSERT INTO fb.db.tagged VALUES (1)")

    spark.sql("ALTER TABLE fb.db.tagged CREATE TAG v1")
    spark.sql("INSERT INTO fb.db.tagged VALUES (2)")
    assert(count("SELECT count(*) FROM fb.db.tagged VERSION AS OF 'v1'") == 1)

    spark.sql("ALTER TABLE fb.db.tagged DROP TAG v1")
    assert(refs("fb.db.tagged") == Set("main"))
  }
}
