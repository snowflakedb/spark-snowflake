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
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, V1Table}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.mutable.ArrayBuffer

/** Records the statements a SnowflakeSparkTable runs and answers its refs/snapshot queries. */
object RecordingSnowflakeSql extends SnowflakeSql {
  val statements: ArrayBuffer[String] = ArrayBuffer()
  // name -> (type, snapshot id)
  var refs: Map[String, (String, Long)] = Map.empty

  def reset(): Unit = synchronized {
    statements.clear()
    readParams.clear()
    refs = Map("main" -> ("BRANCH", 3L), "x" -> ("BRANCH", 2L), "t0" -> ("TAG", 1L))
  }

  val readParams: ArrayBuffer[Map[String, String]] = ArrayBuffer()

  override def schema(params: Map[String, String]): StructType = synchronized {
    readParams += params
    StructType(Seq(StructField("ID", IntegerType)))
  }

  override def query(sql: String): Seq[Seq[String]] = synchronized {
    if (sql.contains("GET_TABLE_VERSION_REFS")) {
      refs.toSeq.map { case (n, (t, id)) => Seq(n, t, id.toString, null, null, null) }
    } else if (sql.contains("ICEBERG_TABLE_SNAPSHOTS")) {
      Seq(Seq("1", null, "1000", "append"), Seq("2", "1", "2000", "append"),
        Seq("3", "2", "3000", "append"))
    } else {
      statements += sql
      // Snowflake's own effect of the statements the tests check outputs for
      if (sql.contains("MERGE BRANCH 'x'")) refs += "main" -> ("BRANCH", refs("x")._2)
      if (sql.contains("CHERRYPICK")) refs += "main" -> ("BRANCH", 4L)
      Seq(Seq("Statement executed successfully."))
    }
  }
}

class RecordingFallbackCatalog extends ForbiddingFallbackCatalog {
  override protected def newSnowflakeSql(params: Map[String, String]): SnowflakeSql =
    RecordingSnowflakeSql
}

/**
 * Iceberg's own Spark SQL on a table Horizon 403s, with snowflake-fallback-mode=iceberg: branch
 * and tag DDL and procedures reach Snowflake as ALTER ICEBERG TABLE statements.
 */
class SnowflakeSparkTableTest extends AnyFunSuite with BeforeAndAfterAll with BeforeAndAfterEach {

  private var spark: SparkSession = _
  private val alter = "ALTER ICEBERG TABLE MYDB.db.secret"

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())
    val path = Files.createTempDirectory("snowflake-spark-table").toUri.toString
    def fallback(b: SparkSession.Builder, name: String, mode: String) = b
      .config(s"spark.sql.catalog.$name", classOf[RecordingFallbackCatalog].getName)
      .config(s"spark.sql.catalog.$name.hadoop-warehouse", path)
      .config(s"spark.sql.catalog.$name.forbidden-tables", "db.secret")
      .config(s"spark.sql.catalog.$name.cache-enabled", "false")
      .config(s"spark.sql.catalog.$name.warehouse", "MYDB")
      .config(s"spark.sql.catalog.$name.snowflake-fallback-mode", mode)
    val builder = SparkSession.builder()
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.artifact.isolation.enabled", "false")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.raw", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.raw.type", "hadoop")
      .config("spark.sql.catalog.raw.warehouse", path)
    spark = fallback(fallback(builder, "fb", "iceberg"), "fbv1", "v1").getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS raw.db")
    spark.sql("CREATE TABLE raw.db.secret (id INT) USING iceberg")
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  override def beforeEach(): Unit = RecordingSnowflakeSql.reset()

  private def statements: Seq[String] = RecordingSnowflakeSql.statements.toList

  private def catalog(name: String): TableCatalog =
    spark.sessionState.catalogManager.catalog(name).asInstanceOf[TableCatalog]

  test("the iceberg mode returns a Snowflake-backed SparkTable; v1 keeps the V1Table") {
    val secret = Identifier.of(Array("db"), "secret")
    assert(catalog("fb").loadTable(secret).isInstanceOf[SnowflakeSparkTable])
    assert(catalog("fbv1").loadTable(secret).isInstanceOf[V1Table])
    // A readable table is still plain Iceberg
    spark.sql("CREATE TABLE raw.db.open (id INT) USING iceberg")
    assert(catalog("fb").loadTable(Identifier.of(Array("db"), "open")).getClass.getName ==
      "org.apache.iceberg.spark.source.SparkTable")
  }

  test("CREATE BRANCH at the tip leaves the snapshot to Snowflake") {
    spark.sql("ALTER TABLE fb.db.secret CREATE BRANCH dev")
    assert(statements == Seq(s"$alter CREATE BRANCH 'dev'"))
  }

  test("CREATE BRANCH AS OF VERSION and REPLACE BRANCH") {
    spark.sql("ALTER TABLE fb.db.secret CREATE BRANCH dev AS OF VERSION 1")
    spark.sql("ALTER TABLE fb.db.secret REPLACE BRANCH x AS OF VERSION 1")
    spark.sql("ALTER TABLE fb.db.secret CREATE OR REPLACE BRANCH x AS OF VERSION 2")
    spark.sql("ALTER TABLE fb.db.secret CREATE OR REPLACE BRANCH y AS OF VERSION 2")
    assert(statements == Seq(
      s"$alter CREATE BRANCH 'dev' AS OF VERSION 1",
      s"$alter REPLACE BRANCH 'x' AS OF VERSION 1",
      s"$alter REPLACE BRANCH 'x' AS OF VERSION 2",
      s"$alter CREATE BRANCH 'y' AS OF VERSION 2"))
  }

  test("IF NOT EXISTS and IF EXISTS are decided from Snowflake's refs") {
    spark.sql("ALTER TABLE fb.db.secret CREATE BRANCH IF NOT EXISTS x")
    spark.sql("ALTER TABLE fb.db.secret DROP BRANCH IF EXISTS nope")
    spark.sql("ALTER TABLE fb.db.secret DROP TAG IF EXISTS nope")
    assert(statements.isEmpty)
    spark.sql("ALTER TABLE fb.db.secret DROP BRANCH x")
    spark.sql("ALTER TABLE fb.db.secret DROP TAG t0")
    assert(statements == Seq(s"""$alter DROP BRANCH 'x'""", s"""$alter DROP VERSION_TAG "t0""""))
  }

  test("tags keep their case and take RETAIN") {
    spark.sql("ALTER TABLE fb.db.secret CREATE TAG Rel1")
    spark.sql("ALTER TABLE fb.db.secret CREATE TAG rel2 AS OF VERSION 1 RETAIN 30 DAYS")
    spark.sql("ALTER TABLE fb.db.secret REPLACE TAG t0 AS OF VERSION 2 RETAIN 90 MINUTES")
    spark.sql("ALTER TABLE fb.db.secret CREATE TAG rel3 RETAIN 2 HOURS")
    assert(statements == Seq(
      s"""$alter CREATE VERSION_TAG "Rel1"""",
      s"""$alter CREATE VERSION_TAG "rel2" AS OF VERSION 1 RETAIN 30 DAYS""",
      s"""$alter REPLACE VERSION_TAG "t0" AS OF VERSION 2 RETAIN 90 MINUTES""",
      // A tag at the tip with RETAIN names main's snapshot: Snowflake requires AS OF there
      s"""$alter CREATE VERSION_TAG "rel3" AS OF VERSION 3 RETAIN 2 HOURS"""))
  }

  test("branch retention fails before Snowflake runs anything") {
    val e = intercept[UnsupportedOperationException](
      spark.sql("ALTER TABLE fb.db.secret CREATE BRANCH r WITH SNAPSHOT RETENTION 3 SNAPSHOTS"))
    assert(e.getMessage.contains("Branch snapshot retention"))
    intercept[UnsupportedOperationException](
      spark.sql("ALTER TABLE fb.db.secret CREATE BRANCH r RETAIN 7 DAYS"))
    assert(statements.isEmpty)
  }

  test("CALL fast_forward merges the branch into main and reports both snapshots") {
    val row = spark.sql("CALL fb.system.fast_forward('db.secret', 'main', 'x')").head
    assert(statements == Seq(s"$alter AT(BRANCH => 'main') MERGE BRANCH 'x'"))
    assert(row.getString(0) == "main" && row.getLong(1) == 3L && row.getLong(2) == 2L)
  }

  test("CALL fast_forward of a branch other than main fails") {
    val e = intercept[UnsupportedOperationException](
      spark.sql("CALL fb.system.fast_forward('db.secret', 'x', 'main')"))
    assert(e.getMessage.contains("only into main"))
    assert(statements.isEmpty)
  }

  test("CALL cherrypick_snapshot reports Snowflake's new main snapshot") {
    val row = spark.sql("CALL fb.system.cherrypick_snapshot('db.secret', 2)").head
    assert(statements == Seq(s"$alter CHERRYPICK 2"))
    assert(row.getLong(0) == 2L && row.getLong(1) == 4L)
  }

  test("procedures Snowflake can't run fail clearly") {
    for (call <- Seq("rollback_to_snapshot('db.secret', 1)", "set_current_snapshot('db.secret', 1)",
        "expire_snapshots('db.secret')")) {
      val e = intercept[Exception](spark.sql(s"CALL fb.system.$call").collect())
      assert(e.getMessage.contains("not supported"), s"$call: ${e.getMessage}")
    }
    assert(statements.isEmpty)
  }

  test("refs and snapshots come from Snowflake") {
    val table = catalog("fb").loadTable(Identifier.of(Array("db"), "secret"))
      .asInstanceOf[SnowflakeSparkTable].icebergTable
    assert(table.currentSnapshot().snapshotId() == 3L)
    assert(table.snapshot("x").snapshotId() == 2L && table.snapshot("x").parentId() == 1L)
    assert(table.refs().get("t0").isTag)
  }

  test("row-level operations fail with a clear error") {
    val e = intercept[Exception](spark.sql("DELETE FROM fb.db.secret WHERE id = 1").collect())
    assert(e.getMessage.contains("not supported yet"), e.getMessage)
  }
}
