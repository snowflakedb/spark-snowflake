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

import java.time.{LocalDateTime, ZoneId}

import net.snowflake.spark.snowflake.{DefaultSource, Parameters}
import org.apache.spark.sql.snowflake.catalog.IcebergTimeTravel.{Branch, Ref, Snapshot}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class IcebergTimeTravelSuite extends AnyFunSuite with BeforeAndAfterAll {

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    SparkSession.getActiveSession.foreach(_.stop())
    SparkSession.getDefaultSession.foreach(_.stop())
    spark = SparkSession.builder().master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private val fallbackTable = Map("dbtable" -> "MYDB.db.secret", "sfdatabase" -> "MYDB")

  private def rewritten(options: (String, String)*): Map[String, String] =
    IcebergTimeTravel.rewriteReadParameters(fallbackTable ++ options)

  private def query(options: (String, String)*): Option[String] = rewritten(options: _*).get("query")

  private val epochMillis = 1764547200000L

  test("each time travel read option maps to its Snowflake AT clause") {
    val ts = "2025-12-01 00:00:00"
    val tsMicros = LocalDateTime.parse("2025-12-01T00:00:00")
      .atZone(ZoneId.of("UTC")).toInstant.toEpochMilli * 1000L
    val cases = Seq(
      ("snapshot-id" -> "123") -> "AT(VERSION => 123)",
      ("as-of-timestamp" -> epochMillis.toString) ->
        s"AT(TIMESTAMP => TO_TIMESTAMP_LTZ(${epochMillis * 1000L}, 6))",
      ("branch" -> "audit") -> "AT(BRANCH => 'audit')",
      ("tag" -> "v1") -> "AT(VERSION_REF => 'v1')",
      ("versionAsOf" -> "123") -> "AT(VERSION => 123)",
      ("versionAsOf" -> "audit") -> "AT(VERSION_REF => 'audit')",
      ("timestampAsOf" -> ts) -> s"AT(TIMESTAMP => TO_TIMESTAMP_LTZ($tsMicros, 6))")
    for ((option, clause) <- cases) {
      val params = rewritten(option)
      assert(params.get("query").contains(s"SELECT * FROM MYDB.db.secret $clause"), option)
      assert(!params.contains("dbtable"), option)
      assert(!params.contains(option._1.toLowerCase), s"$option must not reach JDBC")
    }
  }

  test("branch and tag names are quoted as Snowflake string literals") {
    assert(query("branch" -> "it's") ==
      Some("SELECT * FROM MYDB.db.secret AT(BRANCH => 'it''s')"))
    assert(query("tag" -> "it's") ==
      Some("SELECT * FROM MYDB.db.secret AT(VERSION_REF => 'it''s')"))
  }

  test("a read known to be of a branch uses AT(BRANCH); a branch-or-tag name uses VERSION_REF") {
    // Snowflake refreshes a recorded branch of an unmanaged table only for AT(BRANCH)
    assert(IcebergTimeTravel.fromBranch("dev").contains(Branch("dev")))
    assert(IcebergTimeTravel.fromBranch("main").isEmpty)
    assert(IcebergTimeTravel.fromVersion("dev").contains(Ref("dev")))
  }

  test("VERSION AS OF picks a snapshot ID only for numeric versions") {
    assert(IcebergTimeTravel.fromVersion("42").contains(Snapshot(42L)))
    assert(IcebergTimeTravel.fromVersion("v42").contains(Ref("v42")))
    assert(IcebergTimeTravel.fromVersion("").contains(Ref("")))
  }

  test("main is read as the current table, without an AT clause") {
    assert(IcebergTimeTravel.fromVersion("main").isEmpty)
    assert(IcebergTimeTravel.fromRef("main").isEmpty)
    // Iceberg ref names are case-sensitive
    assert(IcebergTimeTravel.fromRef("MAIN").contains(Ref("MAIN")))
    for (option <- Seq("branch" -> "main", "versionAsOf" -> "main")) {
      val params = rewritten(option)
      assert(params.get("dbtable").contains("MYDB.db.secret"), option)
      assert(!params.contains("query"), option)
      assert(!params.contains(option._1.toLowerCase), s"$option must not reach JDBC")
    }
  }

  test("a fallback table without time travel options keeps dbtable") {
    assert(rewritten() == fallbackTable)
  }

  test("Spark's own AS OF options are dropped once the catalog applied them") {
    val applied = fallbackTable - "dbtable" + ("query" -> "SELECT * FROM t AT(VERSION => 1)")
    assert(IcebergTimeTravel.rewriteReadParameters(applied + ("versionAsOf" -> "1")) == applied)
  }

  test("conflicting time travel requests fail") {
    intercept[IllegalArgumentException](rewritten("branch" -> "x", "tag" -> "t"))
    val applied = fallbackTable - "dbtable" + ("query" -> "SELECT * FROM t AT(VERSION => 1)")
    intercept[IllegalArgumentException](
      IcebergTimeTravel.rewriteReadParameters(applied + ("branch" -> "x")))
  }

  test("invalid option values fail") {
    intercept[NumberFormatException](rewritten("snapshot-id" -> "abc"))
    intercept[IllegalArgumentException](rewritten("timestampAsOf" -> "not a timestamp"))
  }

  private val creds = Map("sfurl" -> "acct.snowflakecomputing.com:443", "sfuser" -> "u",
    "sfpassword" -> "p")
  private val schema = StructType(Seq(StructField("id", IntegerType)))

  private def relation(
      params: Map[String, String],
      source: FallbackDefaultSource = new FallbackDefaultSource()): FallbackRelation =
    source.createRelation(spark.sqlContext, params ++ creds, schema)
      .asInstanceOf[FallbackRelation]

  /** Answers the branch lookup from `branches` instead of Snowflake, and records the lookups. */
  private class StubRefsSource(branches: Set[String]) extends FallbackDefaultSource {
    val lookups = scala.collection.mutable.ArrayBuffer[(String, String)]()
    override protected def branchExists(
        params: Map[String, String], dbtable: String, branch: String): Boolean = {
      lookups += (dbtable -> branch)
      branches.contains(branch)
    }
  }

  private def withWapBranch[T](branch: String)(f: => T): T = {
    spark.conf.set("spark.wap.branch", branch)
    try f finally spark.conf.unset("spark.wap.branch")
  }

  // SnowflakeRelation is private to the connector package, so it is read by reflection
  private def paramsOf(rel: FallbackRelation): Parameters.MergedParameters =
    rel.relation.getClass.getMethod("params").invoke(rel.relation)
      .asInstanceOf[Parameters.MergedParameters]

  private def selectSql(rel: FallbackRelation): String = {
    val inner = rel.relation
    val method =
      inner.getClass.getDeclaredMethods.find(_.getName.endsWith("standardStatement")).get
    method.setAccessible(true)
    val statement = method.invoke(inner, Array("id"), Array.empty[Filter])
    statement.getClass.getMethod("statementString").invoke(statement).asInstanceOf[String]
  }

  test("FallbackDefaultSource sends the AT clause in the SELECT") {
    val rel = relation(fallbackTable + ("snapshot-id" -> "123"))
    assert(paramsOf(rel).table.isEmpty)
    assert(selectSql(rel) ==
      """SELECT "ID" FROM (SELECT * FROM MYDB.db.secret AT(VERSION => 123))""")
    assert(!paramsOf(rel).sfExtraOptions.contains("snapshot-id"))
    assert(rel.timeTravel)
  }

  test("FallbackDefaultSource reads a table without options from dbtable") {
    val rel = relation(fallbackTable)
    assert(paramsOf(rel).table.map(_.toString).contains("MYDB.db.secret"))
    assert(paramsOf(rel).query.isEmpty)
    assert(!rel.timeTravel)
  }

  test("FallbackRelation passes reads through to the Snowflake relation") {
    val rel = relation(fallbackTable)
    assert(rel.schema == schema)
    assert(rel.sizeInBytes == rel.relation.sizeInBytes)
    assert(rel.needConversion == rel.relation.needConversion)
    val filters: Array[Filter] = Array(org.apache.spark.sql.sources.EqualTo("id", 1))
    assert(rel.unhandledFilters(filters).toSeq == rel.relation.unhandledFilters(filters).toSeq)
  }

  test("writes to a branch, tag or snapshot of a fallback table fail") {
    assert(relation(fallbackTable + ("branch" -> "x")).timeTravel)
    intercept[UnsupportedOperationException](IcebergTimeTravel.checkWrite(timeTravel = true, None))
  }

  test("writes to a fallback table with spark.wap.branch set fail instead of writing main") {
    IcebergTimeTravel.checkWrite(timeTravel = false, None)
    IcebergTimeTravel.checkWrite(timeTravel = false, Some(""))
    IcebergTimeTravel.checkWrite(timeTravel = false, Some("main"))
    val e = intercept[UnsupportedOperationException](
      IcebergTimeTravel.checkWrite(timeTravel = false, Some("audit")))
    assert(e.getMessage.contains("spark.wap.branch=audit"))
  }

  test("writes to a fallback table with spark.wap.id set fail instead of writing main") {
    IcebergTimeTravel.checkWrite(timeTravel = false, None, Some(""))
    val e = intercept[UnsupportedOperationException](
      IcebergTimeTravel.checkWrite(timeTravel = false, None, Some("w1")))
    assert(e.getMessage.contains("spark.wap.id=w1"))
  }

  test("with spark.wap.branch set, a plain read reads the WAP branch once it exists") {
    withWapBranch("dev") {
      val source = new StubRefsSource(Set("dev"))
      val rel = relation(fallbackTable, source)
      assert(paramsOf(rel).query.contains("SELECT * FROM MYDB.db.secret AT(BRANCH => 'dev')"))
      assert(paramsOf(rel).table.isEmpty)
      assert(source.lookups == Seq("MYDB.db.secret" -> "dev"))
    }
  }

  test("with spark.wap.branch set to a branch that doesn't exist yet, a plain read reads main") {
    withWapBranch("dev") {
      val rel = relation(fallbackTable, new StubRefsSource(Set.empty))
      assert(paramsOf(rel).table.map(_.toString).contains("MYDB.db.secret"))
      assert(paramsOf(rel).query.isEmpty)
    }
  }

  test("an explicit version wins over spark.wap.branch, and main needs no lookup") {
    withWapBranch("dev") {
      val source = new StubRefsSource(Set("dev"))
      val rel = relation(fallbackTable + ("snapshot-id" -> "123"), source)
      assert(paramsOf(rel).query.contains("SELECT * FROM MYDB.db.secret AT(VERSION => 123)"))
      assert(source.lookups.isEmpty)
    }
    withWapBranch("main") {
      val source = new StubRefsSource(Set("main"))
      assert(paramsOf(relation(fallbackTable, source)).query.isEmpty)
      assert(source.lookups.isEmpty)
    }
  }

  test("a write with spark.wap.branch set fails with the WAP message, also on a WAP branch read") {
    val e = intercept[UnsupportedOperationException](
      IcebergTimeTravel.checkWrite(timeTravel = true, Some("dev")))
    assert(e.getMessage.contains("spark.wap.branch=dev"))
  }

  test("the refs metadata table is read from GET_TABLE_VERSION_REFS with Iceberg's columns") {
    assert(IcebergTimeTravel.query("MYDB.db.secret", IcebergTimeTravel.Refs) ==
      "SELECT REFERENCE_NAME AS \"name\", REFERENCE_TYPE AS \"type\", VERSION_ID AS " +
        "\"snapshot_id\", MAX_REFERENCE_AGE_IN_MS AS \"max_reference_age_in_ms\", " +
        "MIN_SNAPSHOTS_TO_KEEP AS \"min_snapshots_to_keep\", MAX_SNAPSHOT_AGE_IN_MS AS " +
        "\"max_snapshot_age_in_ms\" FROM TABLE(MYDB.INFORMATION_SCHEMA.GET_TABLE_VERSION_REFS(" +
        "TABLE_NAME => 'MYDB.db.secret'))")
    // Without a database in dbtable, the function resolves in the session's database
    assert(IcebergTimeTravel.query("db.secret", IcebergTimeTravel.Refs)
      .endsWith("FROM TABLE(INFORMATION_SCHEMA.GET_TABLE_VERSION_REFS(TABLE_NAME => 'db.secret'))"))
    assert(IcebergTimeTravel.branchExistsQuery("MYDB.db.secret", "it's").endsWith(
      "WHERE \"name\" = 'it''s' AND \"type\" = 'BRANCH'"))
  }

  test("the Snowflake data source itself is unchanged") {
    val plain = new DefaultSource().createRelation(
      spark.sqlContext, Map("dbtable" -> "T", "branch" -> "x") ++ creds, schema)
    assert(!plain.isInstanceOf[FallbackRelation])
  }
}
