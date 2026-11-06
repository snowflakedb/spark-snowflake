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

import java.util.Locale

import net.snowflake.spark.snowflake.Parameters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String

/**
 * Iceberg time travel for tables that SnowflakeFallbackCatalog reads over JDBC after a 403:
 * maps snapshot / branch / tag / timestamp requests to Snowflake's AT clause.
 */
private[catalog] object IcebergTimeTravel {

  /** A read of a fallback table that Snowflake answers with a query instead of dbtable. */
  sealed trait Spec {
    /** Distinguishes the read in the table's relation cache key. */
    def clause: String
    def query(dbtable: String): String = s"SELECT * FROM $dbtable $clause"
  }

  final case class Snapshot(id: Long) extends Spec {
    def clause: String = s"AT(VERSION => $id)"
  }

  /** A branch or tag; Snowflake's VERSION_REF resolves either. */
  final case class Ref(name: String) extends Spec {
    def clause: String = s"AT(VERSION_REF => '${quote(name)}')"
  }

  /**
   * A branch. Snowflake refreshes a branch of an unmanaged table that it has only recorded when a
   * query reads it with AT(BRANCH); AT(VERSION_REF) fails with "branch has not been refreshed".
   */
  final case class Branch(name: String) extends Spec {
    def clause: String = s"AT(BRANCH => '${quote(name)}')"
  }

  final case class TimestampMicros(micros: Long) extends Spec {
    def clause: String = s"AT(TIMESTAMP => TO_TIMESTAMP_LTZ($micros, 6))"
  }

  /** Iceberg's refs metadata table, from Snowflake's GET_TABLE_VERSION_REFS. */
  case object Refs extends Spec {
    def clause: String = "REFS"
    override def query(dbtable: String): String = {
      // The function resolves in a database's INFORMATION_SCHEMA; dbtable is unquoted
      val parts = dbtable.split('.')
      val infoSchema =
        if (parts.length == 3) s"${parts(0)}.INFORMATION_SCHEMA" else "INFORMATION_SCHEMA"
      """SELECT REFERENCE_NAME AS "name", REFERENCE_TYPE AS "type", VERSION_ID AS """ +
        """"snapshot_id", MAX_REFERENCE_AGE_IN_MS AS "max_reference_age_in_ms", """ +
        """MIN_SNAPSHOTS_TO_KEEP AS "min_snapshots_to_keep", MAX_SNAPSHOT_AGE_IN_MS AS """ +
        s""""max_snapshot_age_in_ms" FROM TABLE($infoSchema.GET_TABLE_VERSION_REFS(""" +
        s"""TABLE_NAME => '${quote(dbtable)}'))"""
    }
  }

  // Iceberg's SnapshotRef.MAIN_BRANCH; ref names are case-sensitive in Iceberg
  val MainBranch = "main"

  /**
   * A branch or tag read. None for main: it is the table's current state, and Snowflake rejects
   * AT(VERSION_REF => 'main'), so main is read without an AT clause.
   */
  def fromRef(name: String): Option[Spec] = if (name == MainBranch) None else Some(Ref(name))

  /** A read known to be of a branch; None for main, as in fromRef. */
  def fromBranch(name: String): Option[Spec] = if (name == MainBranch) None else Some(Branch(name))

  /** Spark's VERSION AS OF: a snapshot ID if numeric, otherwise a branch or tag (as Iceberg). */
  def fromVersion(version: String): Option[Spec] =
    if (version.nonEmpty && version.forall(_.isDigit)) Some(Snapshot(version.toLong))
    else fromRef(version)

  private val SnapshotId = "snapshot-id"
  private val AsOfTimestamp = "as-of-timestamp"
  private val BranchOption = "branch"
  private val Tag = "tag"
  // Spark's own read options; on 4.x Spark turns them into loadTable(ident, version|timestamp)
  private val VersionAsOf = "versionasof"
  private val TimestampAsOf = "timestampasof"
  private val ReadOptions =
    Seq(SnapshotId, AsOfTimestamp, BranchOption, Tag, VersionAsOf, TimestampAsOf)

  private def fromOption(key: String, value: String): Option[Spec] = key match {
    case SnapshotId => Some(Snapshot(value.trim.toLong))
    case AsOfTimestamp => Some(TimestampMicros(Math.multiplyExact(value.trim.toLong, 1000L)))
    case BranchOption => fromBranch(value)
    case Tag => Some(Ref(value))
    case VersionAsOf => fromVersion(value.trim)
    case TimestampAsOf =>
      val zone = DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone)
      Some(TimestampMicros(DateTimeUtils.stringToTimestamp(UTF8String.fromString(value.trim), zone)
        .getOrElse(throw new IllegalArgumentException(s"Invalid timestampAsOf: $value"))))
  }

  private def quote(name: String): String = name.replace("'", "''")

  def query(dbtable: String, spec: Spec): String = spec.query(dbtable)

  /** Lists the branch if the table's refs have it. */
  def branchExistsQuery(dbtable: String, branch: String): String =
    s"""SELECT 1 FROM (${Refs.query(dbtable)}) WHERE "name" = '${quote(branch)}' AND "type" = 'BRANCH'"""

  /** spark.wap.branch, unless unset or main. */
  def wapBranch(conf: org.apache.spark.sql.RuntimeConfig): Option[String] =
    conf.getOption("spark.wap.branch").filter(b => b.nonEmpty && b != MainBranch)

  /**
   * Rewrites the read parameters of a fallback table: a time travel read option becomes a query
   * with the matching AT clause.
   */
  def rewriteReadParameters(parameters: Map[String, String]): Map[String, String] = {
    val params = parameters.map { case (k, v) => k.toLowerCase(Locale.ROOT) -> v }
    val requested = ReadOptions.filter(params.contains)
    val base = params -- ReadOptions
    (requested, base.get(Parameters.PARAM_SF_QUERY)) match {
      case (Seq(), _) => base
      // The catalog already applied Spark's VERSION/TIMESTAMP AS OF to the query
      case (keys, Some(_)) if keys.forall(k => k == VersionAsOf || k == TimestampAsOf) => base
      case (Seq(key), None) =>
        fromOption(key, params(key)).fold(base) { spec =>
          val dbtable = base.getOrElse(Parameters.PARAM_SF_DBTABLE,
            throw new IllegalArgumentException(s"Time travel option '$key' requires dbtable"))
          base - Parameters.PARAM_SF_DBTABLE + (Parameters.PARAM_SF_QUERY -> query(dbtable, spec))
        }
      case (keys, _) =>
        throw new IllegalArgumentException(
          s"Cannot combine time travel options ${keys.mkString(", ")} with each other " +
            "or with VERSION AS OF / TIMESTAMP AS OF")
    }
  }

  /**
   * Branch writes have no Snowflake SQL equivalent, so they fail instead of writing main.
   * timeTravel: the relation reads a branch, tag or snapshot (its parameters have a query).
   */
  def checkWrite(timeTravel: Boolean, wapBranch: Option[String]): Unit = {
    // Checked first: with spark.wap.branch set, a read of the table is itself a branch read
    wapBranch.filter(b => b.nonEmpty && b != MainBranch).foreach { branch =>
      throw new UnsupportedOperationException(
        s"spark.wap.branch=$branch is set, but writes to a policy-protected table go " +
          "through Snowflake, which cannot write to an Iceberg branch. Unset spark.wap.branch " +
          "to write to the main branch.")
    }
    if (timeTravel) {
      throw new UnsupportedOperationException(
        "Cannot write to an Iceberg branch, tag or snapshot of a table read through " +
          "Snowflake (policy-protected table)")
    }
  }
}
