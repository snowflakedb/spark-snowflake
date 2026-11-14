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

import java.util.{List => JList, Map => JMap}
import java.util.concurrent.TimeUnit

import org.apache.iceberg._
import org.apache.iceberg.encryption.EncryptionManager
import org.apache.iceberg.io.{FileIO, LocationProvider}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

/**
 * An Iceberg Table for a policy-protected table that Horizon won't serve (403): its refs and
 * snapshots come from Snowflake, and manageSnapshots() runs ALTER ICEBERG TABLE statements, so
 * Iceberg's own branch/tag DDL and procedures (fast_forward, cherrypick_snapshot) work through
 * Snowflake. It has no files: scans and file-level operations fail.
 *
 * @param name     the Spark name (catalog.namespace.table), as Iceberg reports it
 * @param dbtable  the Snowflake table name, unquoted (DB.SCHEMA.TABLE)
 * @param sparkSchema the table's columns as Snowflake returns them
 */
private[catalog] class SnowflakeIcebergTable(
    val tableName: String,
    val dbtable: String,
    sparkSchema: => StructType,
    private[catalog] val sql: SnowflakeSql)
  extends Table with HasTableOperations {

  import SnowflakeIcebergTable._

  private var loadedRefs: Option[Map[String, SnapshotRef]] = None
  private var loadedSnapshots: Option[Seq[Snapshot]] = None
  private lazy val icebergSchema: Schema = SparkSchemaUtil.convert(sparkSchema)

  private def infoSchema: String = {
    // The functions resolve in a database's INFORMATION_SCHEMA
    val parts = dbtable.split('.')
    if (parts.length == 3) s"${parts(0)}.INFORMATION_SCHEMA" else "INFORMATION_SCHEMA"
  }

  override def name(): String = tableName
  override def toString: String = tableName

  override def refresh(): Unit = {
    loadedRefs = None
    loadedSnapshots = None
  }

  override def refs(): JMap[String, SnapshotRef] = currentRefs.asJava

  private[catalog] def currentRefs: Map[String, SnapshotRef] = loadedRefs.getOrElse {
    val refs = sql.query(
      "SELECT REFERENCE_NAME, REFERENCE_TYPE, VERSION_ID, MAX_REFERENCE_AGE_IN_MS, " +
        "MIN_SNAPSHOTS_TO_KEEP, MAX_SNAPSHOT_AGE_IN_MS FROM TABLE(" +
        s"$infoSchema.GET_TABLE_VERSION_REFS(TABLE_NAME => '${quote(dbtable)}'))"
    ).map { row =>
      val id = row(2).toLong
      val builder =
        if (row(1).equalsIgnoreCase("TAG")) SnapshotRef.tagBuilder(id)
        else SnapshotRef.branchBuilder(id)
      Option(row(3)).foreach(v => builder.maxRefAgeMs(v.toLong))
      if (!row(1).equalsIgnoreCase("TAG")) {
        Option(row(4)).foreach(v => builder.minSnapshotsToKeep(v.toInt))
        Option(row(5)).foreach(v => builder.maxSnapshotAgeMs(v.toLong))
      }
      row(0) -> builder.build()
    }.toMap
    loadedRefs = Some(refs)
    refs
  }

  /** After a commit: the refs it is known to have produced, until the next refresh. */
  private[catalog] def setRefs(refs: Map[String, SnapshotRef]): Unit = loadedRefs = Some(refs)

  override def currentSnapshot(): Snapshot =
    currentRefs.get(SnapshotRef.MAIN_BRANCH).map(ref => snapshot(ref.snapshotId())).orNull

  override def snapshots(): java.lang.Iterable[Snapshot] = allSnapshots.asJava

  private def allSnapshots: Seq[Snapshot] = loadedSnapshots.getOrElse {
    val snaps = sql.query(
      "SELECT SNAPSHOT_ID, PARENT_ID, DATE_PART(EPOCH_MILLISECOND, COMMITTED_AT), OPERATION " +
        "FROM TABLE(" +
        s"$infoSchema.ICEBERG_TABLE_SNAPSHOTS('${quote(dbtable)}')) ORDER BY COMMITTED_AT"
    ).map { row =>
      SnowflakeSnapshot(row(0).toLong, Option(row(1)).map(_.toLong),
        Option(row(2)).map(_.toLong).getOrElse(0L), row(3), 0L)
    }
    loadedSnapshots = Some(snaps)
    snaps
  }

  override def snapshot(snapshotId: Long): Snapshot =
    allSnapshots.find(_.snapshotId() == snapshotId).getOrElse {
      // Snowflake can list a ref before its snapshot (ICEBERG_TABLE_SNAPSHOTS lags new commits)
      if (currentRefs.values.exists(_.snapshotId() == snapshotId)) {
        SnowflakeSnapshot(snapshotId, None, 0L, null, 0L)
      } else {
        null
      }
    }

  override def history(): JList[HistoryEntry] = allSnapshots.map { s =>
    new HistoryEntry {
      override def timestampMillis(): Long = s.timestampMillis()
      override def snapshotId(): Long = s.snapshotId()
    }: HistoryEntry
  }.asJava

  override def manageSnapshots(): ManageSnapshots = new SnowflakeManageSnapshots(this)

  override def schema(): Schema = icebergSchema
  override def schemas(): JMap[Integer, Schema] =
    Map(Integer.valueOf(icebergSchema.schemaId()) -> icebergSchema).asJava
  override def spec(): PartitionSpec = PartitionSpec.unpartitioned()
  override def specs(): JMap[Integer, PartitionSpec] =
    Map(Integer.valueOf(0) -> PartitionSpec.unpartitioned()).asJava
  override def sortOrder(): SortOrder = SortOrder.unsorted()
  override def sortOrders(): JMap[Integer, SortOrder] =
    Map(Integer.valueOf(0) -> SortOrder.unsorted()).asJava
  override def properties(): JMap[String, String] = Map.empty[String, String].asJava
  override def location(): String = null
  override def statisticsFiles(): JList[StatisticsFile] = List.empty[StatisticsFile].asJava

  // No files: Snowflake reads, writes and maintains the table
  private def unsupported(what: String): Nothing =
    throw new UnsupportedOperationException(
      s"$what is not supported on $tableName: it is policy-protected, so Spark reads and " +
        "writes it through Snowflake")

  // Iceberg's actions (expire_snapshots, rewrite_*) cast to HasTableOperations first
  override def operations(): TableOperations =
    unsupported("Iceberg file-level operations (MERGE, UPDATE, table maintenance)")
  override def newScan(): TableScan = unsupported("An Iceberg file scan")
  override def updateSchema(): UpdateSchema = unsupported("Iceberg schema evolution")
  override def updateSpec(): UpdatePartitionSpec = unsupported("Iceberg partition evolution")
  override def updateProperties(): UpdateProperties = unsupported("Setting table properties")
  override def replaceSortOrder(): ReplaceSortOrder = unsupported("Setting the sort order")
  override def updateLocation(): UpdateLocation = unsupported("Setting the table location")
  override def newAppend(): AppendFiles = unsupported("An Iceberg file append")
  override def newRewrite(): RewriteFiles = unsupported("Rewriting data files")
  override def rewriteManifests(): RewriteManifests = unsupported("Rewriting manifests")
  override def newOverwrite(): OverwriteFiles = unsupported("An Iceberg file overwrite")
  override def newRowDelta(): RowDelta = unsupported("An Iceberg row delta")
  override def newReplacePartitions(): ReplacePartitions = unsupported("Replacing partitions")
  override def newDelete(): DeleteFiles = unsupported("Deleting Iceberg files")
  override def expireSnapshots(): ExpireSnapshots = unsupported("Expiring snapshots")
  override def newTransaction(): Transaction = unsupported("An Iceberg transaction")
  override def io(): FileIO = unsupported("Iceberg file IO")
  override def encryption(): EncryptionManager = unsupported("Iceberg encryption")
  override def locationProvider(): LocationProvider = unsupported("An Iceberg location provider")
}

private[catalog] object SnowflakeIcebergTable {
  def quote(name: String): String = name.replace("'", "''")

  /** Tag names are identifiers in Snowflake: double-quoted to keep Iceberg's case. */
  def quoteIdentifier(name: String): String = "\"" + name.replace("\"", "\"\"") + "\""

  /** Snowflake's RETAIN clause for Iceberg's max-ref-age-ms. */
  def retainClause(maxRefAgeMs: Long): String = {
    val minutes = TimeUnit.MILLISECONDS.toMinutes(maxRefAgeMs)
    require(minutes > 0 && TimeUnit.MINUTES.toMillis(minutes) == maxRefAgeMs,
      s"Snowflake retains tags for whole minutes, hours or days; got $maxRefAgeMs ms")
    if (minutes % (24 * 60) == 0) s"RETAIN ${minutes / (24 * 60)} DAYS"
    else if (minutes % 60 == 0) s"RETAIN ${minutes / 60} HOURS"
    else s"RETAIN $minutes MINUTES"
  }
}

/** A snapshot as Snowflake lists it: no manifests or files. */
private[catalog] case class SnowflakeSnapshot(
    id: Long, parent: Option[Long], committedAtMs: Long, op: String, sequence: Long)
  extends Snapshot {
  override def sequenceNumber(): Long = sequence
  override def snapshotId(): Long = id
  override def parentId(): java.lang.Long = parent.map(java.lang.Long.valueOf).orNull
  override def timestampMillis(): Long = committedAtMs
  override def operation(): String = op
  override def summary(): JMap[String, String] = Map.empty[String, String].asJava
  override def manifestListLocation(): String = null

  private def noFiles: Nothing = throw new UnsupportedOperationException(
    "Snapshot files are not available for a table read through Snowflake")
  override def allManifests(io: FileIO): JList[ManifestFile] = noFiles
  override def dataManifests(io: FileIO): JList[ManifestFile] = noFiles
  override def deleteManifests(io: FileIO): JList[ManifestFile] = noFiles
  override def addedDataFiles(io: FileIO): java.lang.Iterable[DataFile] = noFiles
  override def removedDataFiles(io: FileIO): java.lang.Iterable[DataFile] = noFiles
}

/**
 * Iceberg's ManageSnapshots as ALTER ICEBERG TABLE statements, run in order on commit().
 * Snowflake has no branch retention, merges branches only into main, and has no rollback, so
 * those fail before anything runs.
 */
private[catalog] class SnowflakeManageSnapshots(table: SnowflakeIcebergTable)
  extends ManageSnapshots {

  import SnowflakeIcebergTable._

  private sealed trait Op
  private case class CreateBranch(name: String, id: Option[Long], replace: Boolean) extends Op
  private case class CreateTag(name: String, id: Long, replace: Boolean, var retainMs: Option[Long])
    extends Op
  private case class DropRef(name: String, tag: Boolean) extends Op
  private case class FastForward(to: String) extends Op
  private case class Cherrypick(id: Long) extends Op

  private val ops = ArrayBuffer[Op]()
  // main as loaded when the operation started: Iceberg's DDL passes it as the snapshot of a
  // branch or tag created "at the tip"; Snowflake then uses its own current state
  private val loadedMain = table.currentRefs.get(SnapshotRef.MAIN_BRANCH).map(_.snapshotId())

  private def snapshotOf(id: Long): Option[Long] = if (loadedMain.contains(id)) None else Some(id)

  private def unsupported(what: String): Nothing =
    throw new UnsupportedOperationException(s"$what is not supported by Snowflake on ${table.name()}")

  private def checkNotMain(name: String): Unit =
    if (name.equalsIgnoreCase(SnapshotRef.MAIN_BRANCH)) {
      throw new IllegalArgumentException(s"Cannot change the main branch of ${table.name()} " +
        "this way: Snowflake reserves 'main'")
    }

  override def createBranch(name: String): ManageSnapshots = {
    checkNotMain(name)
    ops += CreateBranch(name, None, replace = false)
    this
  }

  override def createBranch(name: String, snapshotId: Long): ManageSnapshots = {
    checkNotMain(name)
    ops += CreateBranch(name, snapshotOf(snapshotId), replace = false)
    this
  }

  override def replaceBranch(name: String, snapshotId: Long): ManageSnapshots = {
    checkNotMain(name)
    ops += CreateBranch(name, snapshotOf(snapshotId), replace = true)
    this
  }

  override def removeBranch(name: String): ManageSnapshots = {
    checkNotMain(name)
    ops += DropRef(name, tag = false)
    this
  }

  override def createTag(name: String, snapshotId: Long): ManageSnapshots = {
    ops += CreateTag(name, snapshotId, replace = false, None)
    this
  }

  override def replaceTag(name: String, snapshotId: Long): ManageSnapshots = {
    ops += CreateTag(name, snapshotId, replace = true, None)
    this
  }

  override def removeTag(name: String): ManageSnapshots = {
    ops += DropRef(name, tag = true)
    this
  }

  override def setMaxRefAgeMs(name: String, maxRefAgeMs: Long): ManageSnapshots = {
    ops.collect { case t: CreateTag if t.name == name => t }.lastOption match {
      case Some(tag) => tag.retainMs = Some(maxRefAgeMs)
      case None => unsupported("Retention for a branch, or for a tag outside CREATE/REPLACE TAG,")
    }
    this
  }

  override def setMinSnapshotsToKeep(branchName: String, minSnapshotsToKeep: Int): ManageSnapshots =
    unsupported("Branch snapshot retention")

  override def setMaxSnapshotAgeMs(branchName: String, maxSnapshotAgeMs: Long): ManageSnapshots =
    unsupported("Branch snapshot retention")

  override def fastForwardBranch(from: String, to: String): ManageSnapshots = {
    if (from != SnapshotRef.MAIN_BRANCH) {
      unsupported(s"Fast-forwarding branch '$from' (Snowflake merges branches only into main)")
    }
    ops += FastForward(to)
    this
  }

  override def cherrypick(snapshotId: Long): ManageSnapshots = {
    ops += Cherrypick(snapshotId)
    this
  }

  override def setCurrentSnapshot(snapshotId: Long): ManageSnapshots =
    unsupported("Setting the current snapshot")
  override def rollbackToTime(timestampMillis: Long): ManageSnapshots =
    unsupported("Rolling back to a timestamp")
  override def rollbackTo(snapshotId: Long): ManageSnapshots =
    unsupported("Rolling back to a snapshot")
  override def renameBranch(name: String, newName: String): ManageSnapshots =
    unsupported("Renaming a branch")
  override def replaceBranch(from: String, to: String): ManageSnapshots =
    unsupported("Replacing a branch with another branch")

  /** The statements commit() runs. */
  private[catalog] def statements: Seq[String] = {
    val alter = s"ALTER ICEBERG TABLE ${table.dbtable}"
    def asOf(id: Option[Long]) = id.fold("")(i => s" AS OF VERSION $i")
    ops.map {
      case CreateBranch(name, id, false) => s"$alter CREATE BRANCH '${quote(name)}'${asOf(id)}"
      case CreateBranch(name, id, true) => s"$alter REPLACE BRANCH '${quote(name)}'${asOf(id)}"
      case CreateTag(name, id, replace, retain) =>
        s"$alter ${if (replace) "REPLACE" else "CREATE"} VERSION_TAG ${quoteIdentifier(name)}" +
          // Snowflake's tag at the tip takes no RETAIN, and REPLACE requires AS OF
          asOf(if (replace || retain.isDefined) Some(id) else snapshotOf(id)) +
          retain.fold("")(ms => " " + retainClause(ms))
      case DropRef(name, false) => s"$alter DROP BRANCH '${quote(name)}'"
      case DropRef(name, true) => s"$alter DROP VERSION_TAG ${quoteIdentifier(name)}"
      case FastForward(to) => s"$alter AT(BRANCH => 'main') MERGE BRANCH '${quote(to)}'"
      case Cherrypick(id) => s"$alter CHERRYPICK $id"
    }.toList
  }

  /** main's snapshot after the operations, when it doesn't depend on a snapshot Snowflake creates. */
  override def apply(): Snapshot = expectedRefs
    .flatMap(_.get(SnapshotRef.MAIN_BRANCH))
    .map(ref => table.snapshot(ref.snapshotId()))
    .getOrElse(table.currentSnapshot())

  /** The refs after the operations, if they don't depend on a snapshot Snowflake creates. */
  private def expectedRefs: Option[Map[String, SnapshotRef]] = {
    val start = table.currentRefs
    ops.foldLeft(Option(start)) {
      case (Some(refs), CreateBranch(name, id, _)) =>
        id.orElse(loadedMain).map(i => refs + (name -> SnapshotRef.branchBuilder(i).build()))
      case (Some(refs), CreateTag(name, id, _, retain)) =>
        val b = SnapshotRef.tagBuilder(id)
        retain.foreach(ms => b.maxRefAgeMs(ms))
        Some(refs + (name -> b.build()))
      case (Some(refs), DropRef(name, _)) => Some(refs - name)
      case (Some(refs), FastForward(to)) =>
        refs.get(to).map(r => refs + (SnapshotRef.MAIN_BRANCH ->
          SnapshotRef.branchBuilder(r.snapshotId()).build()))
      case _ => None
    }
  }

  override def commit(): Unit = {
    val expected = expectedRefs
    statements.foreach(table.sql.execute)
    table.refresh()
    // Snowflake's refs function can lag a commit: keep the refs the statements are known to
    // produce; a cherry-pick's new snapshot comes from Snowflake
    expected.foreach(table.setRefs)
    if (expected.isEmpty) awaitMainChange()
  }

  private def awaitMainChange(): Unit = {
    val deadline = System.currentTimeMillis() + 30000L
    while (table.currentRefs.get(SnapshotRef.MAIN_BRANCH).map(_.snapshotId()) == loadedMain &&
        System.currentTimeMillis() < deadline) {
      Thread.sleep(1000L)
      table.refresh()
    }
  }
}
