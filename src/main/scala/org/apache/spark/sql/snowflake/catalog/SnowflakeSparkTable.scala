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

import java.util.{Locale, UUID}

import net.snowflake.spark.snowflake.{DefaultSource, Parameters}
import org.apache.iceberg.SnapshotRef
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.connector.catalog.{MetadataColumn, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{ScanBuilder, SupportsPushDownFilters, SupportsPushDownRequiredColumns, V1Scan}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, RowLevelOperationBuilder, RowLevelOperationInfo, SupportsTruncate, V1Write, WriteBuilder}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, TableScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
 * The table SnowflakeFallbackCatalog returns for a policy-protected table when
 * snowflake-fallback-mode=iceberg: an Iceberg SparkTable, so Iceberg's branch/tag DDL and
 * procedures accept it, over a SnowflakeIcebergTable that runs them as Snowflake SQL. Reads and
 * writes go through Snowflake, which applies the table's policies.
 *
 * @param params     Snowflake connection parameters with dbtable, or query for a time travel read
 * @param timeTravel the branch, tag, snapshot or timestamp the table was loaded at, if any
 */
private[catalog] class SnowflakeSparkTable(
    table: SnowflakeIcebergTable,
    params: Map[String, String],
    timeTravel: Option[IcebergTimeTravel.Spec])
  extends SparkTable(table, false) {

  private def spark: SparkSession = SparkSession.active

  private def relation(options: Map[String, String]): FallbackRelation =
    new FallbackDefaultSource().createRelation(spark.sqlContext, params ++ options)
      .asInstanceOf[FallbackRelation]

  private lazy val tableSchema: StructType = table.sql.schema(params)

  def icebergTable: SnowflakeIcebergTable = table

  override def schema(): StructType = tableSchema
  override def partitioning(): Array[Transform] = Array.empty
  override def properties(): java.util.Map[String, String] =
    Map("provider" -> "iceberg", "snowflake-table" -> table.dbtable).asJava
  override def metadataColumns(): Array[MetadataColumn] = Array.empty
  override def capabilities(): java.util.Set[TableCapability] = Set(
    TableCapability.BATCH_READ, TableCapability.V1_BATCH_WRITE, TableCapability.TRUNCATE).asJava

  override def branch(): String = timeTravel match {
    case Some(IcebergTimeTravel.Branch(b)) => b
    case _ => null
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    new SnowflakeScanBuilder(relation(options.asScala.toMap))

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = new WriteBuilder
    with SupportsTruncate {
    private var truncating = false
    override def truncate(): WriteBuilder = { truncating = true; this }
    override def build(): V1Write = new V1Write {
      override def toInsertableRelation: InsertableRelation = new InsertableRelation {
        // Spark's V1 fallback always passes overwrite = false; truncate() carries INSERT OVERWRITE
        override def insert(data: DataFrame, overwrite: Boolean): Unit =
          write(data, truncating || overwrite)
      }
    }
  }

  override def newRowLevelOperationBuilder(info: RowLevelOperationInfo): RowLevelOperationBuilder =
    throw new UnsupportedOperationException(
      s"${info.command()} is not supported yet on ${table.name()}: it is policy-protected, so " +
        "Spark writes it through Snowflake")

  override def canDeleteWhere(predicates: Array[Predicate]): Boolean = false

  override def deleteWhere(predicates: Array[Predicate]): Unit =
    throw new UnsupportedOperationException(s"DELETE is not supported yet on ${table.name()}")

  /** The branch a write goes to: the loaded branch, else spark.wap.branch; None for main. */
  private def writeBranch(): Option[String] = {
    val conf = spark.conf
    conf.getOption("spark.wap.id").filter(_.nonEmpty).foreach { id =>
      throw new UnsupportedOperationException(
        s"spark.wap.id=$id is set, but Snowflake cannot stage a snapshot of ${table.name()}; " +
          "use spark.wap.branch")
    }
    val wap = IcebergTimeTravel.wapBranch(conf)
    timeTravel match {
      case None => wap
      case Some(IcebergTimeTravel.Branch(b)) =>
        wap.filter(_ != b).foreach { w =>
          throw new IllegalArgumentException(
            s"Cannot write to branch $b of ${table.name()} while spark.wap.branch=$w is set")
        }
        Some(b)
      case Some(other) =>
        throw new UnsupportedOperationException(
          s"Cannot write to ${table.name()} at ${other.clause}: only branches take writes")
    }
  }

  /**
   * Loads the rows into a staging table, then inserts them into the target branch: COPY can't
   * target an Iceberg branch, INSERT ... AT(BRANCH) can. The staging table is loaded with the
   * regular Snowflake writer, or with JDBC batch inserts where the account's stage type isn't
   * supported by the writer (a local deployment's file-system stage).
   */
  private def write(data: DataFrame, overwrite: Boolean): Unit = {
    val branch = writeBranch()
    val dbtable = table.dbtable
    val staging = {
      val parts = dbtable.split('.')
      (parts.init :+ s"SPARK_SNOWFLAKE_WRITE_${UUID.randomUUID().toString.replace("-", "")}")
        .mkString(".").toUpperCase(Locale.ROOT)
    }
    val writeParams = params - Parameters.PARAM_SF_QUERY + (Parameters.PARAM_SF_DBTABLE -> staging)
    try {
      // The target's columns, in its order: Spark has already aligned the rows to them
      table.sql.execute(s"CREATE TABLE $staging AS SELECT * FROM $dbtable WHERE 1 = 0")
      try {
        new DefaultSource().createRelation(spark.sqlContext, SaveMode.Append, writeParams, data)
      } catch {
        case e: UnsupportedOperationException if String.valueOf(e.getMessage).contains("stage") =>
          SnowflakeSparkTable.insertWithJdbc(data, writeParams, staging)
      }
      val alter = s"ALTER ICEBERG TABLE $dbtable"
      // Iceberg creates the WAP branch on its first write
      if (timeTravel.isEmpty) {
        branch.foreach(b => table.sql.execute(
          s"$alter CREATE BRANCH IF NOT EXISTS '${SnowflakeIcebergTable.quote(b)}'"))
      }
      val at = branch.filter(_ != SnapshotRef.MAIN_BRANCH)
        .fold("")(b => s" AT(BRANCH => '${SnowflakeIcebergTable.quote(b)}')")
      table.sql.execute(
        s"INSERT ${if (overwrite) "OVERWRITE " else ""}INTO $dbtable$at SELECT * FROM $staging")
      table.refresh()
    } finally {
      table.sql.execute(s"DROP TABLE IF EXISTS $staging")
    }
  }
}

private[catalog] object SnowflakeSparkTable {
  private val BatchSize = 1000

  /** Executors insert their partition's rows over JDBC, in batches. */
  def insertWithJdbc(data: DataFrame, params: Map[String, String], staging: String): Unit = {
    val columns = data.schema.length
    val insert = s"INSERT INTO $staging VALUES (${Seq.fill(columns)("?").mkString(", ")})"
    data.foreachPartition { (rows: Iterator[Row]) =>
      val conn = net.snowflake.spark.snowflake.Utils.getJDBCConnection(params)
      try {
        val ps = conn.prepareStatement(insert)
        rows.grouped(BatchSize).foreach { batch =>
          batch.foreach { row =>
            (0 until columns).foreach { i =>
              if (row.isNullAt(i)) ps.setNull(i + 1, java.sql.Types.NULL)
              else ps.setObject(i + 1, row.get(i))
            }
            ps.addBatch()
          }
          ps.executeBatch()
        }
        ps.close()
      } finally {
        conn.close()
      }
    }
  }
}

/** A V1 scan of the Snowflake relation: filters and columns are pushed into its query. */
private[catalog] class SnowflakeScanBuilder(relation: FallbackRelation)
  extends ScanBuilder with SupportsPushDownFilters with SupportsPushDownRequiredColumns {

  private var required: StructType = relation.schema
  private var pushed: Array[Filter] = Array.empty

  // Spark evaluates every filter again after the scan
  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    pushed = filters
    filters
  }

  override def pushedFilters(): Array[Filter] = pushed

  override def pruneColumns(requiredSchema: StructType): Unit = required = requiredSchema

  override def build(): V1Scan = {
    val columns = required
    val filters = pushed
    new V1Scan {
      override def readSchema(): StructType = columns
      override def toV1TableScan[T <: BaseRelation with TableScan](context: SQLContext): T =
        new BaseRelation with TableScan {
          override def sqlContext: SQLContext = context
          override def schema: StructType = columns
          override def buildScan(): RDD[Row] = relation.buildScan(columns.fieldNames, filters)
        }.asInstanceOf[T]
      override def description(): String = s"Snowflake ${relation.relation}"
    }
  }
}
