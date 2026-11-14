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

import net.snowflake.spark.snowflake.Parameters
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.spark.SparkCatalog
import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.{Identifier, Table, V1Table}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}

private object SnowflakeFallbackCatalog {
  // Iceberg SparkCatalog's snapshot selectors in the table name
  val SnapshotIdSelector = "snapshot_id_(\\d+)".r
  val AtTimestampSelector = "at_timestamp_(\\d+)".r
  val BranchSelector = "branch_(.+)".r
  val TagSelector = "tag_(.+)".r
  // Iceberg MetadataTableType names
  val IcebergMetadataTables = Set("entries", "files", "data_files", "delete_files", "history",
    "metadata_log_entries", "snapshots", "refs", "manifests", "partitions", "all_data_files",
    "all_delete_files", "all_files", "all_manifests", "all_entries", "position_deletes")
  // Options of this catalog that Iceberg must not see
  val SnowflakeOptions = Set("catalog-impl", "snowflake-database", "snowflake-fallback-mode")
  // snowflake-fallback-mode: v1 (default) returns a read-only V1Table for a protected table;
  // iceberg returns a SnowflakeSparkTable, which also takes Iceberg DDL, procedures and writes
  val FallbackModes = Set("v1", "iceberg")
}

/**
 * Iceberg's SparkCatalog (e.g. Horizon IRC with type=rest) that reads a table through Snowflake
 * JDBC when Iceberg returns 403 for it, e.g. for a table with a Snowflake policy. Everything
 * else (branches, tags, procedures, writes) is plain Iceberg, so it needs only
 * IcebergSparkSessionExtensions, like SparkCatalog.
 */
class SnowflakeFallbackCatalog extends SparkCatalog {

  import SnowflakeFallbackCatalog._

  private val log = LoggerFactory.getLogger(getClass)
  private var catalogName: String = _
  private var snowflakeDatabase: Option[String] = None
  private var warehouse: Option[String] = None
  private var icebergFallback = false

  override protected def buildIcebergCatalog(
      name: String, options: CaseInsensitiveStringMap): Catalog =
    super.buildIcebergCatalog(name, configure(name, options))

  /**
   * Reads this catalog's own options and returns the options for Iceberg: without them, and
   * with the spark-snowflake User-Agent for Horizon.
   */
  protected def configure(
      name: String, options: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    this.catalogName = name
    // Inserted unquoted into dbtable
    this.snowflakeDatabase =
      Option(options.get("snowflake-database")).map(_.trim).filter(_.nonEmpty)
    // Horizon IRC maps the Iceberg warehouse to the Snowflake database
    this.warehouse = Option(options.get("warehouse")).map(_.trim).filter(_.nonEmpty)
    val mode = Option(options.get("snowflake-fallback-mode")).map(_.trim.toLowerCase).getOrElse("v1")
    require(FallbackModes.contains(mode), s"Catalog '$name': snowflake-fallback-mode must be one " +
      s"of ${FallbackModes.mkString(", ")}, got $mode")
    this.icebergFallback = mode == "iceberg"
    log.info("Initializing SnowflakeFallbackCatalog: {}", name)

    // Before this catalog extended SparkCatalog, catalog-impl named the delegate SparkCatalog
    Option(options.get("catalog-impl")).foreach { impl =>
      if (impl != classOf[SparkCatalog].getName) {
        throw new IllegalArgumentException(
          s"Catalog '$name': SnowflakeFallbackCatalog is an Iceberg SparkCatalog; " +
            s"catalog-impl must be unset or ${classOf[SparkCatalog].getName}, got $impl")
      }
    }
    // Format: spark-snowflake/<connector-version> spark/<spark-version>; header.User-Agent works
    // with all Iceberg versions
    val userAgent =
      s"spark-snowflake/${net.snowflake.spark.snowflake.Utils.VERSION} spark/$SPARK_VERSION"
    val icebergOptions = options.asScala
      .filterKeys(key => !SnowflakeOptions.contains(key.toLowerCase))
      .toMap + ("header.User-Agent" -> userAgent)
    new CaseInsensitiveStringMap(icebergOptions.asJava)
  }

  // Set while SparkCatalog's versioned loadTable runs: on some Iceberg versions it calls
  // loadTable(ident), which must then rethrow the 403 so the versioned load falls back with its
  // time travel spec rather than receive a plain fallback table
  private val inVersionedLoad = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      super.loadTable(ident)
    } catch {
      case ex: Throwable if inVersionedLoad.get() => throw ex
      case ex: Throwable =>
        if (isForbiddenException(ex)) {
          if (isMetadataTableOfProtected(ident)) {
            // Snowflake has only the refs metadata table (GET_TABLE_VERSION_REFS)
            if (!ident.name().equalsIgnoreCase("refs")) throw ex
            fallbackToSnowflake(parentOf(ident), Some(IcebergTimeTravel.Refs), ex)
          } else {
            fallbackToSnowflake(ident, None, ex)
          }
        } else if (ex.isInstanceOf[NoSuchTableException]) {
          loadProtectedSelector(ident).getOrElse(throw ex)
        } else {
          throw ex
        }
    }
  }

  override def loadTable(ident: Identifier, version: String): Table =
    loadTimeTravel(ident, IcebergTimeTravel.fromVersion(version)) {
      super.loadTable(ident, version)
    }

  override def loadTable(ident: Identifier, timestamp: Long): Table =
    loadTimeTravel(ident, Some(IcebergTimeTravel.TimestampMicros(timestamp))) {
      super.loadTable(ident, timestamp)
    }

  /** spec None reads the current table (VERSION AS OF 'main'). */
  private def loadTimeTravel(ident: Identifier, spec: Option[IcebergTimeTravel.Spec])(
      load: => Table): Table = {
    val outer = inVersionedLoad.get()
    inVersionedLoad.set(true)
    try {
      load
    } catch {
      case ex: Throwable if !outer && isForbiddenException(ex) =>
        fallbackToSnowflake(ident, spec, ex)
    } finally {
      inVersionedLoad.set(outer)
    }
  }

  private def fallbackToSnowflake(
      ident: Identifier, spec: Option[IcebergTimeTravel.Spec], original: Throwable): Table = {
    log.info(s"403 Forbidden detected for ${ident}, attempting JDBC fallback")
    try {
      if (icebergFallback) createSnowflakeSparkTable(ident, spec)
      else createSnowflakeV1Table(ident, spec)
    } catch {
      case fallbackEx: Throwable =>
        // rethrow original exception if fallback also fails
        log.error(s"Failed to create V1Table fallback for ${ident}", fallbackEx)
        throw original
    }
  }

  /**
   * Iceberg's REST client also returns the base table's 403 for its metadata tables (db.t.refs);
   * Snowflake has no equivalent, so they must not fall back to a table named db.t.refs.
   */
  private def parentOf(ident: Identifier): Identifier =
    Identifier.of(ident.namespace().init, ident.namespace().last)

  private def isMetadataTableOfProtected(ident: Identifier): Boolean =
    ident.namespace().nonEmpty &&
      IcebergMetadataTables.contains(ident.name().toLowerCase(java.util.Locale.ROOT)) && {
        val parent = parentOf(ident)
        try {
          super.loadTable(parent)
          false
        } catch {
          case ex: Throwable => isForbiddenException(ex)
        }
      }

  /**
   * Iceberg's SparkCatalog reports a selector (db.t.branch_x, tag_x, snapshot_id_N,
   * at_timestamp_N) of a 403 table as not found, so check the parent table for a 403.
   */
  private def loadProtectedSelector(ident: Identifier): Option[Table] = {
    if (ident.namespace().isEmpty) {
      return None
    }
    // Outer None: not a selector; inner None: branch_main, the current table
    val selector: Option[Option[IcebergTimeTravel.Spec]] = ident.name() match {
      case SnapshotIdSelector(id) => Some(Some(IcebergTimeTravel.Snapshot(id.toLong)))
      case AtTimestampSelector(ms) =>
        Some(Some(IcebergTimeTravel.TimestampMicros(ms.toLong * 1000L)))
      case BranchSelector(ref) => Some(IcebergTimeTravel.fromBranch(ref))
      case TagSelector(ref) => Some(Some(IcebergTimeTravel.Ref(ref)))
      case _ => None
    }
    selector.flatMap { spec =>
      val parent = parentOf(ident)
      try {
        super.loadTable(parent)
        None
      } catch {
        case ex: Throwable if isForbiddenException(ex) =>
          Some(fallbackToSnowflake(parent, spec, ex))
        case _: Throwable => None
      }
    }
  }


  private def isForbiddenException(ex: Throwable): Boolean = {
    val exceptionClass = ex.getClass.getName
    val message = if (ex.getMessage != null) ex.getMessage else ""
    val causeMessage = Option(ex.getCause).map(_.getMessage).getOrElse("")

    // Check for Iceberg ForbiddenException class
    // TODO: Optimize 403 error handling to avoid unnecessary fallback attempts.
    // Current behavior: When a 403 error occurs, we always attempt a JDBC fallback,
    // which may result in two failed requests if the 403 is due to genuinely invalid
    // credentials rather than FGAC restrictions.
    // Potential optimization: If Horizon IRC includes metadata in the error message
    // indicating whether the 403 is specifically due to FGAC policies (vs authentication
    // failure), we could inspect that metadata to skip the fallback when we know the
    // credentials are invalid. This would reduce latency and avoid redundant failures
    // in the credential error case.
    if (exceptionClass.contains("ForbiddenException")) {
      return true
    }

    // Fallback to message checking for other exception types
    (message.contains("403") || message.contains("Forbidden")) ||
    causeMessage.contains("403") || causeMessage.contains("Forbidden")
  }

  private[snowflake] def resolveDatabase(sessionDatabase: Option[String]): Option[String] =
    snowflakeDatabase.orElse(warehouse.filter(_ => sessionDatabase.isEmpty))

  /**
   * Constructs the dbtable property value from a V2 Identifier.
   * This converts multi-part namespaces into Snowflake's dot-separated format.
   *
   * Examples:
   * - Identifier(["db", "schema"], "table") → "db.schema.table"
   * - Identifier(["schema"], "table") → "schema.table"
   * - Identifier([], "table") → "table"
   * - with database "db", Identifier(["schema"], "table") -> "db.schema.table";
   *   any other namespace length is rejected
   *
   * @param ident The V2 Identifier with namespace and table name
   * @param database The Snowflake database from resolveDatabase, if any
   * @return The fully qualified table name for the dbtable property
   */
  private[snowflake] def buildFullTableName(
      ident: Identifier, database: Option[String]): String = {
    require(database.isEmpty || ident.namespace().length == 1,
      s"Catalog '$catalogName' resolves Snowflake database '${database.get}', which requires " +
        s"a single-level namespace (schema.table), but got: $ident")
    val namespace = if (ident.namespace().nonEmpty) {
      Some((database.toSeq ++ ident.namespace()).mkString("."))
    } else {
      None
    }

    namespace match {
      case Some(ns) => s"$ns.${ident.name()}"
      case None => ident.name()
    }
  }

  /**
   * Creates a CatalogTable using Scala runtime reflection to ensure compatibility
   * across different Spark versions. This approach dynamically discovers constructor
   * parameters and matches them by name, avoiding binary incompatibility issues.
   */
  private def createCatalogTableWithReflection(
      tableIdentifier: TableIdentifier,
      tableType: CatalogTableType,
      storage: CatalogStorageFormat,
      schema: StructType,
      provider: Option[String],
      partitionColumnNames: Seq[String],
      bucketSpec: Option[BucketSpec]
  ): CatalogTable = {
    try {
      val mirror = ru.runtimeMirror(getClass.getClassLoader)
      val cls = mirror.staticClass("org.apache.spark.sql.catalyst.catalog.CatalogTable")

      // Get primary constructor (the one with fewest parameters)
      val ctors = cls.primaryConstructor.alternatives ++ cls.typeSignature.decls.collect {
        case m: ru.MethodSymbol if m.isConstructor => m
      }
      val ctorSymbol = ctors.minBy(_.asMethod.paramLists.flatten.size).asMethod
      val ctorMirror = mirror.reflectClass(cls).reflectConstructor(ctorSymbol)

      val params = ctorSymbol.paramLists.flatten
      log.debug(s"Using CatalogTable constructor with ${params.size} parameters")

      // Log parameter details for debugging
      params.foreach { p =>
        val name = p.name.toString.trim
        val typeStr = p.typeSignature.toString
        log.debug(s"Constructor parameter: $name: $typeStr")
      }

      // Default values for known fields - only the ones we need
      val defaults: Map[String, AnyRef] = Map(
        "identifier" -> tableIdentifier,
        "tableType" -> tableType,
        "storage" -> storage,
        "schema" -> schema,
        "provider" -> provider,  // Keep as Option[String]
        "partitionColumnNames" -> partitionColumnNames,
        "bucketSpec" -> bucketSpec,  // Keep as Option[BucketSpec]
        "owner" -> "",
        "createTime" -> java.lang.Long.valueOf(System.currentTimeMillis()),
        "lastAccessTime" -> java.lang.Long.valueOf(-1L),
        "createVersion" -> "",
        "properties" -> Map.empty[String, String],
        "stats" -> (None: Option[org.apache.spark.sql.catalyst.catalog.CatalogStatistics]),
        "viewText" -> (None: Option[String]),
        "comment" -> (None: Option[String]),
        "collation" -> (None: Option[String]),
        "unsupportedFeatures" -> Seq.empty[String],
        "tracksPartitionsInCatalog" -> java.lang.Boolean.FALSE,
        "schemaPreservesCase" -> java.lang.Boolean.TRUE,
        "ignoredProperties" -> Map.empty[String, String],
        "viewOriginalText" -> (None: Option[String]),
        "entityStorageLocations" -> Seq.empty,
        "resourceName" -> (None: Option[String])
      )

      // Build argument list based on parameter names
      val args = params.map { p =>
        val name = p.name.toString.trim
        val typeStr = p.typeSignature.toString
        val value = defaults.getOrElse(name, {
          log.warn(s"No default value for CatalogTable parameter: $name ($typeStr) - using null")
          null
        })
        log.debug(s"Parameter $name = $value")
        value.asInstanceOf[AnyRef]
      }

      val catalogTable = ctorMirror(args: _*).asInstanceOf[CatalogTable]
      log.debug(s"Successfully created CatalogTable using reflection for table: ${tableIdentifier}")
      catalogTable

    } catch {
      case ex: Exception =>
        log.error(s"Reflection-based CatalogTable creation failed: ${ex.getMessage}", ex)
        throw new RuntimeException(
          s"Failed to create CatalogTable using reflection for ${tableIdentifier}. " +
          s"This may indicate a Spark version compatibility issue.", ex)
    }
  }
  

  private def createSnowflakeV1Table(
      ident: Identifier, timeTravel: Option[IcebergTimeTravel.Spec] = None): Table = {
    val namespace = if (ident.namespace().nonEmpty) Some(ident.namespace().mkString(".")) else None

    // Spark caches the relation per (identifier catalog, database, table), ignoring read
    // options; key each time travel spec separately. The SQL alias doesn't use this catalog.
    val cacheCatalog = timeTravel.fold(catalogName)(spec => s"$catalogName ${spec.clause}")

    // TableIdentifier enforces: catalog.isEmpty || database.isDefined (Spark 3.5+ / 4.x).
    // Only set catalog when we have a database/namespace qualifier.
    val tableIdentifier = namespace match {
      case Some(db) =>
        TableIdentifier(table = ident.name(), database = Some(db), catalog = Some(cacheCatalog))
      case None =>
        TableIdentifier(table = ident.name(), database = None, catalog = None)
    }

    // Get active SparkSession to access SparkConf
    val spark = org.apache.spark.sql.SparkSession.getActiveSession.getOrElse(
      throw new IllegalStateException("No active SparkSession found")
    )

    // Read options (snapshot-id, branch, ...) are applied when the relation is created, so drop
    // a relation cached by an earlier read of this table
    if (timeTravel.isEmpty) {
      namespace.foreach { db =>
        spark.sessionState.catalog.invalidateCachedTable(
          QualifiedTableName(catalogName, db, ident.name()))
      }
    }

    val (properties, _) = snowflakeParameters(ident, timeTravel)
    val storage = CatalogStorageFormat(
      locationUri = None,
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      properties = properties
    )

    // Use reflection-based creation to ensure compatibility across Spark versions
    val catalogTable = createCatalogTableWithReflection(
      tableIdentifier = tableIdentifier,
      tableType = CatalogTableType.EXTERNAL,
      storage = storage,
      schema = StructType(Seq.empty), // Empty schema, V1Table will use its own schema
      provider = Some(classOf[FallbackDefaultSource].getName),
      partitionColumnNames = Seq.empty,
      bucketSpec = None
    )

    new V1Table(catalogTable)
  }

  /** A protected table as an Iceberg SparkTable backed by Snowflake (snowflake-fallback-mode). */
  private def createSnowflakeSparkTable(
      ident: Identifier, timeTravel: Option[IcebergTimeTravel.Spec]): Table = {
    val (params, fullTableName) = snowflakeParameters(ident, timeTravel)
    val sql = newSnowflakeSql(params)
    lazy val sparkTable: SnowflakeSparkTable = new SnowflakeSparkTable(
      new SnowflakeIcebergTable((catalogName +: ident.namespace() :+ ident.name()).mkString("."),
        fullTableName, sparkTable.schema(), sql),
      params, timeTravel)
    sparkTable
  }

  protected def newSnowflakeSql(params: Map[String, String]): SnowflakeSql =
    new JdbcSnowflakeSql(params)

  /**
   * The Snowflake connector parameters for a protected table (credentials from SparkConf only,
   * dbtable, or query for a time travel read), and its Snowflake name.
   */
  private def snowflakeParameters(
      ident: Identifier, timeTravel: Option[IcebergTimeTravel.Spec]): (Map[String, String], String) = {
    val spark = org.apache.spark.sql.SparkSession.getActiveSession.getOrElse(
      throw new IllegalStateException("No active SparkSession found")
    )
    val allConfs =
      spark.sparkContext.getConf.getAll.toMap  // Hardening: use ONLY immutable SparkConf; spark.conf/sessionState.conf are runtime SQLConf settable via SET and must not feed privileged connector options  // Hardening: sessionState.conf removed — it is user-mutable via SET and must not be merged into privileged connector options

    // Get Snowflake connection properties from SparkConf only
    val snowflakeProps = allConfs.filter { case (key, _) =>
      key.toLowerCase.startsWith("spark.snowflake.") || key.toLowerCase.startsWith("snowflake.")
    }.map { case (key, value) =>
      val cleanKey = if (key.toLowerCase.startsWith("spark.snowflake.")) {
        key.substring("spark.snowflake.".length).toLowerCase
      } else if (key.toLowerCase.startsWith("snowflake.")) {
        key.substring("snowflake.".length).toLowerCase
      } else {
        key.toLowerCase
      }
      cleanKey -> value
    }

    log.info(s"Snowflake properties for V1Table " +
      s"(${snowflakeProps.size} keys): ${snowflakeProps.keys.mkString(", ")}")

    // Build full table name for dbtable using the buildFullTableName method
    val database = resolveDatabase(snowflakeProps.get(Parameters.PARAM_SF_DATABASE))
    val fullTableName = buildFullTableName(ident, database)

    (database.map(Parameters.PARAM_SF_DATABASE -> _).toMap ++ snowflakeProps +
      timeTravel.fold(Parameters.PARAM_SF_DBTABLE -> fullTableName)(spec =>
        Parameters.PARAM_SF_QUERY -> IcebergTimeTravel.query(fullTableName, spec)) ++
      // Iceberg's refs columns are lowercase; the connector would otherwise quote them ("name")
      (if (timeTravel.contains(IcebergTimeTravel.Refs)) {
        Map(Parameters.PARAM_KEEP_COLUMN_CASE -> "on")
      } else {
        Map.empty
      }),
      fullTableName)
  }



  override def tableExists(ident: Identifier): Boolean = {
    try {
      super.tableExists(ident)
    } catch {
      case _: Exception => false
    }
  }
}
