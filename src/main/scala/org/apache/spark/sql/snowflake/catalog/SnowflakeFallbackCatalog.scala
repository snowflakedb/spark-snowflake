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

import net.snowflake.spark.snowflake.Utils
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.reflect.runtime.{universe => ru}

class SnowflakeFallbackCatalog extends CatalogExtension with SupportsNamespaces {

  private val log = LoggerFactory.getLogger(getClass)
  private var delegateCatalog: CatalogPlugin = _
  private var catalogName: String = _

  // Typed delegate fields for type-safe access (avoids asInstanceOf casts)
  // TableCatalog is REQUIRED - validated in setDelegateCatalog
  private var tableCatalog: TableCatalog = _

  // Optional interfaces - null if delegate doesn't implement them
  // Methods will throw UnsupportedOperationException if accessed when null
  private var supportsNamespaces: SupportsNamespaces = _
  private var functionCatalog: FunctionCatalog = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    log.info("Initializing SnowflakeFallbackCatalog: {}", name)

    // Create delegate catalog if not already set by Spark
    if (delegateCatalog == null) {
      val delegate = createDelegateCatalog(name, options)
      setDelegateCatalog(delegate)  // This populates typed fields
    }
  }

  override def setDelegateCatalog(catalog: CatalogPlugin): Unit = {
    this.delegateCatalog = catalog

    // REQUIRED: Validate TableCatalog interface
    catalog match {
      case tc: TableCatalog =>
        tableCatalog = tc
        log.debug("Delegate implements TableCatalog")
      case _ =>
        throw new IllegalArgumentException(
          s"Delegate catalog must implement TableCatalog, got ${catalog.getClass.getName}"
        )
    }

    // OPTIONAL: Check for SupportsNamespaces
    supportsNamespaces = catalog match {
      case ns: SupportsNamespaces =>
        log.debug("Delegate implements SupportsNamespaces")
        ns
      case _ =>
        log.debug("Delegate does not implement SupportsNamespaces")
        null
    }

    // OPTIONAL: Check for FunctionCatalog
    functionCatalog = catalog match {
      case fc: FunctionCatalog =>
        log.debug("Delegate implements FunctionCatalog")
        fc
      case _ =>
        log.debug("Delegate does not implement FunctionCatalog")
        null
    }

    log.debug("Delegate catalog set: {}", catalog.getClass.getName)
  }
  
  private def createDelegateCatalog(
    name: String, options: CaseInsensitiveStringMap): CatalogPlugin = {
    val catalogImpl = options.get("catalog-impl")
    
    if (catalogImpl == null) {
      throw new IllegalArgumentException(
        s"Catalog '$name' must specify 'catalog-impl' option to define the delegate catalog")
    }
    
    log.debug("Creating delegate catalog: {}", catalogImpl)
    
    try {
      val delegateClass = Utils.classForName(catalogImpl)
      val delegate =
          delegateClass.getDeclaredConstructor().newInstance().asInstanceOf[CatalogPlugin]
      // Filter out wrapper-specific options before passing to delegate
      val wrapperSpecificKeys = Set("catalog-impl")
      val delegateOptions = options.asScala
        .filterKeys(key => !wrapperSpecificKeys.contains(key.toLowerCase))
        .toMap
      
      val delegateOptionsMap = new CaseInsensitiveStringMap(delegateOptions.asJava)
      
      log.debug(
          "Initializing delegate with filtered options: {}", delegateOptions.keys.mkString(", "))
      delegate.initialize(name, delegateOptionsMap)
      
      log.debug("Successfully initialized delegate catalog: {}", catalogImpl)
      delegate
    } catch {
      case ex: Exception =>
        log.error(s"Failed to create delegate catalog: $catalogImpl", ex)
        throw new RuntimeException(s"Failed to create delegate catalog: $catalogImpl", ex)
    }
  }

  override def name(): String = catalogName

  override def loadTable(ident: Identifier): Table = {
    try {
      tableCatalog.loadTable(ident)
    } catch {
      case ex: Throwable =>
        if (isForbiddenException(ex)) {
          log.info(s"403 Forbidden detected for ${ident}, attempting JDBC fallback")
          try {
            createSnowflakeV1Table(ident)
          } catch {
            case fallbackEx: Throwable =>
              // rethrow original exception if fallback also fails
              log.error(s"Failed to create V1Table fallback for ${ident}", fallbackEx)
              throw ex
          }
        } else {
          throw ex
        }
    }
  }
  
  private def isForbiddenException(ex: Throwable): Boolean = {
    val exceptionClass = ex.getClass.getName
    val message = if (ex.getMessage != null) ex.getMessage else ""
    val causeMessage = Option(ex.getCause).map(_.getMessage).getOrElse("")

    // Check for Iceberg ForbiddenException class
    // TODO: Optimize if the ERROR message sent by Horizon IRC contains
    // info that 403 is due to FGAC.
    if (exceptionClass.contains("ForbiddenException")) {
      return true
    }

    // Fallback to message checking for other exception types
    (message.contains("403") || message.contains("Forbidden")) ||
    causeMessage.contains("403") || causeMessage.contains("Forbidden")
  }

  /**
   * Constructs the dbtable property value from a V2 Identifier.
   * This converts multi-part namespaces into Snowflake's dot-separated format.
   *
   * Examples:
   * - Identifier(["db", "schema"], "table") → "db.schema.table"
   * - Identifier(["schema"], "table") → "schema.table"
   * - Identifier([], "table") → "table"
   *
   * @param ident The V2 Identifier with namespace and table name
   * @return The fully qualified table name for the dbtable property
   */
  private[snowflake] def buildFullTableName(ident: Identifier): String = {
    val namespace = if (ident.namespace().nonEmpty) {
      Some(ident.namespace().mkString("."))
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
  
  private def createSnowflakeV1Table(ident: Identifier): Table = {
    val namespace = if (ident.namespace().nonEmpty) Some(ident.namespace().mkString(".")) else None

    // TableIdentifier needs catalog field set for V2 catalogs
    val tableIdentifier = TableIdentifier(
      table = ident.name(),
      database = namespace,
      catalog = Some(catalogName) // Set catalog name!
    )

    // Get active SparkSession to access SparkConf
    val spark = org.apache.spark.sql.SparkSession.getActiveSession.getOrElse(
      throw new IllegalStateException("No active SparkSession found")
    )

    val allConfs =
      spark.sparkContext.getConf.getAll.toMap ++ spark.conf.getAll ++ spark.sessionState.conf.getAllConfs

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
    val fullTableName = buildFullTableName(ident)

    val storage = CatalogStorageFormat(
      locationUri = None,
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      properties = snowflakeProps + ("dbtable" -> fullTableName)
    )

    // Use reflection-based creation to ensure compatibility across Spark versions
    val catalogTable = createCatalogTableWithReflection(
      tableIdentifier = tableIdentifier,
      tableType = CatalogTableType.EXTERNAL,
      storage = storage,
      schema = StructType(Seq.empty), // Empty schema, V1Table will use its own schema
      provider = Some("net.snowflake.spark.snowflake.DefaultSource"),
      partitionColumnNames = Seq.empty,
      bucketSpec = None
    )

    new V1Table(catalogTable)
  }


  override def listTables(namespace: Array[String]): Array[Identifier] = {
    tableCatalog.listTables(namespace)
  }

  override def tableExists(ident: Identifier): Boolean = {
    try {
      tableCatalog.tableExists(ident)
    } catch {
      case _: Exception => false
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    tableCatalog.createTable(ident, schema, partitions, properties)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    tableCatalog.alterTable(ident, changes: _*)
  }

  override def dropTable(ident: Identifier): Boolean = {
    tableCatalog.dropTable(ident)
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    tableCatalog.renameTable(oldIdent, newIdent)
  }

  override def listNamespaces(): Array[Array[String]] = {
    if (supportsNamespaces == null) {
      throw new UnsupportedOperationException(
        s"Delegate catalog ${delegateCatalog.getClass.getName} does not support namespaces"
      )
    }
    supportsNamespaces.listNamespaces()
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    if (supportsNamespaces == null) {
      throw new UnsupportedOperationException(
        s"Delegate catalog ${delegateCatalog.getClass.getName} does not support namespaces"
      )
    }
    supportsNamespaces.listNamespaces(namespace)
  }

  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] = {
    if (supportsNamespaces == null) {
      throw new UnsupportedOperationException(
        s"Delegate catalog ${delegateCatalog.getClass.getName} does not support namespaces"
      )
    }
    supportsNamespaces.loadNamespaceMetadata(namespace)
  }

  override def createNamespace(
      namespace: Array[String], metadata: java.util.Map[String, String]): Unit = {
    if (supportsNamespaces == null) {
      throw new UnsupportedOperationException(
        s"Delegate catalog ${delegateCatalog.getClass.getName} does not support namespaces"
      )
    }
    supportsNamespaces.createNamespace(namespace, metadata)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    if (supportsNamespaces == null) {
      throw new UnsupportedOperationException(
        s"Delegate catalog ${delegateCatalog.getClass.getName} does not support namespaces"
      )
    }
    supportsNamespaces.alterNamespace(namespace, changes: _*)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    if (supportsNamespaces == null) {
      throw new UnsupportedOperationException(
        s"Delegate catalog ${delegateCatalog.getClass.getName} does not support namespaces"
      )
    }
    supportsNamespaces.dropNamespace(namespace, cascade)
  }

  def listFunctions(namespace: Array[String]): Array[Identifier] = {
    if (functionCatalog == null) {
      throw new UnsupportedOperationException(
        s"Delegate catalog ${delegateCatalog.getClass.getName} does not support functions"
      )
    }
    functionCatalog.listFunctions(namespace)
  }

  def loadFunction(
      ident: Identifier): org.apache.spark.sql.connector.catalog.functions.UnboundFunction = {
    if (functionCatalog == null) {
      throw new UnsupportedOperationException(
        s"Delegate catalog ${delegateCatalog.getClass.getName} does not support functions"
      )
    }
    functionCatalog.loadFunction(ident)
  }
}
