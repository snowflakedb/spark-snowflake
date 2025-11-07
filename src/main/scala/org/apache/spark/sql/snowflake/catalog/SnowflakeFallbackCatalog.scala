package org.apache.spark.sql.snowflake.catalog

import net.snowflake.spark.snowflake.Utils
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class SnowflakeFallbackCatalog extends CatalogExtension with SupportsNamespaces {
  
  private val log = LoggerFactory.getLogger(getClass)
  private var delegateCatalog: CatalogPlugin = _
  private var catalogName: String = _
  private var optionsMap: CaseInsensitiveStringMap = _
  private var options: Map[String, String] = Map.empty

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    this.catalogName = name
    this.optionsMap = options
    this.options = options.asScala.toMap
    log.info("Initializing SnowflakeFallbackCatalog: {}", name)
    
    // Create delegate catalog if not already set by Spark
    if (delegateCatalog == null) {
      delegateCatalog = createDelegateCatalog(name, options)
    }
  }

  override def setDelegateCatalog(catalog: CatalogPlugin): Unit = {
    this.delegateCatalog = catalog
    log.debug("Delegate catalog set by Spark: {}", catalog.getClass.getName)
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
      delegateCatalog.asInstanceOf[TableCatalog].loadTable(ident)
    } catch {
      case ex: Throwable =>
        if (isForbiddenException(ex)) {
          log.info("Access forbidden for table {}, falling back to Snowflake JDBC", ident.toString)
          val db = if (ident.namespace().length > 0) ident.namespace().mkString(".") else ""
          throw new FGACForbiddenException(db, ident.name(), Some(ex))
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

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    delegateCatalog.asInstanceOf[TableCatalog].listTables(namespace)
  }

  override def tableExists(ident: Identifier): Boolean = {
    try {
      delegateCatalog.asInstanceOf[TableCatalog].tableExists(ident)
    } catch {
      case _: Exception => false
    }
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = {
    delegateCatalog.asInstanceOf[TableCatalog].createTable(ident, schema, partitions, properties)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    delegateCatalog.asInstanceOf[TableCatalog].alterTable(ident, changes: _*)
  }

  override def dropTable(ident: Identifier): Boolean = {
    delegateCatalog.asInstanceOf[TableCatalog].dropTable(ident)
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    delegateCatalog.asInstanceOf[TableCatalog].renameTable(oldIdent, newIdent)
  }

  override def listNamespaces(): Array[Array[String]] = {
    delegateCatalog.asInstanceOf[SupportsNamespaces].listNamespaces()
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    delegateCatalog.asInstanceOf[SupportsNamespaces].listNamespaces(namespace)
  }

  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] = {
    delegateCatalog.asInstanceOf[SupportsNamespaces].loadNamespaceMetadata(namespace)
  }

  override def createNamespace(
      namespace: Array[String], metadata: java.util.Map[String, String]): Unit = {
    delegateCatalog.asInstanceOf[SupportsNamespaces].createNamespace(namespace, metadata)
  }

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {
    delegateCatalog.asInstanceOf[SupportsNamespaces].alterNamespace(namespace, changes: _*)
  }

  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = {
    delegateCatalog.asInstanceOf[SupportsNamespaces].dropNamespace(namespace, cascade)
  }

  def listFunctions(namespace: Array[String]): Array[Identifier] = {
    delegateCatalog.asInstanceOf[FunctionCatalog].listFunctions(namespace)
  }

  def loadFunction(
      ident: Identifier): org.apache.spark.sql.connector.catalog.functions.UnboundFunction = {
    delegateCatalog.asInstanceOf[FunctionCatalog].loadFunction(ident)
  }
}
