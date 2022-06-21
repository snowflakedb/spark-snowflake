package net.snowflake.spark.snowflake.catalog

import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import net.snowflake.spark.snowflake.Parameters.{MergedParameters, PARAM_SF_DATABASE, PARAM_SF_DBTABLE, PARAM_SF_SCHEMA}
import net.snowflake.spark.snowflake.{DefaultJDBCWrapper, Parameters}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{Identifier, NamespaceChange, SupportsNamespaces, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.sql.SQLException
import scala.collection.convert.ImplicitConversions.`map AsScala`
import scala.collection.mutable.ArrayBuilder

class SfCatalog  extends TableCatalog with Logging with SupportsNamespaces{
  var catalogName: String = null
  var params: MergedParameters = _
  val jdbcWrapper = DefaultJDBCWrapper

  override def name(): String = {
    require(catalogName != null, "The SfCatalog is not initialed")
    catalogName
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    val map = options.asCaseSensitiveMap().toMap
    //  to pass the check
    params = Parameters.mergeParameters(map +
      (PARAM_SF_DATABASE -> "__invalid_database") +
      (PARAM_SF_SCHEMA -> "__invalid_schema") +
      (PARAM_SF_DBTABLE -> "__invalid_dbtable"))
    catalogName = name
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
      checkNamespace(namespace)
    val catalog = if (namespace.length == 2) namespace(0) else null
    val schemaPattern = if (namespace.length == 2) namespace(1) else null
      val rs = DefaultJDBCWrapper.getConnector(params).getMetaData()
        .getTables(catalog, schemaPattern, "%", Array("TABLE"))
      new Iterator[Identifier] {
        def hasNext = rs.next()
        def next() = Identifier.of(namespace, rs.getString("TABLE_NAME"))
      }.toArray

  }

  override def tableExists(ident: Identifier): Boolean = {
    checkNamespace(ident.namespace())
    DefaultJDBCWrapper.tableExists(params, getFullTableName(ident))
  }

  override def dropTable(ident: Identifier): Boolean = {
    checkNamespace(ident.namespace())
    val conn = DefaultJDBCWrapper.getConnector(params)
    conn.dropTable(getFullTableName(ident))
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    checkNamespace(oldIdent.namespace())
    val conn = DefaultJDBCWrapper.getConnector(params)
    conn.renameTable(getFullTableName(newIdent), getFullTableName(newIdent))
  }

  override def loadTable(ident: Identifier): Table = {
    checkNamespace(ident.namespace())
    val map = params.parameters
    params = Parameters.mergeParameters(map +
      (PARAM_SF_DBTABLE -> getTableName(ident)) +
      (PARAM_SF_DATABASE -> getDatabase(ident)) +
      (PARAM_SF_SCHEMA -> getSchema(ident)))
    try {
      SfTable(ident, jdbcWrapper, params)
    } catch {
      case _: SQLException => throw QueryCompilationErrors.noSuchTableError(ident)
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  override def namespaceExists(namespace: Array[String]): Boolean = namespace match {
    case Array(catalog, schema) =>
      val rs = DefaultJDBCWrapper.getConnector(params).getMetaData().getSchemas(catalog, schema)

      while (rs.next()) {
        val tableSchema = rs.getString("TABLE_SCHEM")
        if (tableSchema == schema) return true
      }
      false
    case _ => false
  }

  override def listNamespaces(): Array[Array[String]] = {
      val schemaBuilder = ArrayBuilder.make[Array[String]]
      val rs = DefaultJDBCWrapper.getConnector(params).getMetaData().getSchemas()
      while (rs.next()) {
        schemaBuilder += Array(rs.getString("TABLE_SCHEM"))
      }
      schemaBuilder.result
  }

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() =>
        listNamespaces()
      case Array(_, _) if namespaceExists(namespace) =>
        Array()
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] = {
    namespace match {
      case Array(catalog, schema) =>
        if (!namespaceExists(namespace)) {
          throw QueryCompilationErrors.noSuchNamespaceError(Array(catalog, schema))
        }
        new java.util.HashMap[String, String]()
      case _ =>
        throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def createTable(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: java.util.Map[String, String]): Table = ???

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = ???

  override def dropNamespace(namespace: Array[String]): Boolean = ???

  private def checkNamespace(namespace: Array[String]): Unit = {
    // a database and schema comprise a namespace in Snowflake
    if (namespace.length != 2) {
      throw QueryCompilationErrors.noSuchNamespaceError(namespace)
    }
  }

  override def createNamespace(
                                namespace: Array[String],
                                metadata: java.util.Map[String, String]): Unit = ???

  private def getTableName(ident: Identifier): String = {
    (ident.name())
  }
  private def getDatabase(ident: Identifier): String = {
    (ident.namespace())(0)
  }
  private def getSchema(ident: Identifier): String = {
    (ident.namespace())(1)
  }
  private def getFullTableName(ident: Identifier): String = {
    (ident.namespace() :+ ident.name()).mkString(".")

  }
}
