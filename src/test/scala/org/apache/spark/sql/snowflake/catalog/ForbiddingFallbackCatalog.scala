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

import java.util.{List => JList, Map => JMap, Set => JSet}

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.{CatalogUtil, Schema, Table}
import org.apache.iceberg.catalog.{Catalog, Namespace, SupportsNamespaces, TableIdentifier}
import org.apache.iceberg.exceptions.ForbiddenException
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

/**
 * Test-only SnowflakeFallbackCatalog whose Iceberg catalog simulates Horizon IRC denying access
 * to policy-protected tables: it throws ForbiddenException from loadTable/tableExists for every
 * table listed in `forbidden-tables`, as Iceberg's REST client does on a 403. Tables listed in
 * `failing-tables` throw a non-403 error.
 *
 * Tables live in a Hadoop catalog at `hadoop-warehouse`, so `warehouse` stays free to carry the
 * Snowflake database name, like Horizon.
 */
class ForbiddingFallbackCatalog extends SnowflakeFallbackCatalog {
  // The options this catalog passed on to Iceberg
  var icebergOptions: CaseInsensitiveStringMap = _

  override protected def buildIcebergCatalog(
      name: String, options: CaseInsensitiveStringMap): Catalog = {
    icebergOptions = configure(name, options)
    def tables(key: String): Set[String] = Option(options.get(key)).toSeq
      .flatMap(_.split(",")).map(_.trim).filter(_.nonEmpty).toSet
    val warehouse = Option(options.get("hadoop-warehouse")).getOrElse(
      java.nio.file.Files.createTempDirectory("forbidding-catalog").toUri.toString)
    val hadoop = CatalogUtil.buildIcebergCatalog(name,
      Map("type" -> "hadoop", "warehouse" -> warehouse).asJava, new Configuration())
    new ForbiddingCatalog(hadoop, hadoop.asInstanceOf[SupportsNamespaces],
      tables("forbidden-tables"), tables("failing-tables"))
  }
}

class ForbiddingCatalog(
    tables: Catalog, namespaces: SupportsNamespaces, forbidden: Set[String],
    failing: Set[String] = Set.empty)
  extends Catalog with SupportsNamespaces {

  // Horizon's REST client also 403s metadata tables (t.snapshots, t.refs) of a protected table
  private def isMetadataOfForbidden(ident: TableIdentifier): Boolean =
    scala.util.Try(org.apache.iceberg.MetadataTableType.from(ident.name())).toOption
      .exists(_ != null) && forbidden.contains(ident.namespace().toString)
  private def check(ident: TableIdentifier): Unit = {
    if (forbidden.contains(ident.toString) || isMetadataOfForbidden(ident)) {
      throw new ForbiddenException("Forbidden: Access to table %s is denied by policy", ident)
    }
    if (failing.contains(ident.toString)) throw new RuntimeException("Some other error")
  }

  override def name(): String = tables.name()
  override def loadTable(ident: TableIdentifier): Table = { check(ident); tables.loadTable(ident) }
  override def tableExists(ident: TableIdentifier): Boolean = {
    check(ident); tables.tableExists(ident)
  }
  override def buildTable(ident: TableIdentifier, schema: Schema): Catalog.TableBuilder =
    tables.buildTable(ident, schema)
  override def listTables(ns: Namespace): JList[TableIdentifier] = tables.listTables(ns)
  override def dropTable(ident: TableIdentifier, purge: Boolean): Boolean =
    tables.dropTable(ident, purge)
  override def renameTable(from: TableIdentifier, to: TableIdentifier): Unit =
    tables.renameTable(from, to)
  override def invalidateTable(ident: TableIdentifier): Unit = tables.invalidateTable(ident)

  override def createNamespace(ns: Namespace, meta: JMap[String, String]): Unit =
    namespaces.createNamespace(ns, meta)
  override def listNamespaces(ns: Namespace): JList[Namespace] = namespaces.listNamespaces(ns)
  override def loadNamespaceMetadata(ns: Namespace): JMap[String, String] =
    namespaces.loadNamespaceMetadata(ns)
  override def dropNamespace(ns: Namespace): Boolean = namespaces.dropNamespace(ns)
  override def setProperties(ns: Namespace, props: JMap[String, String]): Boolean =
    namespaces.setProperties(ns, props)
  override def removeProperties(ns: Namespace, keys: JSet[String]): Boolean =
    namespaces.removeProperties(ns, keys)
  override def namespaceExists(ns: Namespace): Boolean = namespaces.namespaceExists(ns)
}
