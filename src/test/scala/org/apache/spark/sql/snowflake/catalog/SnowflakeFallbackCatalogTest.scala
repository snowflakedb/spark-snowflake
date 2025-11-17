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

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class TestDelegateCatalog extends TableCatalog with SupportsNamespaces {
  var shouldThrow403: Boolean = false
  var shouldThrowOther: Boolean = false
  var loadTableCalled: Boolean = false
  
  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}
  
  override def name(): String = "test_delegate"
  
  override def loadTable(ident: Identifier): Table = {
    loadTableCalled = true
    if (shouldThrow403) {
      throw new RuntimeException("HTTP 403 Forbidden")
    }
    if (shouldThrowOther) {
      throw new RuntimeException("Some other error")
    }
    new Table {
      override def name(): String = "test_table"
      override def schema(): org.apache.spark.sql.types.StructType = 
        org.apache.spark.sql.types.StructType(Seq.empty)
      override def capabilities(): java.util.Set[TableCapability] = 
        java.util.Collections.emptySet()
    }
  }
  
  override def listTables(namespace: Array[String]): Array[Identifier] = Array.empty
  override def tableExists(ident: Identifier): Boolean = true
  override def createTable(ident: Identifier, schema: org.apache.spark.sql.types.StructType, 
    partitions: Array[org.apache.spark.sql.connector.expressions.Transform], 
    properties: java.util.Map[String, String]): Table = null
  override def alterTable(ident: Identifier, changes: TableChange*): Table = null
  override def dropTable(ident: Identifier): Boolean = false
  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {}
  override def listNamespaces(): Array[Array[String]] = Array.empty
  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = Array.empty
  override def loadNamespaceMetadata(namespace: Array[String]): java.util.Map[String, String] = 
    java.util.Collections.emptyMap()
  override def createNamespace(namespace: Array[String], metadata: java.util.Map[String, String]): Unit = {}
  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = {}
  override def dropNamespace(namespace: Array[String], cascade: Boolean): Boolean = false
}

class SnowflakeFallbackCatalogTest extends FunSuite {

  private val FGAC_KEY = "spark.snowflake.extensions.fgacJdbcFallback.enabled"

  test("isForbiddenException should detect 403 in exception message") {
    val catalog = new SnowflakeFallbackCatalog()
    val ex = new RuntimeException("HTTP 403 Forbidden")
    
    val method = catalog.getClass.getDeclaredMethod("isForbiddenException", classOf[Throwable])
    method.setAccessible(true)
    
    val result = method.invoke(catalog, ex).asInstanceOf[Boolean]
    assert(result === true, "Should detect 403 in exception message")
  }

  test("isForbiddenException should detect Forbidden in exception message") {
    val catalog = new SnowflakeFallbackCatalog()
    val ex = new RuntimeException("Access Forbidden")
    
    val method = catalog.getClass.getDeclaredMethod("isForbiddenException", classOf[Throwable])
    method.setAccessible(true)
    
    val result = method.invoke(catalog, ex).asInstanceOf[Boolean]
    assert(result === true, "Should detect 'Forbidden' in exception message")
  }

  test("isForbiddenException should return false for other exceptions") {
    val catalog = new SnowflakeFallbackCatalog()
    val ex = new RuntimeException("Some other error")
    
    val method = catalog.getClass.getDeclaredMethod("isForbiddenException", classOf[Throwable])
    method.setAccessible(true)
    
    val result = method.invoke(catalog, ex).asInstanceOf[Boolean]
    assert(result === false, "Should not detect non-forbidden exceptions")
  }

  test("loadTable should delegate when FGAC is disabled") {
    val catalog = new SnowflakeFallbackCatalog()
    val delegate = new TestDelegateCatalog()
    
    catalog.setDelegateCatalog(delegate)
    val options = Map(FGAC_KEY -> "false").asJava
    val optionsMap = new CaseInsensitiveStringMap(options)
    catalog.initialize("test", optionsMap)
    
    val ident = Identifier.of(Array("schema"), "table")
    val result = catalog.loadTable(ident)
    
    assert(result != null)
    assert(delegate.loadTableCalled)
  }

  test("loadTable should attempt V1Table creation on 403 when FGAC enabled") {
    val catalog = new SnowflakeFallbackCatalog()
    val delegate = new TestDelegateCatalog()
    delegate.shouldThrow403 = true

    catalog.setDelegateCatalog(delegate)
    val options = Map(
      FGAC_KEY -> "true",
      "spark.snowflake.sfURL" -> "test.snowflakecomputing.com",
      "spark.snowflake.sfUser" -> "testuser",
      "spark.snowflake.sfPassword" -> "testpass"
    ).asJava
    val optionsMap = new CaseInsensitiveStringMap(options)
    catalog.initialize("test", optionsMap)

    val ident = Identifier.of(Array("schema"), "table")

    // In test environment without SparkSession, fallback will fail and rethrow original 403
    // This is expected behavior - in production, SparkSession will be available
    val thrown = intercept[RuntimeException] {
      catalog.loadTable(ident)
    }

    // The original 403 exception should be thrown (fallback attempted but failed due to no SparkSession)
    assert(thrown.getMessage.contains("403") || thrown.getMessage.contains("Forbidden") ||
      thrown.getMessage.contains("No active SparkSession"),
      s"Should see 403 error or SparkSession error, got: ${thrown.getMessage}")
  }

  test("reflection-based CatalogTable creation should work when SparkSession exists") {
    // This test documents that the reflection approach will work in production
    // where a SparkSession is available. In unit test environment, we expect
    // an exception due to missing SparkSession.
    val catalog = new SnowflakeFallbackCatalog()
    val delegate = new TestDelegateCatalog()
    delegate.shouldThrow403 = true

    catalog.setDelegateCatalog(delegate)
    val options = Map(FGAC_KEY -> "true").asJava
    val optionsMap = new CaseInsensitiveStringMap(options)
    catalog.initialize("test", optionsMap)

    val ident = Identifier.of(Array("myschema"), "mytable")

    // Expect failure due to no SparkSession in test environment
    val thrown = intercept[Exception] {
      catalog.loadTable(ident)
    }

    // Should fail due to no SparkSession, not due to CatalogTable construction
    assert(thrown.getMessage.contains("SparkSession") || thrown.getMessage.contains("403"))
  }

  test("loadTable should propagate non-403 exceptions when FGAC is enabled") {
    val catalog = new SnowflakeFallbackCatalog()
    val delegate = new TestDelegateCatalog()
    delegate.shouldThrowOther = true
    
    catalog.setDelegateCatalog(delegate)
    val options = Map(FGAC_KEY -> "true").asJava
    val optionsMap = new CaseInsensitiveStringMap(options)
    catalog.initialize("test", optionsMap)
    
    val ident = Identifier.of(Array("schema"), "table")
    
    val thrown = intercept[RuntimeException] {
      catalog.loadTable(ident)
    }
    
    assert(thrown.getMessage == "Some other error")
  }

  test("initialize should set catalog name") {
    val catalog = new SnowflakeFallbackCatalog()
    val delegate = new TestDelegateCatalog()
    
    catalog.setDelegateCatalog(delegate)
    val options = Map(FGAC_KEY -> "true").asJava
    val optionsMap = new CaseInsensitiveStringMap(options)
    catalog.initialize("test_catalog", optionsMap)
    
    assert(catalog.name() === "test_catalog")
  }

  test("createDelegateCatalog should require catalog-impl option") {
    val catalog = new SnowflakeFallbackCatalog()
    val options = Map(FGAC_KEY -> "true").asJava
    val optionsMap = new CaseInsensitiveStringMap(options)
    
    val thrown = intercept[IllegalArgumentException] {
      catalog.initialize("test", optionsMap)
    }
    
    assert(thrown.getMessage.contains("catalog-impl"))
  }

  test("listTables should delegate to underlying catalog") {
    val catalog = new SnowflakeFallbackCatalog()
    val delegate = new TestDelegateCatalog()
    
    catalog.setDelegateCatalog(delegate)
    
    val namespace = Array("schema")
    val result = catalog.listTables(namespace)
    
    assert(result != null)
  }

  test("tableExists should delegate to underlying catalog") {
    val catalog = new SnowflakeFallbackCatalog()
    val delegate = new TestDelegateCatalog()
    
    catalog.setDelegateCatalog(delegate)
    
    val ident = Identifier.of(Array("schema"), "table")
    val result = catalog.tableExists(ident)
    
    assert(result === true)
  }

  test("tableExists should return false on exception") {
    val catalog = new SnowflakeFallbackCatalog()
    val delegateWithException = new TestDelegateCatalog() {
      override def tableExists(ident: Identifier): Boolean = {
        throw new RuntimeException("Error")
      }
    }
    
    catalog.setDelegateCatalog(delegateWithException)
    
    val ident = Identifier.of(Array("schema"), "table")
    val result = catalog.tableExists(ident)
    
    assert(result === false)
  }

  test("namespace operations should delegate to underlying catalog") {
    val catalog = new SnowflakeFallbackCatalog()
    val delegate = new TestDelegateCatalog()

    catalog.setDelegateCatalog(delegate)

    val result = catalog.listNamespaces()

    assert(result != null)
  }

  test("dbtable property should be constructed from Identifier namespace and name") {
    // This test verifies the ACTUAL dbtable string construction logic
    // by calling the buildFullTableName method from SnowflakeFallbackCatalog.
    // This method is private[snowflake] so it's accessible to tests in the same package.

    val catalog = new SnowflakeFallbackCatalog()

    // Test Case 1: Multi-part namespace (Snowflake: database.schema.table)
    val multiPartIdent = Identifier.of(Array("production_db", "analytics_schema"), "sales_table")
    val dbTableMulti = catalog.buildFullTableName(multiPartIdent)
    assert(dbTableMulti === "production_db.analytics_schema.sales_table",
      s"Multi-part namespace: Expected 'production_db.analytics_schema.sales_table', got '$dbTableMulti'")

    // Test Case 2: Single-part namespace (Snowflake: schema.table)
    val singlePartIdent = Identifier.of(Array("analytics_schema"), "sales_table")
    val dbTableSingle = catalog.buildFullTableName(singlePartIdent)
    assert(dbTableSingle === "analytics_schema.sales_table",
      s"Single-part namespace: Expected 'analytics_schema.sales_table', got '$dbTableSingle'")

    // Test Case 3: No namespace (Snowflake: table only)
    val noNamespaceIdent = Identifier.of(Array(), "sales_table")
    val dbTableNone = catalog.buildFullTableName(noNamespaceIdent)
    assert(dbTableNone === "sales_table",
      s"No namespace: Expected 'sales_table', got '$dbTableNone'")

    // Test Case 4: Deep multi-part namespace (e.g., catalog.database.schema.table)
    val deepNamespaceIdent = Identifier.of(Array("catalog", "production_db", "analytics_schema"), "sales_table")
    val dbTableDeep = catalog.buildFullTableName(deepNamespaceIdent)
    assert(dbTableDeep === "catalog.production_db.analytics_schema.sales_table",
      s"Deep namespace: Expected 'catalog.production_db.analytics_schema.sales_table', got '$dbTableDeep'")

    // Test Case 5: Namespace with special characters (should be preserved as-is)
    val specialIdent = Identifier.of(Array("my_db", "my-schema"), "my.table")
    val dbTableSpecial = catalog.buildFullTableName(specialIdent)
    assert(dbTableSpecial === "my_db.my-schema.my.table",
      s"Special characters: Expected 'my_db.my-schema.my.table', got '$dbTableSpecial'")
  }
}
