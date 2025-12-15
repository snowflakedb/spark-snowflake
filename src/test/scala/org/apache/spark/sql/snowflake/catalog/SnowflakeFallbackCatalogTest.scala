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

  test("loadTable should delegate when no 403 error occurs") {
    val catalog = new SnowflakeFallbackCatalog()
    val delegate = new TestDelegateCatalog()

    catalog.setDelegateCatalog(delegate)
    val options = Map.empty[String, String].asJava
    val optionsMap = new CaseInsensitiveStringMap(options)
    catalog.initialize("test", optionsMap)

    val ident = Identifier.of(Array("schema"), "table")
    val result = catalog.loadTable(ident)

    assert(result != null)
    assert(delegate.loadTableCalled)
  }

  test("loadTable should attempt V1Table creation on 403") {
    // Create a minimal SparkSession for this test
    val testSpark = org.apache.spark.sql.SparkSession.builder()
      .master("local[1]")
      .appName("SnowflakeFallbackTest")
      .config("spark.snowflake.url", "test.snowflakecomputing.com")
      .config("spark.snowflake.user", "testuser")
      .config("spark.snowflake.password", "testpass")
      .config("spark.snowflake.database", "testdb")
      .config("spark.snowflake.schema", "testschema")
      .config("spark.ui.enabled", "false")  // Disable UI to avoid port conflicts
      .config("spark.driver.host", "localhost")
      .getOrCreate()

    try {
      // Set as active session
      org.apache.spark.sql.SparkSession.setActiveSession(testSpark)

      val catalog = new SnowflakeFallbackCatalog()
      val delegate = new TestDelegateCatalog()
      delegate.shouldThrow403 = true

      catalog.setDelegateCatalog(delegate)
      val options = Map.empty[String, String].asJava
      val optionsMap = new CaseInsensitiveStringMap(options)
      catalog.initialize("test_catalog", optionsMap)

      val ident = Identifier.of(Array("testschema"), "testtable")

      // With an active SparkSession, fallback should succeed and return a V1Table
      val result = catalog.loadTable(ident)

      // Verify we got a table back (not an exception)
      assert(result != null, "Should return a table")
      assert(result.isInstanceOf[V1Table],
        "Should return a V1Table instance")

      // Verify the delegate was called (403 was triggered)
      assert(delegate.loadTableCalled, "Delegate should have been called")

      // Verify the V1Table has the expected properties
      val v1Table = result.asInstanceOf[V1Table]
      val catalogTable = v1Table.catalogTable

      // Check that dbtable is correctly set
      val dbtable = catalogTable.storage.properties.get("dbtable")
      assert(dbtable.isDefined, "dbtable property should be set")
      assert(dbtable.get == "testschema.testtable",
        s"dbtable should be 'testschema.testtable', got '${dbtable.get}'")

      // Verify Snowflake configs were read from SparkSession
      assert(catalogTable.storage.properties.get("url").contains("test.snowflakecomputing.com"),
        "Should have url from SparkSession config")
      assert(catalogTable.storage.properties.get("user").contains("testuser"),
        "Should have user from SparkSession config")

      // Verify provider is set correctly
      assert(catalogTable.provider.contains("net.snowflake.spark.snowflake.DefaultSource"),
        "Should have Snowflake DefaultSource as provider")

    } finally {
      // Clean up: stop the test session
      testSpark.stop()
      org.apache.spark.sql.SparkSession.clearActiveSession()
    }
  }

  test("loadTable should propagate non-403 exceptions") {
    val catalog = new SnowflakeFallbackCatalog()
    val delegate = new TestDelegateCatalog()
    delegate.shouldThrowOther = true

    catalog.setDelegateCatalog(delegate)
    val options = Map.empty[String, String].asJava
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
    val options = Map.empty[String, String].asJava
    val optionsMap = new CaseInsensitiveStringMap(options)
    catalog.initialize("test_catalog", optionsMap)

    assert(catalog.name() === "test_catalog")
  }

  test("createDelegateCatalog should require catalog-impl option") {
    val catalog = new SnowflakeFallbackCatalog()
    val options = Map.empty[String, String].asJava
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
