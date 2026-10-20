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

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import scala.collection.JavaConverters._

class TestDelegateCatalog extends TableCatalog with SupportsNamespaces {
  var shouldThrow403: Boolean = false
  var shouldThrowOther: Boolean = false
  var loadTableCalled: Boolean = false
  var initializeOptions: CaseInsensitiveStringMap = _

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // Capture the options passed during initialization
    initializeOptions = options
  }

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

class SnowflakeFallbackCatalogTest extends AnyFunSuite {

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
    // The fallback now reads ONLY the immutable SparkConf, so the connector
    // properties below must be present at SparkContext creation time. Other suites
    // in the shared test JVM may have left a SparkContext running, in which case
    // getOrCreate() would reuse it and our builder configs would land in the
    // session SQLConf (which the fix intentionally ignores) rather than the
    // immutable SparkConf. Tear down any existing session so we start fresh.
    org.apache.spark.sql.SparkSession.getDefaultSession.foreach(_.stop())
    org.apache.spark.sql.SparkSession.clearActiveSession()
    org.apache.spark.sql.SparkSession.clearDefaultSession()

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
      org.apache.spark.sql.SparkSession.clearDefaultSession()
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

  test("User-Agent header should be passed to delegate catalog") {
    val catalog = new SnowflakeFallbackCatalog()

    val options = Map(
      "catalog-impl" -> "org.apache.spark.sql.snowflake.catalog.TestDelegateCatalog",
      "custom-option" -> "custom-value",
      "snowflake-database" -> "MY_DB_2"
    ).asJava
    val optionsMap = new CaseInsensitiveStringMap(options)

    catalog.initialize("test_catalog", optionsMap)

    val delegateField = catalog.getClass.getDeclaredField("delegateCatalog")
    delegateField.setAccessible(true)
    val delegate = delegateField.get(catalog).asInstanceOf[TestDelegateCatalog]

    assert(delegate.initializeOptions != null)

    val userAgent = delegate.initializeOptions.get("header.User-Agent")
    assert(userAgent != null, "User-Agent header should be set")

    val expectedUserAgent = s"spark-snowflake/${net.snowflake.spark.snowflake.Utils.VERSION} spark/${SPARK_VERSION}"
    assert(userAgent == expectedUserAgent,
      s"Expected '$expectedUserAgent', got '$userAgent'")

    assert(userAgent.startsWith("spark-snowflake/"))
    assert(userAgent.contains(s" spark/${SPARK_VERSION}"))
    assert(userAgent.contains(net.snowflake.spark.snowflake.Utils.VERSION))

    val pattern = """spark-snowflake/\d+\.\d+\.\d+ spark/\d+\.\d+\.\d+""".r
    assert(pattern.findFirstIn(userAgent).isDefined,
      s"User-Agent should match pattern spark-snowflake/X.Y.Z spark/X.Y.Z")

    assert(delegate.initializeOptions.get("custom-option") == "custom-value")
    assert(delegate.initializeOptions.get("catalog-impl") == null)
    assert(delegate.initializeOptions.get("snowflake-database") == null)
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
    val dbTableMulti = catalog.buildFullTableName(multiPartIdent, None)
    assert(dbTableMulti === "production_db.analytics_schema.sales_table",
      s"Multi-part namespace: Expected 'production_db.analytics_schema.sales_table', got '$dbTableMulti'")

    // Test Case 2: Single-part namespace (Snowflake: schema.table)
    val singlePartIdent = Identifier.of(Array("analytics_schema"), "sales_table")
    val dbTableSingle = catalog.buildFullTableName(singlePartIdent, None)
    assert(dbTableSingle === "analytics_schema.sales_table",
      s"Single-part namespace: Expected 'analytics_schema.sales_table', got '$dbTableSingle'")

    // Test Case 3: No namespace (Snowflake: table only)
    val noNamespaceIdent = Identifier.of(Array(), "sales_table")
    val dbTableNone = catalog.buildFullTableName(noNamespaceIdent, None)
    assert(dbTableNone === "sales_table",
      s"No namespace: Expected 'sales_table', got '$dbTableNone'")

    // Test Case 4: Deep multi-part namespace (e.g., catalog.database.schema.table)
    val deepNamespaceIdent = Identifier.of(Array("catalog", "production_db", "analytics_schema"), "sales_table")
    val dbTableDeep = catalog.buildFullTableName(deepNamespaceIdent, None)
    assert(dbTableDeep === "catalog.production_db.analytics_schema.sales_table",
      s"Deep namespace: Expected 'catalog.production_db.analytics_schema.sales_table', got '$dbTableDeep'")

    // Test Case 5: Namespace with special characters (should be preserved as-is)
    val specialIdent = Identifier.of(Array("my_db", "my-schema"), "my.table")
    val dbTableSpecial = catalog.buildFullTableName(specialIdent, None)
    assert(dbTableSpecial === "my_db.my-schema.my.table",
      s"Special characters: Expected 'my_db.my-schema.my.table', got '$dbTableSpecial'")
  }

  // Hardening: a runtime SQLConf value (what a Spark-SQL `SET` writes) must NOT
  // leak into the privileged V1Table option map; only immutable SparkConf should flow.
  test("runtime SQLConf values must not leak into V1Table properties (dbee43e2)") {
    val testSpark = org.apache.spark.sql.SparkSession.builder()
      .master("local[1]")
      .appName("FallbackCatalogMergeSecurityTest")
      .config("spark.snowflake.url", "test.snowflakecomputing.com")
      .config("spark.snowflake.user", "testuser")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
    try {
      org.apache.spark.sql.SparkSession.setActiveSession(testSpark)
      // Simulate a malicious `SET snowflake.preactions=...` (writes to runtime SQLConf).
      testSpark.conf.set("snowflake.preactions", "GRANT ROLE ACCOUNTADMIN TO USER attacker")

      val catalog = new SnowflakeFallbackCatalog()
      val delegate = new TestDelegateCatalog()
      delegate.shouldThrow403 = true
      catalog.setDelegateCatalog(delegate)
      catalog.initialize("test_catalog",
        new CaseInsensitiveStringMap(Map.empty[String, String].asJava))

      val result = catalog.loadTable(Identifier.of(Array("testschema"), "testtable"))
      val props = result.asInstanceOf[V1Table].catalogTable.storage.properties

      assert(props.get("preactions").isEmpty,
        s"runtime SET value leaked into V1Table properties: ${props.get("preactions")}")
      // Legit admin SparkConf value must still flow through.
      assert(props.get("url").contains("test.snowflakecomputing.com"),
        "admin SparkConf 'url' should still be present in V1Table properties")
    } finally {
      testSpark.conf.unset("snowflake.preactions")
      org.apache.spark.sql.SparkSession.clearActiveSession()
    }
  }

  test("snowflake-database catalog option should qualify dbtable") {
    val catalog = new SnowflakeFallbackCatalog()
    catalog.setDelegateCatalog(new TestDelegateCatalog())
    catalog.initialize("alias", new CaseInsensitiveStringMap(
      Map("snowflake-database" -> "MY_DB_2").asJava))

    assert(catalog.buildFullTableName(Identifier.of(Array("MY_SCHEMA"), "MY_TABLE"),
      catalog.resolveDatabase(None)) ===
      "MY_DB_2.MY_SCHEMA.MY_TABLE")
    intercept[IllegalArgumentException] {
      catalog.buildFullTableName(Identifier.of(Array("A", "B"), "MY_TABLE"),
        catalog.resolveDatabase(None))
    }
    intercept[IllegalArgumentException] {
      catalog.buildFullTableName(Identifier.of(Array.empty[String], "MY_TABLE"),
        catalog.resolveDatabase(None))
    }
  }

  test("blank snowflake-database should fall back to the warehouse") {
    val catalog = new SnowflakeFallbackCatalog()
    catalog.setDelegateCatalog(new TestDelegateCatalog())
    catalog.initialize("alias", new CaseInsensitiveStringMap(
      Map("snowflake-database" -> "  ", "warehouse" -> "MY_DB_1").asJava))

    assert(catalog.buildFullTableName(Identifier.of(Array("MY_SCHEMA"), "MY_TABLE"),
      catalog.resolveDatabase(None)) ===
      "MY_DB_1.MY_SCHEMA.MY_TABLE")
  }

  test("warehouse should qualify dbtable when snowflake-database is unset") {
    val catalog = new SnowflakeFallbackCatalog()
    catalog.setDelegateCatalog(new TestDelegateCatalog())
    catalog.initialize("MY_DB_1", new CaseInsensitiveStringMap(
      Map("warehouse" -> "MY_DB_2").asJava))

    assert(catalog.buildFullTableName(Identifier.of(Array("MY_SCHEMA"), "MY_TABLE"),
      catalog.resolveDatabase(None)) ===
      "MY_DB_2.MY_SCHEMA.MY_TABLE")
    intercept[IllegalArgumentException] {
      catalog.buildFullTableName(Identifier.of(Array("A", "B"), "MY_TABLE"),
        catalog.resolveDatabase(None))
    }
    intercept[IllegalArgumentException] {
      catalog.buildFullTableName(Identifier.of(Array.empty[String], "MY_TABLE"),
        catalog.resolveDatabase(None))
    }
  }

  test("session sfdatabase should replace the warehouse but not snowflake-database") {
    val byWarehouse = new SnowflakeFallbackCatalog()
    byWarehouse.setDelegateCatalog(new TestDelegateCatalog())
    byWarehouse.initialize("alias", new CaseInsensitiveStringMap(
      Map("warehouse" -> "MY_DB_2").asJava))
    assert(byWarehouse.resolveDatabase(Some("MY_DB_1")).isEmpty)

    val explicit = new SnowflakeFallbackCatalog()
    explicit.setDelegateCatalog(new TestDelegateCatalog())
    explicit.initialize("alias", new CaseInsensitiveStringMap(
      Map("snowflake-database" -> "MY_DB_3", "warehouse" -> "MY_DB_2").asJava))
    assert(explicit.resolveDatabase(Some("MY_DB_1")).contains("MY_DB_3"))
  }

  test("each catalog registered in Spark should use its own warehouse as the database") {
    SparkSession.getDefaultSession.foreach(_.stop())
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val delegateImpl = "org.apache.spark.sql.snowflake.catalog.TestDelegateCatalog"
    val spark = SparkSession.builder()
      .master("local[1]")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .config("spark.sql.catalog.cat1", classOf[SnowflakeFallbackCatalog].getName)
      .config("spark.sql.catalog.cat1.catalog-impl", delegateImpl)
      .config("spark.sql.catalog.cat1.warehouse", "MY_DB_1")
      .config("spark.sql.catalog.cat2", classOf[SnowflakeFallbackCatalog].getName)
      .config("spark.sql.catalog.cat2.catalog-impl", delegateImpl)
      .config("spark.sql.catalog.cat2.warehouse", "MY_DB_2")
      .config("spark.sql.catalog.cat3", classOf[SnowflakeFallbackCatalog].getName)
      .config("spark.sql.catalog.cat3.catalog-impl", delegateImpl)
      .config("spark.sql.catalog.cat3.warehouse", "MY_DB_2")
      .config("spark.sql.catalog.cat3.snowflake-database", "MY_DB_3")
      .getOrCreate()
    try {
      def dbtable(name: String): String = {
        val catalog = spark.sessionState.catalogManager.catalog(name)
          .asInstanceOf[SnowflakeFallbackCatalog]
        catalog.buildFullTableName(Identifier.of(Array("MY_SCHEMA"), "MY_TABLE"),
          catalog.resolveDatabase(None))
      }

      assert(dbtable("cat1") === "MY_DB_1.MY_SCHEMA.MY_TABLE")
      assert(dbtable("cat2") === "MY_DB_2.MY_SCHEMA.MY_TABLE")
      assert(dbtable("cat3") === "MY_DB_3.MY_SCHEMA.MY_TABLE")
    } finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  test("resolved database should default sfdatabase only when session does not set it") {
    def fallbackProps(
        sessionConf: Map[String, String],
        catalogOptions: Map[String, String] = Map("snowflake-database" -> "MY_DB_2"))
        : Map[String, String] = {
      SparkSession.getDefaultSession.foreach(_.stop())
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      val spark = SparkSession.builder()
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "localhost")
        .config(sessionConf)
        .getOrCreate()
      try {
        SparkSession.setActiveSession(spark)
        val catalog = new SnowflakeFallbackCatalog()
        val delegate = new TestDelegateCatalog()
        delegate.shouldThrow403 = true
        catalog.setDelegateCatalog(delegate)
        catalog.initialize("alias", new CaseInsensitiveStringMap(catalogOptions.asJava))
        catalog.loadTable(Identifier.of(Array("MY_SCHEMA"), "MY_TABLE"))
          .asInstanceOf[V1Table].catalogTable.storage.properties
      } finally {
        spark.stop()
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
      }
    }

    val defaulted = fallbackProps(Map.empty)
    assert(defaulted.get("sfdatabase").contains("MY_DB_2"))
    assert(defaulted.get("dbtable").contains("MY_DB_2.MY_SCHEMA.MY_TABLE"))
    val sessionSet = fallbackProps(Map("spark.snowflake.sfDatabase" -> "MY_DB_1"))
    assert(sessionSet.get("sfdatabase").contains("MY_DB_1"))
    assert(sessionSet.get("dbtable").contains("MY_DB_2.MY_SCHEMA.MY_TABLE"))
    val fromWarehouse = fallbackProps(Map.empty, Map("warehouse" -> "MY_DB_3"))
    assert(fromWarehouse.get("sfdatabase").contains("MY_DB_3"))
    assert(fromWarehouse.get("dbtable").contains("MY_DB_3.MY_SCHEMA.MY_TABLE"))
    val sessionOverWarehouse = fallbackProps(
      Map("spark.snowflake.sfDatabase" -> "MY_DB_1"), Map("warehouse" -> "MY_DB_3"))
    assert(sessionOverWarehouse.get("sfdatabase").contains("MY_DB_1"))
    assert(sessionOverWarehouse.get("dbtable").contains("MY_SCHEMA.MY_TABLE"))
  }
}
