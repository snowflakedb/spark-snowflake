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

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.FunSuite
import scala.collection.JavaConverters._

class V1TableApproachTest extends FunSuite {

  private val FGAC_KEY = "spark.snowflake.extensions.fgacJdbcFallback.enabled"

  test("loadTable should return V1Table on 403 when FGAC enabled") {
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
    val result = catalog.loadTable(ident)
    
    // Should return V1Table instead of throwing exception
    assert(result != null, "Should return a table")
    assert(result.isInstanceOf[V1Table], "Should return V1Table")
    
    val v1Table = result.asInstanceOf[V1Table]
    val catalogTable = v1Table.catalogTable
    
    assert(catalogTable.provider == Some("snowflake"), "Provider should be snowflake")
    assert(catalogTable.storage.properties.contains("dbtable"), "Should have dbtable property")
    assert(catalogTable.storage.properties("dbtable") == "schema.table", "dbtable should be schema.table")
    // Keys are lowercased by CaseInsensitiveStringMap
    assert(catalogTable.storage.properties.contains("sfurl"), "Should have sfurl property")
    assert(catalogTable.storage.properties("sfurl") == "test.snowflakecomputing.com", 
      "sfurl should be passed through")
    assert(catalogTable.storage.properties.contains("sfuser"), "Should have sfuser property")
    assert(catalogTable.storage.properties.contains("sfpassword"), "Should have sfpassword property")
  }

  test("V1Table should have correct table identifier") {
    val catalog = new SnowflakeFallbackCatalog()
    val delegate = new TestDelegateCatalog()
    delegate.shouldThrow403 = true
    
    catalog.setDelegateCatalog(delegate)
    val options = Map(FGAC_KEY -> "true").asJava
    val optionsMap = new CaseInsensitiveStringMap(options)
    catalog.initialize("test", optionsMap)
    
    val ident = Identifier.of(Array("myschema"), "mytable")
    val result = catalog.loadTable(ident)
    
    val v1Table = result.asInstanceOf[V1Table]
    assert(v1Table.catalogTable.identifier.table == "mytable")
    assert(v1Table.catalogTable.identifier.database == Some("myschema"))
  }

  test("loadTable should still delegate normally when FGAC disabled") {
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
}
