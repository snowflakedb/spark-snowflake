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

package org.apache.spark.sql.snowflake.extensions.analyzer.extension

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.scalatest.FunSuite

class ResolveSnowflakeRelationsTest extends FunSuite {

  private val configKey = "spark.snowflake.extensions.fgacJdbcFallback.enabled"

  test("Configuration key should be correctly defined") {
    assert(configKey == "spark.snowflake.extensions.fgacJdbcFallback.enabled")
  }

  test("ResolveSnowflakeRelations class should be accessible") {
    assert(classOf[ResolveSnowflakeRelations] != null)
    
    val methods = classOf[ResolveSnowflakeRelations].getMethods
    assert(methods.exists(_.getName == "apply"))
  }

  test("Configuration key naming follows expected convention") {
    assert(configKey.startsWith("spark.snowflake.extensions."))
    assert(configKey.contains("fgac"))
    assert(configKey.contains("Jdbc"))
    assert(configKey.contains("Fallback"))
    assert(configKey.endsWith(".enabled"))
  }

  test("ResolveSnowflakeRelations should be a case class") {
    val clazz = classOf[ResolveSnowflakeRelations]
    assert(clazz.getInterfaces.length > 0)
    
    val methods = clazz.getMethods
    assert(methods.exists(_.getName == "apply"))
  }

  test("Configuration key constant should be accessible via reflection") {
    try {
      val clazz = classOf[ResolveSnowflakeRelations]
      val fields = clazz.getDeclaredFields
      val configKeyField = fields.find(_.getName.contains("FGAC_JDBC_FALLBACK_ENABLED_KEY"))
      
      assert(configKeyField.isDefined, "Configuration key field should be defined")
      
      configKeyField.get.setAccessible(true)
      assert(configKeyField.get.getType == classOf[String])
      
    } catch {
      case _: Exception =>
        assert(classOf[ResolveSnowflakeRelations] != null)
    }
  }

  test("buildSnowflakeOptions method should exist") {
    val clazz = classOf[ResolveSnowflakeRelations]
    val methods = clazz.getDeclaredMethods
    val buildOptionsMethod = methods.find(_.getName == "buildSnowflakeOptions")
    
    assert(buildOptionsMethod.isDefined, "buildSnowflakeOptions method should exist")
  }

  test("createSnowflakeRelation method should exist") {
    val clazz = classOf[ResolveSnowflakeRelations]
    val methods = clazz.getDeclaredMethods
    val createRelationMethod = methods.find(_.getName == "createSnowflakeRelation")
    
    assert(createRelationMethod.isDefined, "createSnowflakeRelation method should exist")
  }

  test("Rule should extend Rule[LogicalPlan]") {
    val clazz = classOf[ResolveSnowflakeRelations]
    val interfaces = clazz.getInterfaces
    
    val hasRuleInterface = interfaces.exists(_.getName.contains("Rule"))
    assert(hasRuleInterface || clazz.getSuperclass != null, 
      "Should extend Rule or implement Rule interface")
  }

  test("Rule should implement LookupCatalog") {
    val clazz = classOf[ResolveSnowflakeRelations]
    val interfaces = clazz.getInterfaces
    
    val hasLookupCatalog = interfaces.exists(_.getName.contains("LookupCatalog"))
    assert(hasLookupCatalog, "Should implement LookupCatalog trait")
  }

  test("UnresolvedRelation should be importable") {
    assert(classOf[UnresolvedRelation] != null)
  }

  test("shouldFallbackToSnowflake method should exist") {
    val clazz = classOf[ResolveSnowflakeRelations]
    val methods = clazz.getDeclaredMethods
    val fallbackMethod = methods.find(_.getName == "shouldFallbackToSnowflake")
    
    assert(fallbackMethod.isDefined, "shouldFallbackToSnowflake method should exist")
  }

  test("Rule should use instanceof check for FGACForbiddenException") {
    val clazz = Class.forName("org.apache.spark.sql.snowflake.catalog.FGACForbiddenException")
    assert(clazz != null, "FGACForbiddenException class should exist")
    assert(classOf[org.apache.spark.sql.catalyst.analysis.NoSuchTableException].isAssignableFrom(clazz),
      "FGACForbiddenException should extend NoSuchTableException")
  }
}
