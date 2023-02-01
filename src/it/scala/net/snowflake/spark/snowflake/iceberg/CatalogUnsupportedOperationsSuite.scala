/*
 * Copyright 2015-2016 Snowflake Computing
 * Copyright 2015 Databricks
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

package net.snowflake.spark.snowflake.iceberg

import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Integration tests targeting table operation in snowflake's iceberg Catalog SDK.
  */
class CatalogUnsupportedOperationsSuite extends IcebergSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll();
  }

  test("FAIL: Create database ") {

    val testSparkDb: String = s"test_icebergSparkDB$randomSuffix"
    assertThrows[UnsupportedOperationException](sparkSession.sql(
      s"create database $testSparkDb"))
  }

  test("FAIL: Create schema ") {

    val testSparkSchema: String = s"test_icebergSparkSchema$randomSuffix"
    assertThrows[UnsupportedOperationException](sparkSession.sql(
      s"create namespace $test_database.$testSparkSchema"))
  }

  test("FAIL: Drop namespace ") {

    assertThrows[UnsupportedOperationException](sparkSession.sql(
      s"drop namespace $test_database"))
  }

  test("FAIL: Rename namespace ") {

    assertThrows[UnsupportedOperationException](sparkSession.sql(
      s"drop namespace $test_database"))
  }

  test("FAIL: Add properties to namespace ") {

    assertThrows[UnsupportedOperationException](sparkSession.sql(
      s"alter namespace $test_database set PROPERTIES ('myPropKey' = 'myPropVal') "))
  }

  test("FAIL: Rename/alter table ") {

    val test_icebergTable_1: String = s"test_icebergTable1_$randomSuffix"
    val new_icebergTableName : String = s"NewTab_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("STR", StringType, nullable = false),
          StructField("NUM", IntegerType, nullable = false)
        )
      )

    try {
      conn.createIcebergTable(test_icebergTable_1, schema, params, externalVolume, true);
      sparkSession.sql(s"USE $test_database.$test_schema_1")
      assertThrows[UnsupportedOperationException](sparkSession.sql(
        s"alter table $test_icebergTable_1 rename to $new_icebergTableName"))
    }
    finally {
      conn.dropTable(test_icebergTable_1)
    }

  }
}
