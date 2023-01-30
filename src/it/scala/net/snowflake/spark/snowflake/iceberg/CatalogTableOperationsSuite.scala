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
import net.snowflake.spark.snowflake.IntegrationSuiteBase
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Integration tests targeting table operation in snowflake's iceberg Catalog SDK.
  */
class CatalogTableOperationsSuite extends IcebergSuiteBase {

  private val test_schema_2: String = s"test_icebergSchema2_$randomSuffix"
  override def beforeAll(): Unit = {
    super.beforeAll();
    conn.createStatement().executeUpdate(s"CREATE SCHEMA IF NOT EXISTS $test_schema_2");
  }

  test("Read snowflake iceberg table from spark") {

    val test_icebergTable_1: String = s"test_icebergTable1_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("STR", StringType, nullable = false),
          StructField("NUM", IntegerType, nullable = false)
        )
      )
    conn.createIcebergTable(test_icebergTable_1, schema, params, externalVolume, true);
    conn.execute(s"insert into $test_icebergTable_1 values('a',1),('b',2)")

    val tableData = sparkSession.sql(s"select * from $test_database.$test_schema_2." +
      s"$test_icebergTable_1");

    checkAnswer(tableData, Seq(Row("a", 1), Row("b", 2)));
  }

}
