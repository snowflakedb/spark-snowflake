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

  test("List tables in a schema") {

    val test_icebergTable_1: String = s"test_icebergTable1_$randomSuffix"
    val test_icebergTable_2: String = s"test_icebergTable2_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("STR", StringType, nullable = false),
          StructField("NUM", IntegerType, nullable = false)
        )
      )

    try {
      conn.createIcebergTable(test_icebergTable_1, schema, params, externalVolume, true);
      conn.createIcebergTable(test_icebergTable_2, schema, params, externalVolume, true);

      val tableData = sparkSession.sql(s"show tables in $test_database.$test_schema_2");

      checkAnswer(tableData,
        Seq(
          Row(s"${test_database.toUpperCase}.${test_schema_2.toUpperCase}",
          s"${test_icebergTable_1.toUpperCase}",
            false),
          Row(s"${test_database.toUpperCase}.${test_schema_2.toUpperCase}",
            s"${test_icebergTable_2.toUpperCase}",
            false)));
    }
    finally {
      conn.dropTable(test_icebergTable_1)
      conn.dropTable(test_icebergTable_2)
    }
  }

  test("FAIL : List tables at database level ") {

    val test_icebergTable_1: String = s"test_icebergTable1_$randomSuffix"
    val test_icebergTable_2: String = s"test_icebergTable2_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("STR", StringType, nullable = false),
          StructField("NUM", IntegerType, nullable = false)
        )
      )

    try {
      conn.createIcebergTable(test_icebergTable_1, schema, params, externalVolume, true);
      conn.createIcebergTable(test_icebergTable_2, schema, params, externalVolume, true);

      val caught =
        intercept[IllegalArgumentException](sparkSession.sql(s"show tables in $test_database"))

      assert(caught.getMessage().contains("listTables must be at SCHEMA level"))
    }
    finally {
      conn.dropTable(test_icebergTable_1)
      conn.dropTable(test_icebergTable_2)
    }
  }

  test("Read snowflake iceberg table from spark") {

    val test_icebergTable_1: String = s"test_icebergTable1_$randomSuffix"

    val tableschema =
      new StructType(
        Array(
          StructField("STR", StringType, nullable = false),
          StructField("NUM", IntegerType, nullable = false)
        )
      )

    try {
      conn.createIcebergTable(test_icebergTable_1, tableschema, params, externalVolume, true);
      conn.execute(s"insert into $test_icebergTable_1 values('a',1),('b',2)")

      val tableData = sparkSession.sql(s"select * from $test_database.$test_schema_2." +
        s"$test_icebergTable_1");

      checkAnswer(tableData, Seq(Row("a", 1), Row("b", 2)));
    }
    finally {
      conn.dropTable(test_icebergTable_1)
    }
  }

  test("Read snowflake iceberg table with special characters from spark") {

    val test_icebergTable_1: String = s"@HJ!!.0-w*r()^'_$randomSuffix"
    val test_SpecialDB: String = "#()-@DFdf234"
    val test_SpecialSchema: String = "^%()@~"

    val schema =
      new StructType(
        Array(
          StructField("STR", StringType, nullable = false),
          StructField("NUM", IntegerType, nullable = false)
        )
      )

    try {
      conn.createStatement().executeUpdate(
        s"""CREATE DATABASE IF NOT EXISTS "$test_SpecialDB"""");
      conn.createStatement().executeUpdate(
        s"""CREATE SCHEMA IF NOT EXISTS "$test_SpecialSchema"""");

      conn.createStatement().executeUpdate(s"""USE "$test_SpecialDB"."$test_SpecialSchema"""");
      conn.createIcebergTable(s""""$test_icebergTable_1"""", schema,
        params, externalVolume, true, true);
      conn.execute(s"""insert into "$test_icebergTable_1" values('a',1),('b',2)""")

      val tableData = sparkSession.sql(
        s"""select * from `"$test_SpecialDB"`.`"$test_SpecialSchema"`.`"$test_icebergTable_1"`""");

      checkAnswer(tableData, Seq(Row("a", 1), Row("b", 2)));
    }
    finally {
      conn.dropTable(test_icebergTable_1)
    }
  }

}
