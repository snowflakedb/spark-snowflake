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
import org.apache.iceberg.jdbc.UncheckedSQLException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException

/**
  * Integration tests targeting namespace operation in snowflake's iceberg Catalog SDK.
  */
class CatalogNamespaceOperationSuite extends IcebergSuiteBase {

  private val test_schema_2: String = s"test_icebergSchema2_$randomSuffix"
  override def beforeAll(): Unit = {
    super.beforeAll();
    conn.createStatement().executeUpdate(s"CREATE SCHEMA IF NOT EXISTS $test_schema_2");
  }


  test("List account level namespaces") {

    val sparkNamespaces = sparkSession.sql("show databases");
    val databases = conn.createStatement().executeQuery("show databases");
    val dbCount = results(databases){ rs => Unit}.length

    assert(sparkNamespaces.count() == dbCount);
  }

  test("List db level namespaces") {

    val sparkNamespaces = sparkSession.sql(s"show namespaces in $test_database");
    val schemas = conn.createStatement().executeQuery(s"show schemas in $test_database");

    var schemaNames : Seq[Row] = Seq();

    schemaNames = results(schemas){ rs => Row(rs.getString("database_name") +
      "." + schemas.getString("name"))}.toSeq

    assert(sparkNamespaces.count() == schemaNames.length);
    checkAnswer(sparkNamespaces.select("namespace"), schemaNames);
  }

  test("FAIL : Use non-existing namespace at db level") {

    assertThrows[NoSuchNamespaceException](sparkSession.sql("use namespace NonExistingDB"));
  }

  test("FAIL: Use non existing namespace at schema level ") {

    assertThrows[NoSuchNamespaceException](sparkSession.sql("use namespace NonExistingDB." +
      "NonExistingSchema"));
  }

  test("FAIL: Exceed max supported namespace hierarchy ") {

    val caught =
    intercept[IllegalArgumentException](sparkSession.sql("use namespace NonExistingDB." +
      "NonExistingSchema.SomeTable.SomeMore"));

    assert(caught.getMessage().contains("Snowflake max namespace level is 2"))
  }

  test("Unquoted namespace identifier with no special characters") {

    val unQuotedSchema = "Schema1"
    try {
      conn.createStatement().executeUpdate(
        s"CREATE SCHEMA IF NOT EXISTS $unQuotedSchema");
      sparkSession.sql(s"use namespace $test_database.$unQuotedSchema")
    }
    finally
      {
        conn.createStatement().executeUpdate(s"DROP SCHEMA IF EXISTS $unQuotedSchema")
      }
  }

  test("Unquoted namespace identifier with no special characters " +
    "(digits and letters only) as case insensitive") {

    val unQuotedSchema = "Schema1"
    try {
      conn.createStatement().executeUpdate(
        s"CREATE SCHEMA IF NOT EXISTS $unQuotedSchema");
      sparkSession.sql(s"use namespace $test_database.$unQuotedSchema")
    }
    finally {
      conn.createStatement().executeUpdate(s"DROP SCHEMA IF EXISTS ${unQuotedSchema.toUpperCase()}")
    }
  }

  test("Unquoted namespace identifier with leading underscore") {

    val unQuotedSchema = "_Schema1"
    try {
      conn.createStatement().executeUpdate(
        s"CREATE SCHEMA IF NOT EXISTS $unQuotedSchema");
      sparkSession.sql(s"use namespace $test_database.$unQuotedSchema")
    }
    finally {
      conn.createStatement().executeUpdate(s"DROP SCHEMA IF EXISTS $unQuotedSchema")
    }
  }

  test("Unquoted namespace identifier with dollar") {

    val unQuotedSchema = "_$chema1"
    try {
      conn.createStatement().executeUpdate(
        s"CREATE SCHEMA IF NOT EXISTS $unQuotedSchema");
      sparkSession.sql(s"use namespace $test_database.`$unQuotedSchema`")
    }
    finally {
      conn.createStatement().executeUpdate(s"DROP SCHEMA IF EXISTS $unQuotedSchema")
    }
  }

  test("Unquoted namespace identifier with allowed characters") {

    val unQuotedSchema = "_$chem_a1_"
    try {
      conn.createStatement().executeUpdate(
        s"CREATE SCHEMA IF NOT EXISTS $unQuotedSchema");
      sparkSession.sql(s"use namespace $test_database.`$unQuotedSchema`")
    }
    finally {
      conn.createStatement().executeUpdate(s"DROP SCHEMA IF EXISTS $unQuotedSchema")
    }
  }

  test("Quoted namespace identifier with lower case") {

    val quotedSchema = "lowerquoted"
    try {
      conn.createStatement().executeUpdate(
        s"""CREATE SCHEMA IF NOT EXISTS "$quotedSchema"""")
      sparkSession.sql(s"""use namespace $test_database.`"$quotedSchema"`""")
    }
    finally {
      conn.createStatement().executeUpdate(s"""DROP SCHEMA IF EXISTS "$quotedSchema" """)
    }
  }

  test("Failure on accessing quoted namespace identifier with lower case as unquoted identifier") {

    val quotedSchema = "lowerquoted"
    try {
      conn.createStatement().executeUpdate(
        s"""CREATE SCHEMA IF NOT EXISTS "$quotedSchema"""")
      assertThrows[NoSuchNamespaceException](sparkSession.sql(
        s"use namespace $test_database.`$quotedSchema`"))
    }
    finally {
      conn.createStatement().executeUpdate(s"""DROP SCHEMA IF EXISTS "$quotedSchema" """)
    }
  }

  test("Failure on accessing quoted namespace identifier without case sensitivity") {

    val quotedSchema = "lowerquoted"
    try {
      conn.createStatement().executeUpdate(
        s"""CREATE SCHEMA IF NOT EXISTS "$quotedSchema"""")
      assertThrows[NoSuchNamespaceException](sparkSession.sql(
        s"""use namespace $test_database.`"${quotedSchema.toUpperCase}"`"""))
    }
    finally {
      conn.createStatement().executeUpdate(s"""DROP SCHEMA IF EXISTS "$quotedSchema" """)
    }
  }

  test("Quoted namespace identifier with special characters") {

    val quotedSchema = "H@!!.0-w*r()^'"
    try {
      conn.createStatement().executeUpdate(
        s"""CREATE SCHEMA IF NOT EXISTS "$quotedSchema"""")
      sparkSession.sql(s"""use namespace $test_database.`"$quotedSchema"`""")
    }
    finally {
      conn.createStatement().executeUpdate(s"""DROP SCHEMA IF EXISTS "$quotedSchema" """)
    }
  }

  test("Quoted namespace identifier with double quote as special characters") {

    val quotedSchema = "H@\"!!0\""

    // user is expected to escape the double quotes
    val escapedQuotedSchema = quotedSchema.replace("\"", "\"\"")
    try {
      conn.createStatement().executeUpdate(
        s"""CREATE SCHEMA IF NOT EXISTS "$escapedQuotedSchema"""")
      sparkSession.sql(s"""use namespace $test_database.`"$escapedQuotedSchema"`""")
    }
    finally {
      conn.createStatement().executeUpdate(s"""DROP SCHEMA IF EXISTS "$escapedQuotedSchema" """)
    }
  }

}
