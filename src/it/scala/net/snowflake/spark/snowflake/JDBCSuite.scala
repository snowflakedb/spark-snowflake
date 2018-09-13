package net.snowflake.spark.snowflake

import DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

class JDBCSuite extends IntegrationSuiteBase {


  test("create and drop table") {

    val name = s"spark_test_table_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("num", IntegerType, false)
        )
      )

    conn.createTable(name, schema, true, false)

    assert(conn.tableExists(name))

    conn.dropTable(name)

    assert(!conn.tableExists(name))

    conn.createTable(name, schema, false, false)

    assert(conn.dropTable(name))

    conn.createTable(name, schema, true, true)

    assert(conn.dropTable(name))

    conn.createTable(name, schema, false, true)

    assert(conn.dropTable(name))

    assert(!conn.dropTable(name))

  }

  test("create and drop stage") {

    val name = s"spark_test_stage_$randomSuffix"

    conn.createStage(name, overwrite = false, temporary = false)

    assert(conn.stageExists(name))

    assert(conn.dropStage(name))

    assert(!conn.dropStage(name))

    assert(!conn.stageExists(name))

    conn.createStage(name, overwrite = true, temporary = true)

    assert(conn.dropStage(name))

    assert(!conn.dropStage(name))

    conn.createStage(
      name,
      params.storagePath,
      params.awsAccessKey,
      params.awsSecretKey,
      params.azureSAS,
      false,
      true
    )
    assert(conn.dropStage(name))

  }

  test("test schema") {
    val name = s"spark_test_table_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("STR", StringType, false)
        )
      )

    conn.createTable(name, schema, true, false)

    conn.tableSchema(name).equals(schema)

    conn.dropTable(name)
  }
}
