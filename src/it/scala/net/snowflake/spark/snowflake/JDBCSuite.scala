package net.snowflake.spark.snowflake

import DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

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

    assert(conn.dropStage(name))

    assert(!conn.dropStage(name))

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

  ignore ("test") {
    val statement  =  conn.prepareStatement(
      "create or replace table identifier(?) (num int)"
    )

    statement.setString(1, "\"abc\"")

    DefaultJDBCWrapper.executePreparedQueryInterruptibly(statement)
  }


}
