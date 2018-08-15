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

    assert(conn.tableExists(name))

    conn.dropTable(name)

    conn.createTable(name, schema, true, true)

    assert(conn.tableExists(name))

    conn.dropTable(name)

    conn.createTable(name, schema, false, true)

    assert(conn.tableExists(name))

    conn.dropTable(name)

  }

  test("create and drop stage") {

  }


}
