package net.snowflake.spark.snowflake

import DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
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

  test("test") {

//    val df =
//      sqlContext
//        .read
//        .format(SNOWFLAKE_SOURCE_NAME)
//        .options(connectorOptionsNoTable)
//        .option("dbtable", "test_table")
//        .load()
//
//    df.where("num > 1").show()

    // insert into test_table values("abc"), ("123")

    val st1 = StringVariable("abc") + ConstantString("),(") +
      StringVariable("123") + ConstantString(")")

    val st2 = ConstantString(",(") + StringVariable("456") + ConstantString(")")

    (ConstantString("insert into test_table values (") + st1 + st2).execute(conn)


  }


}
