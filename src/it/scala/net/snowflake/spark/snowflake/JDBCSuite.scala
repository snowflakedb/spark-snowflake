package net.snowflake.spark.snowflake

import DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

class JDBCSuite extends IntegrationSuiteBase {

  test("create and drop table") {

    val name = s"spark_test_table_$randomSuffix"
    val schema =
      new StructType(Array(StructField("num", IntegerType, nullable = false)))

    conn.createTable(name, schema, params, overwrite = true, temporary = false)

    assert(conn.tableExists(name))

    conn.dropTable(name)

    assert(!conn.tableExists(name))

    conn.createTable(name, schema, params, overwrite = false, temporary = false)

    assert(conn.dropTable(name))

    conn.createTable(name, schema, params, overwrite = true, temporary = true)

    assert(conn.dropTable(name))

    conn.createTable(name, schema, params, overwrite = false, temporary = true)

    assert(conn.dropTable(name))

    assert(!conn.dropTable(name))

  }

  test("create and drop stage") {

    val name = s"spark_test_stage_$randomSuffix"

    conn.createStage(name)

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
      temporary = true
    )
    assert(conn.dropStage(name))

  }

  test("create and drop pipe") {
    val schema =
      new StructType(Array(StructField("num", IntegerType, nullable = false)))

    val pipe_name = s"spark_test_pipe_$randomSuffix"
    val stage_name = s"spark_test_stage_$randomSuffix"
    val table_name = s"spark_test_table_$randomSuffix"
    conn.createTable(table_name, schema, params, overwrite = true, temporary = false)
    conn.createStage(stage_name)

    assert(!conn.pipeExists(pipe_name))
    conn.createPipe(
      pipe_name,
      ConstantString("copy into") + table_name + s"from @$stage_name"
    )
    assert(conn.pipeExists(pipe_name))
    conn.createPipe(
      pipe_name,
      ConstantString("copy into") + table_name + s"from @$stage_name",
      overwrite = true
    )
    assert(conn.pipeExists(pipe_name))
    conn.dropPipe(pipe_name)
    assert(!conn.pipeExists(pipe_name))

    conn.dropTable(table_name)
    conn.dropStage(stage_name)

  }

  test("test schema") {
    val name = s"spark_test_table_$randomSuffix"
    val schema =
      new StructType(Array(StructField("STR", StringType, nullable = false)))

    conn.createTable(name, schema, params, overwrite = true, temporary = false)

    conn.tableSchema(name, params).equals(schema)

    conn.dropTable(name)
  }

  test("copy from query") {
    val name = s"spark_test_table_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("num", IntegerType, nullable = false),
          StructField("str", StringType, nullable = false)
        )
      )

    conn.createTable(name, schema, params, overwrite = true, temporary = false)

    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(
        "query",
        s"""select num, str from $name where num = 0 and str = 'test'"""
      )
      .load()
      .show()

    conn.dropTable(name)

  }

  test("copy from query using udf") {

    // create table and insert data
    val name = s"spark_test_table_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("STR", StringType, nullable = false),
          StructField("NUM", IntegerType, nullable = false)
        )
      )
    conn.createTable(name, schema, params, overwrite = true, temporary = false)
    conn.execute(s"insert into $name values('a',1),('b',2)")
    val funcName = s"test_function$randomSuffix"
    conn.execute(
      s"create or replace function $funcName(n int) returns table" +
        s"(a int, b string) as $$$$ select num, str from $name where num = n$$$$"
    )

    val gapstats_query = s"""select * from table($funcName(1)) gs"""
    val test_df =
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", gapstats_query)
        .load()

    assert(test_df.collect().length == 1)

    conn.execute(s"drop function $funcName(int)")
    conn.dropTable(name)
  }

  test("test union") {
    val name = s"spark_test_table_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("STR", StringType, nullable = false),
          StructField("NUM", IntegerType, nullable = false)
        )
      )
    conn.createTable(name, schema, params, overwrite = true, temporary = false)
    conn.execute(s"insert into $name values('a',1),('b',2)")

    val df = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", name)
      .load()

    df.createOrReplaceTempView("table1")

    val df1 = sparkSession.sql("select num,str from table1")
    val df2 = sparkSession.sql("select num,str from table1")
    val df3 = df1.union(df2)

    assert(df3.collect().length == 4)

    conn.dropTable(name)
  }

}
