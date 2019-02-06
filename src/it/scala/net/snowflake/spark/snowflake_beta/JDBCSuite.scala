package net.snowflake.spark.snowflake_beta

import DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import net.snowflake.spark.snowflake_beta.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.Row

class JDBCSuite extends IntegrationSuiteBase {


  test("create and drop table") {

    val name = s"spark_test_table_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("num", IntegerType, false)
        )
      )

    conn.createTable(name, schema, params, true, false)

    assert(conn.tableExists(name))

    conn.dropTable(name)

    assert(!conn.tableExists(name))

    conn.createTable(name, schema, params, false, false)

    assert(conn.dropTable(name))

    conn.createTable(name, schema, params, true, true)

    assert(conn.dropTable(name))

    conn.createTable(name, schema, params, false, true)

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

  test("create and drop pipe") {
    val schema =
      new StructType(
        Array(
          StructField("num", IntegerType, false)
        )
      )

    val pipe_name = s"spark_test_pipe_$randomSuffix"
    val stage_name = s"spark_test_stage_$randomSuffix"
    val table_name = s"spark_test_table_$randomSuffix"
    conn.createTable(table_name, schema, params, true, false)
    conn.createStage(stage_name)

    assert(!conn.pipeExists(pipe_name))
    conn.createPipe(pipe_name, ConstantString("copy into") + table_name + s"from @$stage_name", false)
    assert(conn.pipeExists(pipe_name))
    conn.createPipe(pipe_name, ConstantString("copy into") + table_name + s"from @$stage_name", true)
    assert(conn.pipeExists(pipe_name))
    conn.dropPipe(pipe_name)
    assert(!conn.pipeExists(pipe_name))

    conn.dropTable(table_name)
    conn.dropStage(stage_name)

  }

  test("test schema") {
    val name = s"spark_test_table_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("STR", StringType, false)
        )
      )

    conn.createTable(name, schema, params, true, false)

    conn.tableSchema(name, params).equals(schema)

    conn.dropTable(name)
  }

  test("copy from query") {
    val name = s"spark_test_table_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("num", IntegerType, false),
          StructField("str", StringType, false)
        )
      )

    conn.createTable(name, schema, params,true, false)

    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"""select num, str from $name where num = 0 and str = 'test'""")
      .load().show()

    conn.dropTable(name)

  }

  test("copy from query using udf") {

    //create table and insert data
    val name = s"spark_test_table_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("STR", StringType, false),
          StructField("NUM", IntegerType, false)
        )
      )
    conn.createTable(name, schema, params,true, false)
    conn.execute(s"insert into $name values('a',1),('b',2)")
    conn.execute(s"create or replace function test_function(n int) returns table (a int, b string) as $$$$ select num, str from $name where num = n$$$$")


    val gapstats_query = s"""select * from table(test_function(1)) gs"""
    val test_df =
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query",gapstats_query)
        .load()

    assert(test_df.collect().length == 1)

    conn.execute("drop function test_function(int)")
    conn.dropTable(name)
  }

  test("test union"){
    val name = s"spark_test_table_$randomSuffix"
    val schema =
      new StructType(
        Array(
          StructField("STR", StringType, false),
          StructField("NUM", IntegerType, false)
        )
      )
    conn.createTable(name, schema, params,true, false)
    conn.execute(s"insert into $name values('a',1),('b',2)")

    val df = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable",name)
        .load()


    df.createOrReplaceTempView("table1")

    val df1 = sparkSession.sql("select num,str from table1")
    val df2 = sparkSession.sql("select num,str from table1")
    val df3 = df1.union(df2)

    assert(df3.collect().length == 4)

    conn.dropTable(name)
  }

}