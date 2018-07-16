package net.snowflake.spark.snowflake

import org.apache.spark.sql.types._
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

class VariantTypeSuite extends IntegrationSuiteBase {

  lazy val schema = new StructType(
    Array(
      StructField("NUM", IntegerType, false),
      StructField("VAR",StructType(
        Array(
          StructField("str", StringType, false)
        )
      ), false)
    )
  )

  val tableName1 = s"spark_test_table_$randomSuffix"
  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"create or replace table $tableName1 (num int, var variant)")
    jdbcUpdate(s"""insert into $tableName1 (select 123, parse_json('{"str":"text"}'))""")
  }


  override def afterAll(): Unit = {
    jdbcUpdate(s"drop table if exists $tableName1")
    super.afterAll()
  }

  test("unload non variant data") {
    val df = sqlContext.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName1)
      .load()

    //df.show()

    assert(df.collect().length == 1)
  }
  test("unload variant data") {
    val df = sqlContext.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName1)
      .schema(schema)
      .load()

    assert(df.collect()(0).getStruct(1).getString(0)=="text")

    assert(df.collect().length == 1)
  }

}
