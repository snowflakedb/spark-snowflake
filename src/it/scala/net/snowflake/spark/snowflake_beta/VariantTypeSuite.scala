package net.snowflake.spark.snowflake_beta

import org.apache.spark.sql.types._
import net.snowflake.spark.snowflake_beta.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.{Row, SaveMode}

class VariantTypeSuite extends IntegrationSuiteBase {

  lazy val schema = new StructType(
    Array(
      StructField("ARR", ArrayType(IntegerType), false),
      StructField("OB",StructType(
        Array(
          StructField("str", StringType, false)
        )
      ), false),
      StructField("MAP", MapType(StringType,StringType), false),
      StructField("NUM", IntegerType, false)
    )
  )

  val tableName1 = s"spark_test_table_1$randomSuffix"
  val tableName2 = s"spark_test_table_2$randomSuffix"
  val tableName3 = s"spark_test_table_3$randomSuffix"
  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"create or replace table $tableName1 (arr array, ob object, map variant, num integer)")
    jdbcUpdate(s"""insert into $tableName1 (select parse_json('[1,2,3,4]'), parse_json('{"str":"text"}'), parse_json('{"a":"one","b":"two"}'), 123)""")
    jdbcUpdate(s"""insert into $tableName1 (select parse_json('[1,2,3,4]'), parse_json('{"str":"text"}'), parse_json('{"a":"one","b":"two"}'), 123)""")
  }


  override def afterAll(): Unit = {
    jdbcUpdate(s"drop table if exists $tableName1")
    jdbcUpdate(s"drop table if exists $tableName2")
    jdbcUpdate(s"drop table if exists $tableName3")
    super.afterAll()
  }

  test("unload non variant data") {
    val df = sqlContext.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName1)
      .load().collect()

    assert(df(0).get(0).isInstanceOf[String])
    assert(df(0).get(1).isInstanceOf[String])
    assert(df(0).get(2).isInstanceOf[String])
    assert(df(0).get(3).toString == "123")
    assert(df.length == 2)
  }
  test("unload variant data") {
    val df = sqlContext.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName1)
      .schema(schema)
      .load().collect()

    assert(df(0).getStruct(1).getString(0)=="text")

    assert(df(0).getSeq(0).length == 4)

    assert(df(0).getMap[String,String](2).get("a").get == "one")
    assert(df(0).getMap[String,String](2).get("b").get == "two")

    assert(df(0).getInt(3) == 123)

    assert(df.length == 2)
  }

  test("load variant data") {

    val data = sc.parallelize(
      Seq(
        Row(123, Array(1,2,3), Map("a"->1), Row("abc")),
        Row(456, Array(4,5,6), Map("b"->2), Row("def")),
        Row(789, Array(7,8,9), Map("c"->3), Row("ghi"))
      )
    )
    val schema1 = new StructType(
      Array(
        StructField("NUM", IntegerType, false),
        StructField("ARR", ArrayType(IntegerType), false),
        StructField("MAP", MapType(StringType, IntegerType), false),
        StructField("OBJ", StructType(Array(StructField("STR", StringType, false))))
      )
    )

    val df = sparkSession.createDataFrame(data, schema1)

    df.write.format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName2)
      .mode(SaveMode.Overwrite)
      .save()

    val out = sparkSession.read.format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable",tableName2)
      .schema(schema1)
      .load()

    val result = out.collect()
    assert(result.length == 3)

    assert(result(0).getInt(0) == 123)
    assert(result(0).getList[Int](1).get(0) == 1)
    assert(result(1).getList[Int](1).get(1) == 5)
    assert(result(2).getList[Int](1).get(2) == 9)
    assert(result(1).getMap[String, Int](2)("b") == 2)
    assert(result(2).getStruct(3).getString(0) == "ghi")

  }

  test("unload object and array") {
    jdbcUpdate(s"create or replace table $tableName3(o object, a array)")
    jdbcUpdate(s"insert into $tableName3 select object_construct('a', '1 | 2 | 3'), array_construct('a | b', 'c | d')")

    val df = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable",tableName3)
      .load()

    val result = df.collect()

    assert(result(0).get(0).toString == "{\"a\":\"1 | 2 | 3\"}")
    assert(result(0).get(1).toString == "[\"a | b\",\"c | d\"]")

  }

}
