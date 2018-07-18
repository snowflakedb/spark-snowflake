package net.snowflake.spark.snowflake

import org.apache.spark.sql.types._
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
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

  val tableName1 = s"spark_test_table_$randomSuffix"
  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"create or replace table $tableName1 (arr array, ob object, map variant, num integer)")
    jdbcUpdate(s"""insert into $tableName1 (select parse_json('[1,2,3,4]'), parse_json('{"str":"text"}'), parse_json('{"a":"one","b":"two"}'), 123)""")
    jdbcUpdate(s"""insert into $tableName1 (select parse_json('[1,2,3,4]'), parse_json('{"str":"text"}'), parse_json('{"a":"one","b":"two"}'), 123)""")
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
        Row(123, Array(1,2,3)),
        Row(456, Array(4,5,6)),
        Row(789, Array(7,8,9))
      )
    )
    val schema = new StructType(
      Array(
        StructField("NUM", IntegerType, false),
        StructField("ARR", ArrayType(IntegerType), false)
      )
    )

    val df = sparkSession.createDataFrame(data, schema)

    df.show()

    df.write.format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      //.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .option("dbtable", "test123")
      .mode(SaveMode.Overwrite)
      .save()

  }

}
