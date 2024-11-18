package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.types._
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.{Row, SaveMode}

class VariantTypeSuite extends IntegrationSuiteBase {

  lazy val schema = new StructType(
    Array(
      StructField("ARR", ArrayType(IntegerType), nullable = false),
      StructField(
        "OB",
        StructType(Array(StructField("str", StringType, nullable = false))),
        nullable = false
      ),
      StructField("MAP", MapType(StringType, StringType), nullable = false),
      StructField("NUM", IntegerType, nullable = false)
    )
  )

  val tableName1 = s"spark_test_table_1$randomSuffix"
  val tableName2 = s"spark_test_table_2$randomSuffix"
  val tableName3 = s"spark_test_table_3$randomSuffix"
  val tableName4 = s"spark_test_table_4$randomSuffix"
  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(
      s"create or replace table $tableName1 (arr array, ob object, map variant, num integer)"
    )
    jdbcUpdate(
      s"""insert into $tableName1 (select parse_json('[1,2,3,4]'), parse_json('{"str":"text"}'),
         | parse_json('{"a":"one","b":"two"}'), 123)""".stripMargin
    )
    jdbcUpdate(
      s"""insert into $tableName1 (select parse_json('[1,2,3,4]'), parse_json('{"str":"text"}'),
         | parse_json('{"a":"one","b":"two"}'), 123)""".stripMargin
    )
  }

  override def afterAll(): Unit = {
    jdbcUpdate(s"drop table if exists $tableName1")
    jdbcUpdate(s"drop table if exists $tableName2")
    jdbcUpdate(s"drop table if exists $tableName3")
    jdbcUpdate(s"drop table if exists $tableName4")
    super.afterAll()
  }

  test("unload non variant data") {
    val df = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName1)
      .load()
      .collect()

    assert(df(0).get(0).isInstanceOf[String])
    assert(df(0).get(1).isInstanceOf[String])
    assert(df(0).get(2).isInstanceOf[String])
    assert(df(0).get(3).toString == "123")
    assert(df.length == 2)
  }
  test("unload variant data") {
    val df = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName1)
      .schema(schema)
      .load()
      .collect()

    assert(df(0).getStruct(1).getString(0) == "text")

    assert(df(0).getSeq(0).length == 4)

    assert(df(0).getMap[String, String](2)("a") == "one")
    assert(df(0).getMap[String, String](2)("b") == "two")

    assert(df(0).getInt(3) == 123)

    assert(df.length == 2)
  }

  test("load variant data") {

    val data = sc.parallelize(
      Seq(
        Row(123, Array(1, 2, 3), Map("a" -> 1), Row("abc")),
        Row(456, Array(4, 5, 6), Map("b" -> 2), Row("def")),
        Row(789, Array(7, 8, 9), Map("c" -> 3), Row("ghi"))
      )
    )

    val schema1 = new StructType(
      Array(
        StructField("NUM", IntegerType, nullable = false),
        StructField("ARR", ArrayType(IntegerType), nullable = false),
        StructField("MAP", MapType(StringType, IntegerType), nullable = false),
        StructField(
          "OBJ",
          StructType(Array(StructField("STR", StringType, nullable = false)))
        )
      )
    )

    val df = sparkSession.createDataFrame(data, schema1)

    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName2)
      .mode(SaveMode.Overwrite)
      .save()

    val out = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName2)
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

  // This test case can be removed when removing PARAM_INTERNAL_USE_PARSE_JSON_FOR_WRITE
  // because above test "load variant data" has covered it.
  test("load variant data with parse_json()") {

    val data = sc.parallelize(
      Seq(
        Row(123, Array(1, 2, 3), Map("a" -> 1), Row("abc")),
        Row(456, Array(4, 5, 6), Map("b" -> 2), Row("def")),
        Row(789, Array(7, 8, 9), Map("c" -> 3), Row("ghi"))
      )
    )

    val schema1 = new StructType(
      Array(
        StructField("NUM", IntegerType, nullable = false),
        StructField("ARR", ArrayType(IntegerType), nullable = false),
        StructField("MAP", MapType(StringType, IntegerType), nullable = false),
        StructField(
          "OBJ",
          StructType(Array(StructField("STR", StringType, nullable = false)))
        )
      )
    )

    val df = sparkSession.createDataFrame(data, schema1)

    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName2)
      .option(Parameters.PARAM_INTERNAL_USE_PARSE_JSON_FOR_WRITE, "true")
      .option(Parameters.PARAM_USE_JSON_IN_STRUCTURED_DATA, "true")
      .mode(SaveMode.Overwrite)
      .save()

    val out = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName2)
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
    jdbcUpdate(
      s"insert into $tableName3 select object_construct('a', '1 | 2 | 3')," +
        s"array_construct('a | b', 'c | d')"
    )

    val df = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName3)
      .load()

    val result = df.collect()

    val mapper: ObjectMapper = new ObjectMapper()
    val tree0 = mapper.readTree(result(0).get(0).toString)
    assert(tree0.findValue("a").asText().equals("1 | 2 | 3"))
    val tree1 = mapper.readTree(result(0).get(1).toString)
    assert(
      tree1.get(0).asText().equals("a | b") && tree1
        .get(1)
        .asText()
        .equals("c | d")
    )
  }

  test ("load variant + binary column") {
    // COPY UNLOAD can't be run because it doesn't support binary
    if (!params.useCopyUnload) {
      val data = sc.parallelize(
        Seq(
          Row("binary1".getBytes(), Array(1, 2, 3), Map("a" -> 1), Row("abc")),
          Row("binary2".getBytes(), Array(4, 5, 6), Map("b" -> 2), Row("def")),
          Row("binary3".getBytes(), Array(7, 8, 9), Map("c" -> 3), Row("ghi"))
        )
      )

      val schema1 = new StructType(
        Array(
          StructField("BIN", BinaryType, nullable = false),
          StructField("ARR", ArrayType(IntegerType), nullable = false),
          StructField("MAP", MapType(StringType, IntegerType), nullable = false),
          StructField(
            "OBJ",
            StructType(Array(StructField("STR", StringType, nullable = false)))
          )
        )
      )

      val df = sparkSession.createDataFrame(data, schema1)

      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", tableName4)
        .option(Parameters.PARAM_USE_JSON_IN_STRUCTURED_DATA, "true")
        .mode(SaveMode.Overwrite)
        .save()

      val out = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", tableName4)
        .schema(schema1)
        .load()

      val result = out.collect()
      assert(result.length == 3)

      val bin = result(0).get(0).asInstanceOf[Array[Byte]]
      assert(new String(bin).equals("binary1"))
      assert(result(0).getList[Int](1).get(0) == 1)
      assert(result(1).getList[Int](1).get(1) == 5)
      assert(result(2).getList[Int](1).get(2) == 9)
      assert(result(1).getMap[String, Int](2)("b") == 2)
      assert(result(2).getStruct(3).getString(0) == "ghi")
    }
  }

}
