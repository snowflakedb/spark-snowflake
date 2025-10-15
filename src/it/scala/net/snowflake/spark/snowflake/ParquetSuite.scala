package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Utils.{SNOWFLAKE_SOURCE_NAME, SNOWFLAKE_SOURCE_SHORT_NAME}
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, StringType, StructField, StructType, TimestampType}

import java.sql.{Date, SQLException, Timestamp}
import scala.collection.Seq
import scala.util.Random

class ParquetSuite extends IntegrationSuiteBase {
  val test_all_type: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_all_type_multi_line: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_array_map: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_conversion: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_conversion_by_name: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_column_map: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_trim: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_date: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_special_char: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_special_char_to_exist: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_column_map_parquet: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_column_map_not_match: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_nested_dataframe: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_no_staging_table: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_table_name: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString

  override def afterAll(): Unit = {
    jdbcUpdate(s"drop table if exists $test_all_type")
    jdbcUpdate(s"drop table if exists $test_all_type_multi_line")
    jdbcUpdate(s"drop table if exists $test_array_map")
    jdbcUpdate(s"drop table if exists $test_conversion")
    jdbcUpdate(s"drop table if exists $test_conversion_by_name")
    jdbcUpdate(s"drop table if exists $test_column_map")
    jdbcUpdate(s"drop table if exists $test_trim")
    jdbcUpdate(s"drop table if exists $test_date")
    jdbcUpdate(s"drop table if exists $test_special_char")
    jdbcUpdate(s"drop table if exists $test_special_char_to_exist")
    jdbcUpdate(s"drop table if exists $test_column_map_parquet")
    jdbcUpdate(s"drop table if exists $test_column_map_not_match")
    jdbcUpdate(s"drop table if exists $test_nested_dataframe")
    jdbcUpdate(s"drop table if exists $test_no_staging_table")
    jdbcUpdate(s"drop table if exists $test_table_name")
    super.afterAll()
  }

  test("test parquet with all type") {
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(
          1,
          "string value",
          123456789L,
          123.45,
          true,
          BigDecimal("12345.6789").bigDecimal,
          Timestamp.valueOf("2023-09-16 10:15:30"),
          Date.valueOf("2023-01-01")
        )
      )
    )

    val schema = StructType(List(
      StructField("INT_COL", IntegerType, true),
      StructField("STRING_COL", StringType, true),
      StructField("LONG_COL", LongType, true),
      StructField("DOUBLE_COL", DoubleType, true),
      StructField("BOOLEAN_COL", BooleanType, true),
      StructField("DECIMAL_COL", DecimalType(20, 10), true),
      StructField("TIMESTAMP_COL", TimestampType, true),
      StructField("DATE_COL", DateType, true)
    ))
    val df = sparkSession.createDataFrame(data, schema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .option("dbtable", test_all_type)
      .mode(SaveMode.Overwrite)
      .save()


    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_all_type)
      .load()

    val expectedAnswer = List(
      Row(1, "string value", 123456789, 123.45,
        true, BigDecimal("12345.6789").bigDecimal.setScale(10),
        Timestamp.valueOf("2023-09-16 10:15:30"), Date.valueOf("2023-01-01")
      )
    )
    checkAnswer(newDf, expectedAnswer)

    // assert no staging table is left
    val res = sparkSession.sql(s"show tables like '%${test_all_type}_STAGING%'").collect()
    assert(res.length == 0)
  }

  test("test parquet with all type and multiple lines"){
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(1, "string value", 123456789L, 123.45, true,
          BigDecimal("12345.6789").bigDecimal,
          Timestamp.valueOf("2023-09-16 10:15:30"),
          Date.valueOf("2023-01-01")
        ),
        Row(2, "another string", 123456789L, 123.45, false,
          BigDecimal("12345.6789").bigDecimal,
          Timestamp.valueOf("2024-09-16 10:15:30"),
          Date.valueOf("2024-01-01")
        )
      )
    )

    val schema = StructType(List(
      StructField("INT_COL", IntegerType, true),
      StructField("STRING_COL", StringType, true),
      StructField("LONG_COL", LongType, true),
      StructField("DOUBLE_COL", DoubleType, true),
      StructField("BOOLEAN_COL", BooleanType, true),
      StructField("DECIMAL_COL", DecimalType(20, 10), true),
      StructField("TIMESTAMP_COL", TimestampType, true),
      StructField("DATE_COL", DateType, true)
    ))
    val df = sparkSession.createDataFrame(data, schema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_all_type_multi_line)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .mode(SaveMode.Overwrite)
      .save()


    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_all_type_multi_line)
      .load()

    val expectedAnswer = List(
      Row(1, "string value", 123456789, 123.45,
        true, BigDecimal("12345.6789").bigDecimal,
        Timestamp.valueOf("2023-09-16 10:15:30"), Date.valueOf("2023-01-01")
      ),
      Row(2, "another string", 123456789, 123.45,
        false, BigDecimal("12345.6789").bigDecimal,
        Timestamp.valueOf("2024-09-16 10:15:30"), Date.valueOf("2024-01-01")
      )
    )

    checkAnswer(newDf, expectedAnswer)
  }

  test("test array and map type with parquet"){
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(
          Array("one", "two", "three"),
          Array(1, 2, 3),
          Map("a" -> 1),
        ),
        Row(
          Array("one", "two", "three"),
          Array(1, 2, 3),
          Map("b" -> 2),
        )
      )
    )

    val schema = StructType(List(
      StructField("ARRAY_STRING_FIELD",
        ArrayType(StringType, containsNull = true), nullable = true),
      StructField("ARRAY_INT_FILED", ArrayType(IntegerType, containsNull = true), nullable = true),
      StructField("MAP_FILED", MapType(StringType, IntegerType), nullable = true),
    ))
    val df = sparkSession.createDataFrame(data, schema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_array_map)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .mode(SaveMode.Overwrite)
      .save()


    val res = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_array_map)
      .schema(schema)
      .load()
      .collect()
    assert(res(0).getList[Int](1).get(0) == 1)
    assert(res(1).getList[Int](1).get(2) == 3)
    assert(res(0).getList[String](0).get(0) == "one")
    assert(res(1).getList[String](0).get(2) == "three")
    assert(res(0).getMap[String, Integer](2)("a") == 1)
    assert(res(1).getMap[String, Integer](2)("b") == 2)

  }

  test("test parquet name conversion without column map"){
    val data: RDD[Row] = sc.makeRDD(
      List(Row(1, 2, 3))
    )
    val schema = StructType(List(
      StructField("UPPER_CLASS_COL", IntegerType, true),
      StructField("lower_class_col", IntegerType, true),
      StructField("Mix_Class_Col", IntegerType, true),
    ))
    val df = sparkSession.createDataFrame(data, schema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_conversion)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .mode(SaveMode.Overwrite)
      .save()

    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_conversion)
      .load()

    checkAnswer(newDf, List(Row(1, 2, 3)))
  }

  test("test parquet name conversion with column map by name"){
    jdbcUpdate(
      s"""create or replace table $test_conversion_by_name
        |(ONE int, TWO int, THREE int, "Fo.ur" int)""".stripMargin
    )


    val schema = StructType(List(
      StructField("TWO", IntegerType, true),
      StructField("ONE", IntegerType, true),
      StructField("FO.UR", IntegerType, true),
      StructField("THREE", IntegerType, true),
    ))
    val data: RDD[Row] = sc.makeRDD(
      List(Row(1, 2, 3, 4))
    )
    val df = sparkSession.createDataFrame(data, schema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_conversion_by_name)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .option("column_mapping", "name")
      .mode(SaveMode.Append)
      .save()

    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_conversion_by_name)
      .load()

    checkAnswer(newDf, List(Row(2, 1, 4, 3)))
    assert(newDf.schema.map(field => field.name)
      .mkString(",") == Seq("ONE", "TWO", "THREE", """"Fo.ur"""").mkString(","))

    // assert no staging table is left
    val res = sparkSession.sql(s"show tables like '%${test_conversion_by_name}_STAGING%'").collect()
    assert(res.length == 0)
  }

  test("test parquet name conversion with column map"){
    jdbcUpdate(
      s"create or replace table $test_column_map (ONE int, TWO int, THREE int, Four int)"
    )


    val schema = StructType(List(
      StructField("UPPER_CLASS_COL", IntegerType, true),
      StructField("lower_class_col", IntegerType, true),
      StructField("Mix_Class_Col", IntegerType, true),
    ))
    val data: RDD[Row] = sc.makeRDD(
      List(Row(1, 2, 3))
    )
    val df = sparkSession.createDataFrame(data, schema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_column_map)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .option("columnmap", Map(
        "UPPER_CLASS_COL" -> "ONE",
        "lower_class_col" -> "TWO",
        "Mix_Class_Col" -> "THREE",
      ).toString())
      .mode(SaveMode.Append)
      .save()

    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_column_map)
      .load()

    checkAnswer(newDf, List(Row(1, 2, 3, null)))
    assert(newDf.schema.map(field => field.name)
      .mkString(",") == Seq("ONE", "TWO", "THREE", "FOUR").mkString(","))
  }

  test("trim space - parquet") {
    val st1 = new StructType(
      Array(
        StructField("str", StringType, nullable = false),
        StructField("arr", ArrayType(IntegerType), nullable = false)
      )
    )
    try {
      val df = sparkSession
        .createDataFrame(
          sparkSession.sparkContext.parallelize(
            List(
              Row("ab c", Array(1, 2, 3)),
              Row(" a bc", Array(2, 2, 3)),
              Row("abdc  ", Array(3, 2, 3))
            )
          ),
          st1
        )
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
        .option("dbtable", test_trim)
        .option(Parameters.PARAM_TRIM_SPACE, "true")
        .mode(SaveMode.Overwrite)
        .save()

      var loadDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", test_trim)
        .load()

      assert(loadDf.select("str").collect().forall(row => row.toSeq.head.toString.length == 4))

      // disabled by default
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
        .option("dbtable", test_trim)
        .mode(SaveMode.Overwrite)
        .save()

      loadDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", test_trim)
        .load()
      val result = loadDf.select("str").collect()
      assert(result.head.toSeq.head.toString.length == 4)
      assert(result(1).toSeq.head.toString.length == 5)
      assert(result(2).toSeq.head.toString.length == 6)

    } finally {
      jdbcUpdate(s"drop table if exists $test_trim")
    }
  }

  test("test date time"){
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(
          Timestamp.valueOf("0001-12-30 10:15:30.111"),
          Date.valueOf("0001-03-01")
        )
      )
    )

    val schema = StructType(List(
      StructField("TIMESTAMP_COL", TimestampType, true),
      StructField("DATE_COL", DateType, true)
    ))

    val df = sparkSession.createDataFrame(data, schema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .option("dbtable", test_date)
      .mode(SaveMode.Overwrite)
      .save()

    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_date)
      .load()
    newDf.show()

    checkAnswer(newDf, List(
      Row(
        Timestamp.valueOf("0001-12-30 10:15:30.111"),
        Date.valueOf("0001-03-01")
      )
    ))
  }

  test("test parquet with special character"){
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(
          Timestamp.valueOf("0001-12-30 10:15:30"),
          Date.valueOf("0001-03-01")
        )
      )
    )

    val schema = StructType(List(
      StructField("\"timestamp.()col\"", TimestampType, true),
      StructField("date.()col", DateType, true)
    ))

    val df = sparkSession.createDataFrame(data, schema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .option("dbtable", test_special_char)
      .mode(SaveMode.Overwrite)
      .save()

    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_special_char)
      .load()
    newDf.show()

    checkAnswer(newDf, List(
      Row(
        Timestamp.valueOf("0001-12-30 10:15:30"),
        Date.valueOf("0001-03-01")
      )
    ))
    assert(newDf.schema.fieldNames.contains("\"timestamp.()col\""))
  }

  test("test parquet with special character to existing table"){
    jdbcUpdate(
      s"""create or replace table $test_special_char_to_exist
         |("timestamp1.()col" timestamp, "date1.()col" date)""".stripMargin
    )

    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(
          Timestamp.valueOf("0001-12-30 10:15:30"),
          Date.valueOf("0001-03-01")
        )
      )
    )

    val schema = StructType(List(
      StructField("\"timestamp1.()col\"", TimestampType, true),
      StructField("date1.()col", DateType, true)
    ))

    val df = sparkSession.createDataFrame(data, schema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .option("dbtable", test_special_char_to_exist)
      .mode(SaveMode.Append)
      .save()

    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_special_char_to_exist)
      .load()
    newDf.show()

    checkAnswer(newDf, List(
      Row(
        Timestamp.valueOf("0001-12-30 10:15:30"),
        Date.valueOf("0001-03-01")
      )
    ))
    assert(newDf.schema.fieldNames.contains("\"timestamp1.()col\""))
  }

  test("Test columnMap with parquet") {
    jdbcUpdate(
      s"create or replace table $test_column_map_parquet (ONE int, TWO int, THREE int, Four int)"
    )


    val schema = StructType(List(
      StructField("UPPER_CLASS_COL", IntegerType, true),
      StructField("lower_class_col", IntegerType, true),
      StructField("Mix_Class_Col", IntegerType, true),
    ))
    val data: RDD[Row] = sc.makeRDD(
      List(Row(1, 2, 3))
    )
    val df = sparkSession.createDataFrame(data, schema)

    // throw exception because only support SaveMode.Append
    assertThrows[UnsupportedOperationException] {
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
        .option("dbtable", test_column_map_parquet)
        .option("columnmap", Map("UPPER_CLASS_COL" -> "ONE", "lower_class_col" -> "FOUR").toString())
        .mode(SaveMode.Overwrite)
        .save()
    }

    // throw exception because "aaa" is not a column name of DF
    assertThrows[IllegalArgumentException] {
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
        .option("dbtable", test_column_map_parquet)
        .option("columnmap", Map("aaa" -> "ONE", "Mix_Class_Col" -> "FOUR").toString())
        .mode(SaveMode.Append)
        .save()
    }

    // throw exception because "AAA" is not a column name of table in snowflake database
    assertThrows[IllegalArgumentException] {
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
        .option("dbtable", test_column_map_parquet)
        .option("columnmap", Map("UPPER_CLASS_COL" -> "AAA", "Mix_Class_Col" -> "FOUR").toString())
        .mode(SaveMode.Append)
        .save()
    }
  }

  test("null value in array") {
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(
          Array(null, "one", "two", "three"),
        ),
        Row(
          Array("one", null, "two", "three"),
        )
      )
    )

    val schema = StructType(List(
      StructField("ARRAY_STRING_FIELD",
        ArrayType(StringType, containsNull = true), nullable = true)))
    val df = sparkSession.createDataFrame(data, schema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_array_map)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .mode(SaveMode.Overwrite)
      .save()


    val res = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_array_map)
      .schema(schema)
      .load().collect()
    assert(res.head.getSeq(0) == Seq("null", "one", "two", "three"))
    assert(res(1).getSeq(0) == Seq("one", "null", "two", "three"))
  }

  test("test error when column map does not match") {
    jdbcUpdate(s"create or replace table $test_column_map_not_match (num int, str string)")
    // auto map
    val schema1 = StructType(
      List(StructField("str", StringType), StructField("NUM", IntegerType))
    )
    val data1: RDD[Row] = sc.makeRDD(List(Row("a", 1), Row("b", 2)))
    val df1 = sparkSession.createDataFrame(data1, schema1)

    assertThrows[SQLException]{
      df1.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
        .option("dbtable", test_column_map_not_match)
        .mode(SaveMode.Append)
        .save()
    }
  }

  test("test nested dataframe"){

    val data = sc.parallelize(
      List(
        Row(123, Array(1, 2, 3), Map("a" -> 1), Row("abc")),
        Row(456, Array(4, 5, 6), Map("b" -> 2), Row("def")),
        Row(789, Array(7, 8, 9), Map("c" -> 3), Row("ghi"))
      )
    )

    val schema1 = new StructType(
      Array(
        StructField("NUM", IntegerType, nullable = true),
        StructField("ARR", ArrayType(IntegerType), nullable = true),
        StructField("MAP", MapType(StringType, IntegerType), nullable = true),
        StructField(
          "OBJ",
          StructType(Array(StructField("str", StringType, nullable = true)))
        )
      )
    )

    val df = sparkSession.createDataFrame(data, schema1)

    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .option("dbtable", test_nested_dataframe)
      .mode(SaveMode.Overwrite)
      .save()

    val out = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_nested_dataframe)
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
    assert(result(2).getAs[Row]("OBJ").getAs[String]("str") == "ghi")
  }

  test("test parquet not using staging table") {
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(
          1,
          "string value",
          123456789L,
          123.45,
          true,
          BigDecimal("12345.6789").bigDecimal,
          Timestamp.valueOf("2023-09-16 10:15:30"),
          Date.valueOf("2023-01-01")
        )
      )
    )

    val schema = StructType(List(
      StructField("INT_COL", IntegerType, true),
      StructField("STRING_COL", StringType, true),
      StructField("LONG_COL", LongType, true),
      StructField("DOUBLE_COL", DoubleType, true),
      StructField("BOOLEAN_COL", BooleanType, true),
      StructField("DECIMAL_COL", DecimalType(20, 10), true),
      StructField("TIMESTAMP_COL", TimestampType, true),
      StructField("DATE_COL", DateType, true)
    ))
    val df = sparkSession.createDataFrame(data, schema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .option("usestagingtable", "false")
      .option("dbtable", test_no_staging_table)
      .mode(SaveMode.Overwrite)
      .save()


    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_no_staging_table)
      .load()

    val expectedAnswer = List(
      Row(1, "string value", 123456789, 123.45,
        true, BigDecimal("12345.6789").bigDecimal.setScale(10),
        Timestamp.valueOf("2023-09-16 10:15:30"), Date.valueOf("2023-01-01")
      )
    )
    checkAnswer(newDf, expectedAnswer)

    // assert no staging table is left
    val res = sparkSession.sql(s"show tables like '%${test_all_type}_STAGING%'").collect()
    assert(res.length == 0)
  }

  test("use parquet in structured type by default") {
    // use CSV by default
    sparkSession
      .sql("select 1")
      .write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_name)
      .mode(SaveMode.Overwrite)
      .save()
    assert(Utils.getLastCopyLoad.contains("TYPE=CSV"))

    // use Parquet on structured types
    sparkSession
      .sql("select array(1, 2)")
      .write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_name)
      .mode(SaveMode.Overwrite)
      .save()
    assert(Utils.getLastCopyLoad.contains("TYPE=PARQUET"))

    // use Json on structured types when PARAM_USE_JSON_IN_STRUCTURED_DATA is true
    sparkSession
      .sql("select array(1, 2)")
      .write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_name)
      .option(Parameters.PARAM_USE_JSON_IN_STRUCTURED_DATA, "true")
      .mode(SaveMode.Overwrite)
      .save()
    assert(Utils.getLastCopyLoad.contains("TYPE = JSON"))

    // PARAM_USE_PARQUET_IN_WRITE can overwrite PARAM_USE_JSON_IN_STRUCTURED_DATA
    sparkSession
      .sql("select array(1, 2)")
      .write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_name)
      .option(Parameters.PARAM_USE_JSON_IN_STRUCTURED_DATA, "true")
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .mode(SaveMode.Overwrite)
      .save()
    assert(Utils.getLastCopyLoad.contains("TYPE=PARQUET"))
  }

  test("parquet arrays/maps of decimals should succeed") {
    val rows = java.util.Arrays.asList(
      Row(
        Array(BigDecimal("1.23").bigDecimal, BigDecimal("4.56").bigDecimal),
        Map("a" -> BigDecimal("7.89").bigDecimal)
      )
    )

    val schema = StructType(List(
      StructField("ARR_DEC", ArrayType(DecimalType(10, 2), containsNull = true), nullable = true),
      StructField("MAP_DEC", MapType(StringType, DecimalType(10, 2)), nullable = true)
    ))

    val df = sparkSession.createDataFrame(rows, schema)

    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .option("dbtable", Random.alphanumeric.filter(_.isLetter).take(10).mkString)
      .mode(SaveMode.Overwrite)
      .save()
  }
}