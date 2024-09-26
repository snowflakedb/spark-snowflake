package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Utils.{SNOWFLAKE_SOURCE_NAME, SNOWFLAKE_SOURCE_SHORT_NAME}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, StringType, StructField, StructType, TimestampType}

import java.sql.{Date, SQLException, Timestamp}
import scala.collection.Seq
import scala.util.Random

class ParquetSuite extends IntegrationSuiteBase {
  val test_parquet_table: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_parquet_column_map: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_special_character: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString

  override def afterAll(): Unit = {
    runSql(s"drop table if exists $test_parquet_table")
    runSql(s"drop table if exists $test_parquet_column_map")
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
      .option("dbtable", test_parquet_table)
      .mode(SaveMode.Overwrite)
      .save()


    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_parquet_table)
      .load()

    val expectedAnswer = List(
      Row(1, "string value", 123456789, 123.45,
        true, BigDecimal("12345.6789").bigDecimal.setScale(10),
        Timestamp.valueOf("2023-09-16 10:15:30"), Date.valueOf("2023-01-01")
      )
    )
    checkAnswer(newDf, expectedAnswer)
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
      .option("dbtable", test_parquet_table)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .mode(SaveMode.Overwrite)
      .save()


    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_parquet_table)
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
      .option("dbtable", test_parquet_table)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .mode(SaveMode.Overwrite)
      .save()


    val res = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_parquet_table)
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
      .option("dbtable", test_parquet_table)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .mode(SaveMode.Overwrite)
      .save()

    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_parquet_table)
      .load()

    checkAnswer(newDf, List(Row(1, 2, 3)))
  }

  test("test parquet name conversion with column map by name"){
    jdbcUpdate(
      s"create or replace table $test_parquet_column_map (ONE int, TWO int, THREE int, Four int)"
    )


    val schema = StructType(List(
      StructField("TWO", IntegerType, true),
      StructField("ONE", IntegerType, true),
      StructField("FOUR", IntegerType, true),
      StructField("THREE", IntegerType, true),
    ))
    val data: RDD[Row] = sc.makeRDD(
      List(Row(1, 2, 3, 4))
    )
    val df = sparkSession.createDataFrame(data, schema)
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_parquet_column_map)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .option("column_mapping", "name")
      .mode(SaveMode.Append)
      .save()

    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_parquet_column_map)
      .load()

    checkAnswer(newDf, List(Row(2, 1, 4, 3)))
    assert(newDf.schema.map(field => field.name)
      .mkString(",") == Seq("ONE", "TWO", "THREE", "FOUR").mkString(","))
  }

  test("test parquet name conversion with column map"){
    jdbcUpdate(
      s"create or replace table $test_parquet_column_map (ONE int, TWO int, THREE int, Four int)"
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
      .option("dbtable", test_parquet_column_map)
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
      .option("dbtable", test_parquet_column_map)
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
    val tt: String = s"tt_$randomSuffix"
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
        .option("dbtable", tt)
        .option(Parameters.PARAM_TRIM_SPACE, "true")
        .mode(SaveMode.Overwrite)
        .save()

      var loadDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .load()

      assert(loadDf.select("str").collect().forall(row => row.toSeq.head.toString.length == 4))

      // disabled by default
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
        .option("dbtable", tt)
        .mode(SaveMode.Overwrite)
        .save()

      loadDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .load()
      val result = loadDf.select("str").collect()
      assert(result.head.toSeq.head.toString.length == 4)
      assert(result(1).toSeq.head.toString.length == 5)
      assert(result(2).toSeq.head.toString.length == 6)

    } finally {
      jdbcUpdate(s"drop table if exists $tt")
    }
  }

  test("test date time"){
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(
          Timestamp.valueOf("0001-12-30 10:15:30"),
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
      .option("dbtable", test_parquet_table)
      .mode(SaveMode.Overwrite)
      .save()

    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_parquet_table)
      .load()
    newDf.show()

    checkAnswer(newDf, List(
      Row(
        Timestamp.valueOf("0001-12-30 10:15:30"),
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
      .option("dbtable", test_parquet_table)
      .mode(SaveMode.Overwrite)
      .save()

    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_parquet_table)
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
      s"""create or replace table $test_special_character
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
      .option("dbtable", test_special_character)
      .mode(SaveMode.Append)
      .save()

    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_special_character)
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
      s"create or replace table $test_parquet_column_map (ONE int, TWO int, THREE int, Four int)"
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
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
        .option("dbtable", test_parquet_column_map)
        .option("columnmap", Map("UPPER_CLASS_COL" -> "ONE", "lower_class_col" -> "FOUR").toString())
        .mode(SaveMode.Overwrite)
        .save()
    }

    // throw exception because "aaa" is not a column name of DF
    assertThrows[IllegalArgumentException] {
      df.write
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
        .option("dbtable", test_parquet_column_map)
        .option("columnmap", Map("aaa" -> "ONE", "Mix_Class_Col" -> "FOUR").toString())
        .mode(SaveMode.Append)
        .save()
    }

    // throw exception because "AAA" is not a column name of table in snowflake database
    assertThrows[IllegalArgumentException] {
      df.write
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
        .option("dbtable", test_parquet_column_map)
        .option("columnmap", Map("UPPER_CLASS_COL" -> "AAA", "Mix_Class_Col" -> "FOUR").toString())
        .mode(SaveMode.Append)
        .save()
    }
  }
}