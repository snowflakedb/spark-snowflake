package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

import java.sql.{Date, Timestamp}
import scala.collection.Seq
import scala.util.Random

class ParquetSuite extends IntegrationSuiteBase {
  val test_parquet_table: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString
  val test_parquet_column_map: String = Random.alphanumeric.filter(_.isLetter).take(10).mkString

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
          123.45f,
          true,
          BigDecimal("12345.6789").bigDecimal,
          Array("one", "two", "three"),
          Array(1, 2, 3),
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
      StructField("FLOAT_COL", FloatType, true),
      StructField("BOOLEAN_COL", BooleanType, true),
      StructField("DECIMAL_COL", DecimalType(20, 10), true),
      StructField("ARRAY_STRING_FIELD",
        ArrayType(StringType, containsNull = true), nullable = true),
      StructField("ARRAY_INT_FILED", ArrayType(IntegerType, containsNull = true), nullable = true),
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
      Row(
        1,
        "string value",
        123456789,
        123.45,
        123.44999694824219,
        true,
        BigDecimal("12345.6789").bigDecimal,
        """[
          |  "one",
          |  "two",
          |  "three"
          |]""".stripMargin,
        """[
          |  1,
          |  2,
          |  3
          |]""".stripMargin,
        Timestamp.valueOf("2023-09-16 10:15:30"),
        Date.valueOf("2023-01-01")
      )
    )

    checkAnswer(newDf, expectedAnswer)
  }

  test("test parquet with all type and multiple lines"){
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(1, "string value", 123456789L, 123.45, 123.45f, true,
          BigDecimal("12345.6789").bigDecimal,
          Array("one", "two", "three"),
          Array(1, 2, 3),
          Timestamp.valueOf("2023-09-16 10:15:30"),
          Date.valueOf("2023-01-01")
        ),
        Row(2, "another string", 123456789L, 123.45, 123.45f, false,
          BigDecimal("12345.6789").bigDecimal,
          Array("one", "two", "three"),
          Array(1, 2, 3),
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
      StructField("FLOAT_COL", FloatType, true),
      StructField("BOOLEAN_COL", BooleanType, true),
      StructField("DECIMAL_COL", DecimalType(20, 10), true),
      StructField("ARRAY_STRING_FIELD",
        ArrayType(StringType, containsNull = true), nullable = true),
      StructField("ARRAY_INT_FILED", ArrayType(IntegerType, containsNull = true), nullable = true),
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
      Row(1, "string value", 123456789, 123.45, 123.44999694824219,
        true, BigDecimal("12345.6789").bigDecimal,
        """[
          |  "one",
          |  "two",
          |  "three"
          |]""".stripMargin,
        """[
          |  1,
          |  2,
          |  3
          |]""".stripMargin,
        Timestamp.valueOf("2023-09-16 10:15:30"), Date.valueOf("2023-01-01")
      ),
      Row(2, "another string", 123456789, 123.45, 123.44999694824219,
        false, BigDecimal("12345.6789").bigDecimal,
        """[
          |  "one",
          |  "two",
          |  "three"
          |]""".stripMargin,
        """[
          |  1,
          |  2,
          |  3
          |]""".stripMargin,
        Timestamp.valueOf("2024-09-16 10:15:30"), Date.valueOf("2024-01-01")
      )
    )

    checkAnswer(newDf, expectedAnswer)
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
}