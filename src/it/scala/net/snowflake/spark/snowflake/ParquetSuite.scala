package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType, StructField, StructType, TimestampNTZType, TimestampType}
import org.apache.spark.sql.functions.{col, to_json}

import java.sql.{Date, Timestamp}
import java.time.LocalDateTime
import scala.util.Random

class ParquetSuite extends IntegrationSuiteBase {
  test("create table") {
    val rowNum = 100000
    val colNum = 550
    val arrColNum = 80
    val schema = StructType(
      (0 until (colNum - arrColNum)).map(x => {
        StructField(s"COL$x", StringType)
      }) ++ ((colNum - arrColNum) until colNum).map(x => {
        StructField(s"COL$x", StringType)
      })
    )

    val rows = sparkSession.sparkContext.parallelize((0 until rowNum).map(_ => {
      Row((0 until (colNum - arrColNum))
        .map(_ => Random.alphanumeric.take(20).mkString) ++
        ((colNum - arrColNum) until colNum)
          .map(_ => (1 to 10)
            .map(_ => Random.nextInt()).mkString("[", ",", "]"))
        : _*)
    }))

    val df = sparkSession.createDataFrame(rows, schema)
    df.write.parquet("mix2")
    //    df.write.format(Utils.SNOWFLAKE_SOURCE_NAME)
    //      .mode(SaveMode.Overwrite)
    //      .options(connectorOptionsNoTable)
    //      .option("dbtable", name)
    //      .save()

    df.schema.printTreeString()
  }

  test("test csv") {
    val df = sparkSession.read.parquet("integer")
    val cached = df.cache()
    cached.schema.printTreeString()
    val t1 = System.currentTimeMillis()
    cached.write.format(Utils.SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "false")
      .option("dbtable", "parquet_test1")
      .mode(SaveMode.Overwrite)
      .save()
    val t2 = System.currentTimeMillis()
    // scalastyle:off println
    println(
      s"""
         |XXXXXXXXXX
         |time: ${t2 - t1}
         |XXXXXXXXXX
         |""".stripMargin
    )
    // scalastyle:on println
  }
  test("test csv 2") {
    val df = sparkSession.read.parquet("arr2")
    val cached = df.cache()
    cached.schema.printTreeString()
    // scalastyle:off println
    println(cached.count())
    val t0 = System.currentTimeMillis()
    val newDf = cached.select(
      ((0 until 470).map(x => col(s"COL$x")) ++
        (470 until 550).map(x => to_json(col(s"COL$x")).as(s"COL$x"))): _*
    )
    val newCached = newDf.cache()
    newCached.schema.printTreeString()
    println(newCached.count())
    val t1 = System.currentTimeMillis()
    newCached.write.format(Utils.SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "false")
      .option("dbtable", "parquet_test1")
      .mode(SaveMode.Overwrite)
      .save()
    val t2 = System.currentTimeMillis()

    println(
      s"""
         |XXXXXXXXXX
         |time1: ${t1 - t0}
         |time: ${t2 - t1}
         |XXXXXXXXXX
         |""".stripMargin
    )
    // scalastyle:on println
  }

  test("test parquet") {
    val df = sparkSession.read.parquet("arr2")
    val cached = df.cache()
    cached.schema.printTreeString()
    cached.count()
    val t1 = System.currentTimeMillis()
    cached.write.format(Utils.SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "true")
      .option("dbtable", "parquet_test1")
      .mode(SaveMode.Overwrite)
      .save()
    val t2 = System.currentTimeMillis()
    // scalastyle:off println
    println(
      s"""
         |XXXXXXXXXX
         |time: ${t2 - t1}
         |XXXXXXXXXX
         |""".stripMargin
    )
    // scalastyle:on println
  }

  test("test file") {
    val df = sparkSession.read.format(Utils.SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", "parquet_test")
      .load()

    val cached = df.cache()
    cached.write.parquet("parquet.out")
    cached.write.csv("csv.out")
    cached.write.json("json.out")
    // scalastyle:on println
  }
  test("test parquet with all type") {
    val data = Seq(
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
//        LocalDateTime.now(),
        Date.valueOf("2023-01-01")
      ),
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
//      StructField("timestampntz_col", TimestampNTZType, true),
      StructField("DATE_COL", DateType, true)
    ))
    val rdd = sparkSession.sparkContext.parallelize(data)
    val df = sparkSession.createDataFrame(rdd, schema)
    val start = System.currentTimeMillis()
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", "test_parquet")
      //      .option(Parameters.PARAM_USE_PARQUET_IN_WRITE, "false")
      .mode(SaveMode.Overwrite)
      .save()
    val end = System.currentTimeMillis()
    print(end - start)

    val newDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", "test_parquet")
      .load()
    newDf.show()
    print(newDf.schema)
  }
}