package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, to_json}

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
  test("read parquet") {
    // Define the schema

    val arrayColumns = (1 to 80).map(i =>
      StructField(s"arrayField$i", ArrayType(IntegerType), nullable = true))
    val otherColumns = (81 to 550).map(i => StructField(s"field$i", DoubleType, nullable = true))
    val schema = StructType(arrayColumns ++ otherColumns)

    // Generate random data
    val data = (1 to 10000).map { _ =>
      val arrayValues = (1 to 80).map(_ =>
        (1 to 100).map(_ => Random.nextInt(100)).toArray) // Random arrays of size 100
      val otherValues = (81 to 550).map(_ =>
        Random.nextDouble()) // Random strings of 10 characters
      Row.fromSeq(arrayValues ++ otherValues)
    }
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

  }



}