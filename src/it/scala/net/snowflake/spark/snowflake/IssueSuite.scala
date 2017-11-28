package net.snowflake.spark.snowflake

import org.apache.spark.sql.{Row, SaveMode}
import Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.types._

import scala.util.Random

/**
  * Created by ema on 6/29/17.
  * For testing fixes for specific miscellaneous issues (bug fixes).
  */
class IssueSuite extends IntegrationSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  test("Column names can be reserved keywords if quoted.") {
    // For saving into SF, use reserved names to see if they work (should be auto-quoted)
    // Fix for (https://github.com/snowflakedb/spark-snowflake/issues/12)

    val numRows = 100

    val st1 = new StructType(
      Array(StructField("id", IntegerType, nullable = true),
            StructField("order", IntegerType, nullable = true),
            StructField("sort", StringType, nullable = true),
            StructField("select", BooleanType, nullable = true),
            StructField("randLong", LongType, nullable = true)))

    val tt: String = s"tt_$randomSuffix"

    try {
      sqlContext
        .createDataFrame(sc.parallelize(1 to numRows)
                           .map[Row](value => {
                             val rand = new Random(System.nanoTime())
                             Row(value,
                                 rand.nextInt(),
                                 rand.nextString(10),
                                 rand.nextBoolean(),
                                 rand.nextLong())
                           }),
                         st1)
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .mode(SaveMode.Overwrite)
        .save()

      val loadedDf = sqlContext.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", tt)
        .load()

      assert(loadedDf.collect.length == numRows)

    } finally {
      jdbcUpdate(s"drop table if exists $tt")
    }
  }

  test("Empty strings do not cause exceptions on non-nullable columns.") {

    val numRows = 100

    val st1 = new StructType(
      Array(StructField("str", StringType, nullable = false),
            StructField("randLong", LongType, nullable = true)))

    val tt: String = s"tt_$randomSuffix"

    try {

      def getString(rand: Random, value: Int): String = {
        if (value % 3 == 2) ""
        else rand.nextString(10)
      }

      sqlContext
        .createDataFrame(sc.parallelize(1 to numRows)
                           .map[Row](value => {
                             val rand = new Random(System.nanoTime())
                             Row(getString(rand, value), rand.nextLong())
                           }),
                         st1)
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .mode(SaveMode.Overwrite)
        .save()

      val loadedDf = sqlContext.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", tt)
        .load()

      assert(loadedDf.collect.length == numRows)
    } finally {
      jdbcUpdate(s"drop table if exists $tt")
    }
  }

  test("Analyze should not show sensitive data.") {
    val numRows = 100

    val st1 = new StructType(
      Array(StructField("str", StringType, nullable = false),
        StructField("randLong", LongType, nullable = true)))

    val tt: String = s"tt_$randomSuffix"

    try {

      def getString(rand: Random, value: Int): String = {
        if (value % 3 == 2) ""
        else rand.nextString(10)
      }

      sqlContext
        .createDataFrame(sc.parallelize(1 to numRows)
          .map[Row](value => {
          val rand = new Random(System.nanoTime())
          Row(getString(rand, value), rand.nextLong())
        }),
          st1)
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .mode(SaveMode.Overwrite)
        .save()

      val loadedDf = sqlContext.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", tt)
        .load()

      loadedDf.explain()

    } finally {
      jdbcUpdate(s"drop table if exists $tt")
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
}
