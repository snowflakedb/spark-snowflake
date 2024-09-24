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

  test("trim space - csv") {
    val st1 = new StructType(
      Array(StructField("str", StringType, nullable = false))
    )
    val tt: String = s"tt_$randomSuffix"
    try {
      val df = sparkSession
        .createDataFrame(
          sparkSession.sparkContext.parallelize(
            Seq(
              Row("ab c"),
              Row(" a bc"),
              Row("abdc  ")
            )
          ),
          st1
        )
        df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .option(Parameters.PARAM_TRIM_SPACE, "true")
        .mode(SaveMode.Overwrite)
        .save()

      var loadDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .load()

      assert(loadDf.collect().forall(row => row.toSeq.head.toString.length == 4))

      // disabled by default
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .mode(SaveMode.Overwrite)
        .save()

      loadDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .load()
      val result = loadDf.collect()
      assert(result.head.toSeq.head.toString.length == 4)
      assert(result(1).toSeq.head.toString.length == 5)
      assert(result(2).toSeq.head.toString.length == 6)


    } finally {
      jdbcUpdate(s"drop table if exists $tt")
    }
  }

  test("trim space - json") {
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
            Seq(
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

  test("csv delimiter character should not break rows") {
    val st1 = new StructType(
      Array(
        StructField("str", StringType, nullable = false),
        StructField("num", IntegerType, nullable = false)
      )
    )
    val tt: String = s"tt_$randomSuffix"
    try {
      sparkSession
        .createDataFrame(
          sparkSession.sparkContext.parallelize(
            Seq(
              Row("\"\n\"", 123),
              Row("\"|\"", 223),
              Row("\",\"", 345),
              Row("\n", 423)
            )
          ),
          st1
        )
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .mode(SaveMode.Overwrite)
        .save()

      val loadDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .load()

      assert(loadDf.collect().length == 4)

    } finally {
      jdbcUpdate(s"drop table if exists $tt")
    }

  }

  test("Column names can be reserved keywords if quoted.") {
    // For saving into SF, use reserved names to see if they work (should be auto-quoted)
    // Fix for (https://github.com/snowflakedb/spark-snowflake/issues/12)

    val numRows = 100

    val st1 = new StructType(
      Array(
        StructField("id", IntegerType, nullable = true),
        StructField("order", IntegerType, nullable = true),
        StructField("sort", StringType, nullable = true),
        StructField("select", BooleanType, nullable = true),
        StructField("randLong", LongType, nullable = true)
      )
    )

    val tt: String = s"tt_$randomSuffix"

    try {
      sparkSession
        .createDataFrame(
          sc.parallelize(1 to numRows)
            .map[Row](value => {
              val rand = new Random(System.nanoTime())
              Row(
                value,
                rand.nextInt(),
                rand.nextString(10),
                rand.nextBoolean(),
                rand.nextLong()
              )
            }),
          st1
        )
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .mode(SaveMode.Overwrite)
        .save()

      val loadedDf = sparkSession.read
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
      Array(
        StructField("str", StringType, nullable = false),
        StructField("randLong", LongType, nullable = true)
      )
    )

    val tt: String = s"tt_$randomSuffix"

    try {

      def getString(rand: Random, value: Int): String = {
        if (value % 3 == 2) ""
        else rand.nextString(10)
      }

      sparkSession
        .createDataFrame(
          sc.parallelize(1 to numRows)
            .map[Row](value => {
              val rand = new Random(System.nanoTime())
              Row(getString(rand, value), rand.nextLong())
            }),
          st1
        )
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .mode(SaveMode.Overwrite)
        .save()

      val loadedDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", tt)
        .load()

      assert(loadedDf.collect.length == numRows)
    } finally {
      jdbcUpdate(s"drop table if exists $tt")
    }
  }

  test("truncate columns option") {
    val numRows = 100

    val st1 = new StructType(
      Array(
        StructField("str", StringType, nullable = false),
        StructField("randLong", LongType, nullable = true)
      )
    )

    val tt: String = s"tt_$randomSuffix"

    try {

      def getString(rand: Random, value: Int): String = {
        if (value % 3 == 2) ""
        else rand.nextString(10)
      }

      sparkSession
        .createDataFrame(
          sc.parallelize(1 to numRows)
            .map[Row](value => {
              val rand = new Random(System.nanoTime())
              Row(getString(rand, value), rand.nextLong())
            }),
          st1
        )
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(
          connectorOptions
            ++ Seq("truncate_columns" -> "on")
        )
        .option("dbtable", tt)
        .mode(SaveMode.Overwrite)
        .save()

      assert(Utils.getLastCopyLoad contains "TRUNCATECOLUMNS = TRUE")

      val loadedDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", tt)
        .load()

      loadedDf.explain()

    } finally {
      jdbcUpdate(s"drop table if exists $tt")
    }
  }

  test("Analyze should not show sensitive data.") {
    val numRows = 100

    val st1 = new StructType(
      Array(
        StructField("str", StringType, nullable = false),
        StructField("randLong", LongType, nullable = true)
      )
    )

    val tt: String = s"tt_$randomSuffix"

    try {

      def getString(rand: Random, value: Int): String = {
        if (value % 3 == 2) ""
        else rand.nextString(10)
      }

      sparkSession
        .createDataFrame(
          sc.parallelize(1 to numRows)
            .map[Row](value => {
              val rand = new Random(System.nanoTime())
              Row(getString(rand, value), rand.nextLong())
            }),
          st1
        )
        .write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tt)
        .mode(SaveMode.Overwrite)
        .save()

      val loadedDf = sparkSession.read
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
