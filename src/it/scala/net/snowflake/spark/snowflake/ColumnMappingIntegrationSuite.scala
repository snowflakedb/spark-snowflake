package net.snowflake.spark.snowflake

import java.sql.Date
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_SHORT_NAME
import net.snowflake.client.jdbc.SnowflakeSQLException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.types._

class ColumnMappingIntegrationSuite extends IntegrationSuiteBase {

  var df: DataFrame = _
  val dbtable = s"column_mapping_test_$randomSuffix"

  val dbtable1 = s"column_mapping_test_1_$randomSuffix"
  val dbtable2 = s"column_mapping_test_2_$randomSuffix"
  val dbtable3 = s"column_mapping_test_3_$randomSuffix"
  val dbtable4 = s"column_mapping_test_4_$randomSuffix"
  val dbtable5 = s"column_mapping_test_5_$randomSuffix"
  val dbtable6 = s"column_mapping_test_6_$randomSuffix"
  val dbtable7 = s"column_mapping_test_7_$randomSuffix"
  val dbtable8 = s"column_mapping_test_8_$randomSuffix"
  val dbtable9 = s"column_mapping_test_9_$randomSuffix"
  val dbtable10 = s"column_mapping_test_10_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val data: RDD[Row] = sc.makeRDD(
      List(Row(1, 2, 3, 4, 5), Row(2, 3, 4, 5, 6), Row(3, 4, 5, 6, 7))
    )
    val schema = StructType(
      List(
        StructField("one", IntegerType),
        StructField("two", IntegerType),
        StructField("three", IntegerType),
        StructField("four", IntegerType),
        StructField("five", IntegerType)
      )
    )
    df = sqlContext.createDataFrame(data, schema)
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $dbtable")
      jdbcUpdate(s"drop table if exists $dbtable1")
      jdbcUpdate(s"drop table if exists $dbtable2")
      jdbcUpdate(s"drop table if exists $dbtable3")
      jdbcUpdate(s"drop table if exists $dbtable4")
      jdbcUpdate(s"drop table if exists $dbtable5")
      jdbcUpdate(s"drop table if exists $dbtable6")
      jdbcUpdate(s"drop table if exists $dbtable7")
      jdbcUpdate(s"drop table if exists $dbtable8")
      jdbcUpdate(s"drop table if exists $dbtable9")
      jdbcUpdate(s"drop table if exists $dbtable10")
    } finally {
      super.afterAll()
    }
  }

  def checkTestTable(expectedAnswer: Seq[Row]): Unit = {
    val loadedDf = sqlContext.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"select * from $dbtable order by ONE")
      .load()
    checkAnswer(loadedDf, expectedAnswer)
  }

  test("Test columnMap") {

    jdbcUpdate(
      s"create or replace table $dbtable (ONE int, TWO int, THREE int, FOUR int, FIVE int)"
    )

    df.write
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable)
      .option("columnmap", Map("one" -> "ONE", "five" -> "FOUR").toString())
      .mode(SaveMode.Append)
      .save()

    checkTestTable(
      Seq(
        Row(1, null, null, 5, null),
        Row(2, null, null, 6, null),
        Row(3, null, null, 7, null)
      )
    )

    // throw exception because only suppert SavaMode.Append
    assertThrows[UnsupportedOperationException] {
      df.write
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", dbtable)
        .option("columnmap", Map("one" -> "ONE", "five" -> "FOUR").toString())
        .mode(SaveMode.Overwrite)
        .save()
    }

    // throw exception because "aaa" is not a column name of DF
    assertThrows[IllegalArgumentException] {
      df.write
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", dbtable)
        .option("columnmap", Map("aaa" -> "ONE", "five" -> "FOUR").toString())
        .mode(SaveMode.Append)
        .save()
    }

    // throw exception because "AAA" is not a column name of table in snowflake database
    assertThrows[SnowflakeSQLException] {
      df.write
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", dbtable)
        .option("columnmap", Map("one" -> "AAA", "five" -> "FOUR").toString())
        .mode(SaveMode.Append)
        .save()
    }

  }

  test("Automatic Column Mapping") {
    jdbcUpdate(s"create or replace table $dbtable1 (num int, str string)")
    // auto map
    val schema1 = StructType(
      List(StructField("str", StringType), StructField("NUM", IntegerType))
    )
    val data1: RDD[Row] = sc.makeRDD(List(Row("a", 1), Row("b", 2)))
    val df1 = sqlContext.createDataFrame(data1, schema1)

    // error by default
    assertThrows[SnowflakeSQLException] {
      df1.write
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", dbtable1)
        .mode(SaveMode.Append)
        .save()
    }

    // no error when mapping by name
    df1.write
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable1)
      .option("column_mapping", "name")
      .mode(SaveMode.Append)
      .save()

    val result1 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable1)
      .load()

    assert(result1.count() == 2)
    assert(result1.where(result1("NUM") === 1).count() == 1)
    assert(result1.where(result1("STR") === "b").count() == 1)

  }

  test("remove column from DataFrame") {
    jdbcUpdate(s"create or replace table $dbtable2 (num int, str string)")
    val schema = StructType(
      List(
        StructField("str", StringType),
        StructField("useless", BooleanType),
        StructField("NUM", IntegerType)
      )
    )
    val data: RDD[Row] = sc.makeRDD(List(Row("a", true, 1), Row("b", false, 2)))
    val df = sqlContext.createDataFrame(data, schema)

    // error by default
    assertThrows[UnsupportedOperationException] {
      df.write
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", dbtable2)
        .option("column_mapping", "name")
        .mode(SaveMode.Append)
        .save()
    }

    df.write
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable2)
      .option("column_mapping", "name")
      .option("column_mismatch_behavior", "ignore")
      .mode(SaveMode.Append)
      .save()

    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable2)
      .load()

    assert(result.schema.size == 2)
    assert(result.count() == 2)
    assert(result.where(result("NUM") === 1).count() == 1)
    assert(result.where(result("STR") === "b").count() == 1)
  }

  test("add null column into DataFrame") {
    jdbcUpdate(s"create or replace table $dbtable3 (num int, str string)")
    val schema = StructType(List(StructField("str", StringType)))
    val data: RDD[Row] = sc.makeRDD(List(Row("a"), Row("b")))
    val df = sqlContext.createDataFrame(data, schema)

    assertThrows[UnsupportedOperationException] {
      df.write
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", dbtable3)
        .option("column_mapping", "name")
        .mode(SaveMode.Append)
        .save()
    }

    df.write
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable3)
      .option("column_mapping", "name")
      .option("column_mismatch_behavior", "ignore")
      .mode(SaveMode.Append)
      .save()

    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable3)
      .load()

    assert(result.schema.size == 2)
    assert(result.where(result("STR") === "a").count() == 1)
    assert(result.where(result("NUM").isNull).count() == 2)
  }
  test("mix behaviors") {
    jdbcUpdate(s"create or replace table $dbtable4 (num int, str string)")
    val schema = StructType(
      List(StructField("str", StringType), StructField("useless", BooleanType))
    )
    val data: RDD[Row] = sc.makeRDD(List(Row("a", true), Row("b", false)))
    val df = sqlContext.createDataFrame(data, schema)

    assertThrows[UnsupportedOperationException] {
      df.write
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", dbtable4)
        .option("column_mapping", "name")
        .mode(SaveMode.Append)
        .save()
    }

    df.write
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable4)
      .option("column_mapping", "name")
      .option("column_mismatch_behavior", "ignore")
      .mode(SaveMode.Append)
      .save()

    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable4)
      .load()

    assert(result.schema.size == 2)
    assert(result.where(result("STR") === "a").count() == 1)
    assert(result.where(result("NUM").isNull).count() == 2)

  }

  test("column type matching") {
    jdbcUpdate(s"create or replace table $dbtable5 (num int, str string)")
    // todo: it works now (csv and json), but may be not work on binary format
    val schema = StructType(
      List(StructField("STR", DateType), StructField("NUM", IntegerType))
    )
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(new Date(System.currentTimeMillis()), 1),
        Row(new Date(System.currentTimeMillis()), 2)
      )
    )
    val df = sqlContext.createDataFrame(data, schema)

    df.write
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable5)
      .option("column_mapping", "name")
      .mode(SaveMode.Append)
      .save()
  }

  test("Snowflake table contains duplicated column") {
    jdbcUpdate(s"""
    |create or replace table $dbtable6 ("FOO" int, "foo" string)
    """.stripMargin)

    val schema = StructType(
      List(StructField("DATE", DateType), StructField("foo", IntegerType))
    )
    val data: RDD[Row] = sc.makeRDD(
      List(
        Row(new Date(System.currentTimeMillis()), 1),
        Row(new Date(System.currentTimeMillis()), 2)
      )
    )
    val df = sqlContext.createDataFrame(data, schema)

    assertThrows[UnsupportedOperationException] {
      df.write
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", dbtable6)
        .option("column_mapping", "name")
        .option("column_mismatch_behavior", "ignore")
        .mode(SaveMode.Append)
        .save()
    }
  }

  test("Spark DataFrame contains duplicated column") {
    jdbcUpdate(s"""
                  |create or replace table $dbtable7 (foo int, bar string)
    """.stripMargin)

    val schema = StructType(
      List(StructField("Foo", IntegerType), StructField("foo", IntegerType))
    )
    val data: RDD[Row] = sc.makeRDD(List(Row(1, 2), Row(2, 3)))
    val df = sqlContext.createDataFrame(data, schema)

    assertThrows[UnsupportedOperationException] {
      df.write
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", dbtable7)
        .option("column_mapping", "name")
        .option("column_mismatch_behavior", "ignore")
        .mode(SaveMode.Append)
        .save()
    }
  }

  test("FOO and foo in Spark but no foo in Snowflake") {
    jdbcUpdate(s"""
                  |create or replace table $dbtable8 (num int, str string)
    """.stripMargin)

    val schema = StructType(
      List(
        StructField("Foo", IntegerType),
        StructField("foo", IntegerType),
        StructField("num", IntegerType)
      )
    )
    val data: RDD[Row] = sc.makeRDD(List(Row(1, 2, 4), Row(2, 3, 5)))
    val df = sqlContext.createDataFrame(data, schema)

    // no error
    df.write
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable8)
      .option("column_mapping", "name")
      .option("column_mismatch_behavior", "ignore")
      .mode(SaveMode.Append)
      .save()

    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable8)
      .load()

    assert(result.schema.size == 2)
    assert(result.where(result("NUM") === 4).count() == 1)
    assert(result.where(result("NUM") === 5).count() == 1)
    assert(result.where(result("STR").isNull).count() == 2)

  }

  test("FOO and foo in Snowflake but no foo in Spark") {
    jdbcUpdate(s"""
                  |create or replace table $dbtable9 (num int, "FOO" int, "foo" int)
    """.stripMargin)

    val schema = StructType(
      List(StructField("str", StringType), StructField("num", IntegerType))
    )
    val data: RDD[Row] = sc.makeRDD(List(Row("a", 4), Row("b", 5)))
    val df = sqlContext.createDataFrame(data, schema)

    // no error
    df.write
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable9)
      .option("column_mapping", "name")
      .option("column_mismatch_behavior", "ignore")
      .mode(SaveMode.Append)
      .save()

    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_SHORT_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable9)
      .load()

    assert(result.schema.size == 3)
    assert(result.where(result("NUM") === 4).count() == 1)
    assert(result.where(result("NUM") === 5).count() == 1)
    assert(result.where(result("FOO").isNull).count() == 2)
    assert(result.where(result("foo").isNull).count() == 2)

  }

  test("no matched column name") {
    jdbcUpdate(s"""
                  |create or replace table $dbtable10 (num int, str string)
    """.stripMargin)

    val schema = StructType(
      List(StructField("foo", StringType), StructField("bar", IntegerType))
    )
    val data: RDD[Row] = sc.makeRDD(List(Row("a", 4), Row("b", 5)))
    val df = sqlContext.createDataFrame(data, schema)

    assertThrows[UnsupportedOperationException] {
      df.write
        .format(SNOWFLAKE_SOURCE_SHORT_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", dbtable10)
        .option("column_mapping", "name")
        .option("column_mismatch_behavior", "ignore")
        .mode(SaveMode.Append)
        .save()
    }
  }
}
