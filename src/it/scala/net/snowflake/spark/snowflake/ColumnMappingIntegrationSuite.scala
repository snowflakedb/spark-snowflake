package net.snowflake.spark.snowflake

import java.sql.Date

import net.snowflake.client.jdbc.SnowflakeSQLException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.types._
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

class ColumnMappingIntegrationSuite extends IntegrationSuiteBase {

  var df: DataFrame = _
  val dbtable = s"column_mapping_test_$randomSuffix"

  val dbtable1 = s"column_mapping_test_1_$randomSuffix"
  val dbtable2 = s"column_mapping_test_2_$randomSuffix"
  val dbtable3 = s"column_mapping_test_3_$randomSuffix"
  val dbtable4 = s"column_mapping_test_4_$randomSuffix"
  val dbtable5 = s"column_mapping_test_5_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val data: RDD[Row] = sc.makeRDD(List(Row(1, 2, 3, 4, 5), Row(2, 3, 4, 5, 6), Row(3, 4, 5, 6, 7)))
    val schema = StructType(List(
      StructField("one", IntegerType),
      StructField("two", IntegerType),
      StructField("three", IntegerType),
      StructField("four", IntegerType),
      StructField("five", IntegerType)
    ))
    df = sqlContext.createDataFrame(data, schema)
    jdbcUpdate(s"drop table if exists $dbtable")
    jdbcUpdate(s"create or replace table $dbtable1 (num int, str string)")
    jdbcUpdate(s"create or replace table $dbtable2 (num int, str string)")
    jdbcUpdate(s"create or replace table $dbtable3 (num int, str string)")
    jdbcUpdate(s"create or replace table $dbtable4 (num int, str string)")
    jdbcUpdate(s"create or replace table $dbtable5 (num int, str string)")
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $dbtable")
      jdbcUpdate(s"drop table if exists $dbtable1")
      jdbcUpdate(s"drop table if exists $dbtable2")
      jdbcUpdate(s"drop table if exists $dbtable3")
      jdbcUpdate(s"drop table if exists $dbtable4")
      jdbcUpdate(s"drop table if exists $dbtable5")
    } finally {
      super.afterAll()
    }
  }

  def checkTestTable(expectedAnswer: Seq[Row]): Unit = {
    val loadedDf = sqlContext.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"select * from $dbtable order by ONE")
      .load()
    checkAnswer(loadedDf, expectedAnswer)
  }

  test("Test columnMap") {

    jdbcUpdate(s"create or replace table $dbtable (ONE int, TWO int, THREE int, FOUR int, FIVE int)")


    df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
      .option("dbtable", dbtable)
      .option("columnmap", Map("one" -> "ONE", "five" -> "FOUR").toString())
      .mode(SaveMode.Append).save()

    checkTestTable(
      Seq(Row(1, null, null, 5, null),
        Row(2, null, null, 6, null),
        Row(3, null, null, 7, null))
    )

    // throw exception because only suppert SavaMode.Append
    assertThrows[UnsupportedOperationException] {
      df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
        .option("dbtable", dbtable)
        .option("columnmap", Map("one" -> "ONE", "five" -> "FOUR").toString())
        .mode(SaveMode.Overwrite).save()
    }

    // throw exception because "aaa" is not a column name of DF
    assertThrows[IllegalArgumentException] {
      df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
        .option("dbtable", dbtable)
        .option("columnmap", Map("aaa" -> "ONE", "five" -> "FOUR").toString())
        .mode(SaveMode.Append).save()
    }

    // throw exception because "AAA" is not a column name of table in snowflake database
    assertThrows[SnowflakeSQLException] {
      df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
        .option("dbtable", dbtable)
        .option("columnmap", Map("one" -> "AAA", "five" -> "FOUR").toString())
        .mode(SaveMode.Append).save()
    }

  }

  test("Automatic Column Mapping") {
    //auto map
    val schema1 = StructType(List(
      StructField("str", StringType),
      StructField("NUM", IntegerType)
    ))
    val data1: RDD[Row] = sc.makeRDD(List(Row("a", 1), Row("b", 2)))
    val df1 = sqlContext.createDataFrame(data1, schema1)

    //error by default
    assertThrows[SnowflakeSQLException] {
      df1.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
        .option("dbtable", dbtable1)
        .mode(SaveMode.Append)
        .save()
    }

    //no error when mapping by name
    df1.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
      .option("dbtable", dbtable1)
      .option("column_mapping", "name")
      .mode(SaveMode.Append)
      .save()

    val result1 = sparkSession.read.format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable1)
      .load()

    assert(result1.count() == 2)
    assert(result1.where(result1("NUM") === 1).count() == 1)
    assert(result1.where(result1("STR") === "b").count() == 1)

  }

  test("remove column from DataFrame") {
    val schema = StructType(List(
      StructField("str", StringType),
      StructField("useless", BooleanType),
      StructField("NUM", IntegerType)
    ))
    val data: RDD[Row] = sc.makeRDD(List(Row("a", true, 1), Row("b", false, 2)))
    val df = sqlContext.createDataFrame(data, schema)

    //error by default
    assertThrows[UnsupportedOperationException] {
      df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
        .option("dbtable", dbtable2)
        .option("column_mapping", "name")
        .mode(SaveMode.Append)
        .save()
    }

    df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
      .option("dbtable", dbtable2)
      .option("column_mapping", "name")
      .option("column_mismatch_behavior", "ignore")
      .mode(SaveMode.Append)
      .save()

    val result = sparkSession.read.format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable2)
      .load()

    assert(result.schema.size == 2)
    assert(result.count() == 2)
    assert(result.where(result("NUM") === 1).count() == 1)
    assert(result.where(result("STR") === "b").count() == 1)
  }

  test("add null column into DataFrame") {
    val schema = StructType(List(
      StructField("str", StringType)
    ))
    val data: RDD[Row] = sc.makeRDD(List(Row("a"), Row("b")))
    val df = sqlContext.createDataFrame(data, schema)

    assertThrows[UnsupportedOperationException] {
      df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
        .option("dbtable",dbtable3)
        .option("column_mapping", "name")
        .mode(SaveMode.Append)
        .save()
    }

    df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
      .option("dbtable",dbtable3)
      .option("column_mapping", "name")
      .option("column_mismatch_behavior", "ignore")
      .mode(SaveMode.Append)
      .save()

    val result = sparkSession.read.format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable3)
      .load()

    result.show()

    assert(result.schema.size == 2)
    assert(result.where(result("STR") === "a").count() == 1)
    assert(result.where(result("NUM").isNull).count() == 2)
  }
  test("mix behaviors") {
    val schema = StructType(List(
      StructField("str", StringType),
      StructField("useless", BooleanType)
    ))
    val data: RDD[Row] = sc.makeRDD(List(Row("a", true), Row("b", false)))
    val df = sqlContext.createDataFrame(data, schema)

    assertThrows[UnsupportedOperationException] {
      df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
        .option("dbtable", dbtable4)
        .option("column_mapping", "name")
        .mode(SaveMode.Append)
        .save()
    }

    df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
      .option("dbtable", dbtable4)
      .option("column_mapping", "name")
      .option("column_mismatch_behavior", "ignore")
      .mode(SaveMode.Append)
      .save()

    val result = sparkSession.read.format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", dbtable4)
      .load()

    assert(result.schema.size == 2)
    assert(result.where(result("STR") === "a").count() == 1)
    assert(result.where(result("NUM").isNull).count() == 2)

  }

  test("column type matching") {
    //todo: it works now (csv and json), but may be not work on binary format
    val schema = StructType(List(
      StructField("STR", DateType),
      StructField("NUM", IntegerType)
    ))
    val data: RDD[Row] = sc.makeRDD(List(Row(new Date(System.currentTimeMillis()),1),
      Row(new Date(System.currentTimeMillis()),2)))
    val df = sqlContext.createDataFrame(data, schema)

//    df.show()
//    df.printSchema()

    df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
      .option("dbtable", dbtable5)
      .option("column_mapping", "name")
      .mode(SaveMode.Append)
      .save()

//    sparkSession.read.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
//      .option("dbtable", dbtable5)
//      .load().show()
  }
}
