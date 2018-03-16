package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.SnowflakeSQLException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME

class ColumnMappingintegrationSuite extends IntegrationSuiteBase {

  var df: DataFrame = _
  var dbtable = "column_mapping_test"

  override def beforeAll():Unit = {
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
  }

  override def afterAll(): Unit = {
    try{
      jdbcUpdate(s"drop table if exists $dbtable")
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

  test("Test columnMapping") {

    jdbcUpdate(s"create or replace table $dbtable (ONE int, TWO int, THREE int, FOUR int, FIVE int)")


    df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
      .option("dbtable", dbtable)
      .option("columnmap", Map("one" -> "ONE", "five" -> "FOUR").toString())
      .mode(SaveMode.Append).save()

    checkTestTable(
      Seq(Row(1,null,null,5,null),
        Row(2,null,null,6,null),
        Row(3,null,null,7,null))
    )

    assertThrows[UnsupportedOperationException]{
      df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
        .option("dbtable", dbtable)
        .option("columnmap", Map("one" -> "ONE", "five" -> "FOUR").toString())
        .mode(SaveMode.Overwrite).save()
    }

    assertThrows[IllegalArgumentException]{
      df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
        .option("dbtable", dbtable)
        .option("columnmap", Map("aaa" -> "ONE", "five" -> "FOUR").toString())
        .mode(SaveMode.Append).save()
    }
    assertThrows[SnowflakeSQLException]{
      df.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable)
        .option("dbtable", dbtable)
        .option("columnmap", Map("one" -> "AAA", "five" -> "FOUR").toString())
        .mode(SaveMode.Append).save()
    }

  }
}
