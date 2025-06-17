package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

class CopyConfigSuite extends IntegrationSuiteBase {
  lazy val table = s"spark_test_table_$randomSuffix"

  lazy val schema = new StructType(
    Array(StructField("str", StringType, nullable = false))
  )

  lazy val df: DataFrame = sparkSession.createDataFrame(
    sc.parallelize(
      Seq(Row("abc"), Row("def")) // invalid json key
    ),
    schema
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    jdbcUpdate(s"create or replace table $table(str string)")
  }

  override def afterAll(): Unit = {
    jdbcUpdate(s"drop table $table")
    super.afterAll()
  }

  test("force off") {
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("force", "off")
      .mode(SaveMode.Overwrite)
      .save()

    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .load()

    checkAnswer(result, df.collect())
    assert(!Utils.getLastCopyLoad.contains("FORCE"))
  }

  test("force on") {
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .option("force", "on")
      .mode(SaveMode.Overwrite)
      .save()

    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .load()

    checkAnswer(result, df.collect())
    assert(Utils.getLastCopyLoad.contains("FORCE = TRUE"))
  }

  test("force default") {
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .mode(SaveMode.Overwrite)
      .save()

    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .load()

    checkAnswer(result, df.collect())
    assert(!Utils.getLastCopyLoad.contains("FORCE"))
  }
}
