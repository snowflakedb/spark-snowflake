package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.SnowflakeSQLException
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class OnErrorSuite extends IntegrationSuiteBase {
  lazy val table = s"spark_test_table_$randomSuffix"

  lazy val schema = new StructType(
    Array(StructField("var", StringType, nullable = false))
  )

  lazy val df: DataFrame = sparkSession.createDataFrame(
    sc.parallelize(
      Seq(Row("{\"dsadas\nadsa\":12311}"), Row("{\"abc\":334}")) // invalid json key
    ),
    schema
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    jdbcUpdate(s"create or replace table $table(var variant)")
  }

  override def afterAll(): Unit = {
    jdbcUpdate(s"drop table $table")
    super.afterAll()
  }

  test("continue_on_error off") {

    assertThrows[SnowflakeSQLException] {
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", table)
        .mode(SaveMode.Append)
        .save()
    }
  }

  test("continue_on_error on") {
    df.write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("continue_on_error", "on")
      .option("dbtable", table)
      .mode(SaveMode.Append)
      .save()

    val result = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", table)
      .load()

    assert(result.collect().length == 1)
  }

}
