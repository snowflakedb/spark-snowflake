package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}

class SFTableNameSuite extends IntegrationSuiteBase {
  lazy val tableName = s""""spark_test_table_$randomSuffix""""

  override def afterAll(): Unit = {
    conn.dropTable(tableName)
    super.afterAll()
  }

  test("table name include \"") {
    val schema = StructType(
      List(StructField("num", IntegerType), StructField("str", StringType))
    )

    val data = sc.parallelize(Seq(Row(1, "a"), Row(2, "b")))

    val df = sparkSession.createDataFrame(data, schema)

    df.write
      .format("snowflake")
      .options(connectorOptions)
      .option("dbtable", tableName)
      .mode(SaveMode.Overwrite)
      .save()

    val result = sparkSession.read
      .format("snowflake")
      .options(connectorOptions)
      .option("dbtable", tableName)
      .load()
      .count()

    assert(result == 2)

  }
}
