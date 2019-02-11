package net.snowflake.spark.snowflake_beta

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import net.snowflake.spark.snowflake_beta.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake_beta.DefaultJDBCWrapper.DataBaseOperations

class ColumnNameCaseSuite extends IntegrationSuiteBase {

  val table = s"spark_test_table_$randomSuffix"

  override def afterAll(): Unit = {
    conn.dropTable(table)
    super.afterAll()
  }


  test("column number should not contains quotes"){

    val schema = StructType(List(
      StructField("2014 two", IntegerType),
      StructField("one", IntegerType)
    ))

    val data: RDD[Row] = sc.makeRDD(List(Row(1,2)))

    val df = sqlContext.createDataFrame(data, schema)

    df.write.format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable",table)
        .option("keep_column_case","on")
        .mode(SaveMode.Overwrite)
        .save()

    val df1 = sparkSession.read.format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", table)
        .option("keep_column_case","on")
        .load()


    val result = df1.schema.toList

    assert(result.size == 2)
    assert(result.head.name == "2014 two")
    assert(result(1).name == "one")
  }

}
