package net.snowflake.spark.snowflake

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._

class ColumnNameCaseSuite extends IntegrationSuiteBase {

  val table = s"spark_test_table_$randomSuffix"
  val table1 = s"spark_test_table_1_$randomSuffix"
  val table2 = s"spark_test_table_2_$randomSuffix"
  val table3 = s"spark_test_table_3_$randomSuffix"

  override def afterAll(): Unit = {
    conn.dropTable(table)
    conn.dropTable(table1)
    conn.dropTable(table2)
    conn.dropTable(table3)
    super.afterAll()
  }

  test("reading table with lower case column name"){
    val schema = StructType(List(
      StructField("num", IntegerType),
      StructField("str", StringType)
    ))
    val data = sc.makeRDD(List(Row(1,null),Row(null,"b")))

    val df = sqlContext.createDataFrame(data, schema)

    df.write.format("snowflake")
      .options(connectorOptionsNoTable)
      .option("dbtable", table1)
      .option("keep_column_case", "on")
      .save()

    var df1 = sparkSession.read.format("snowflake")
      .options(connectorOptionsNoTable)
      .option("dbtable", table1)
      .option("keep_column_case", "on")
      .load()

    df1 = df1.where(col("str").isNotNull)

    assert(df1.count() == 1)
  }

  test("reading table with lower case column name without keepColumnName"){
    val schema = StructType(List(
      StructField("num", IntegerType),
      StructField("str", StringType)
    ))
    val data = sc.makeRDD(List(Row(1,null),Row(null,"b")))

    val df = sqlContext.createDataFrame(data, schema)

    df.write.format("snowflake")
      .options(connectorOptionsNoTable)
      .option("dbtable", table2)
      .option("keep_column_case", "on")
      .save()

    var df1 = sparkSession.read.format("snowflake")
      .options(connectorOptionsNoTable)
      .option("dbtable", table2)
      .option("keep_column_case", "off")
      .load()

    df1 = df1.where(col("\"str\"").isNotNull)

    assert(df1.count() == 1)
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

  test("Test pushdown query on GROUP BY Operator"){
    jdbcUpdate(s"""create table $table3 ("col" int)""")

    val df = sparkSession
      .read
      .format("snowflake")
      .options(connectorOptionsNoTable)
      .option("dbtable", table3)
      .option("keep_column_case", "on")
      .load()

    df.select("col")
      .groupBy("col")
      .agg(count("*").alias("new_col"))
      .count()

    val result =
      s"""
         |SELECT ( COUNT ( 1 ) ) AS "SUBQUERY_2_COL_0" FROM ( SELECT * FROM
         |( SELECT * FROM ( $table3 ) AS "SF_CONNECTOR_QUERY_ALIAS" ) AS
         | "SUBQUERY_0" GROUP BY "SUBQUERY_0"."col" ) AS "SUBQUERY_1"
         |""".stripMargin.replaceAll("\\s","")

    assert(Utils.getLastSelect.replaceAll("\\s","").equals(result))
  }

}
