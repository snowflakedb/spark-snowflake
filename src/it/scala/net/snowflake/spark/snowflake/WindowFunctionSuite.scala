package net.snowflake.spark.snowflake

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

class WindowFunctionSuite extends IntegrationSuiteBase {

  test("test row number function") {

    val df = sparkSession.read.format("snowflake").options(connectorOptionsNoTable).option("dbtable", "test").load()

    //todo: row_number function works but filter "rank=1" doesn't work yet
    val windowSpec = Window.partitionBy(new Column("id")).orderBy(new Column("time").desc)
    df.withColumn("rank", row_number.over(windowSpec)).filter("rank=1").show()
    //todo: automatically verify pushdown query in this test
    //todo: test keep_column_case="on" with lower case column name 
  }

}
