package net.snowflake.spark.snowflake

import org.scalatest.FunSuite

class ColumnMappingSuite extends FunSuite {

  test("test parseMap function") {
    val origin = Map("col1" -> "col2", "col3" -> "col4", "col5" -> "col5")
    val result = Utils.parseMap(origin.toString())
    assert(origin.toString() == result.toString())
  }
}
