package net.snowflake.spark.snowflake

import org.scalatest.funsuite.AnyFunSuite

class ConnectorVersionSuite extends AnyFunSuite {
  test("Utils.VERSION matches sparkConnectorVersion in build.sbt") {
    assert(Utils.VERSION == System.getProperty("expectedConnectorVersion"))
  }
}
