/*
 * Copyright 2015-2016 Snowflake Computing
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.snowflake.spark.snowflake_beta.benchmarks

import java.util.Properties

import net.snowflake.spark.snowflake_beta.DefaultJDBCWrapper
import net.snowflake.spark.snowflake_beta.Utils._
import org.apache.spark.sql._

import scala.collection.mutable

// This suite only runs with the Snowflake deployment defined in the conf file, so we fill the parquet and csv
// options with the same DF for those.

class RegDeploymentSuite extends PerformanceSuite {

  override var requiredParams = {
    val map = new mutable.LinkedHashMap[String, String]
    map.put("RegSuite", "")
    map
  }
  override var acceptedArguments = {
    val map = new mutable.LinkedHashMap[String, Set[String]]
    map.put("RegSuite", Set("*"))
    map
  }

  override protected var dataSources: mutable.LinkedHashMap[
    String,
    Map[String, DataFrame]] =
    new mutable.LinkedHashMap[String, Map[String, DataFrame]]

  override def beforeAll(): Unit = {
    super.beforeAll()

    if (runTests) {
      val lineitem = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", "LINEITEM")
        .option("sfSchema", "TESTSCHEMA")
        .load()

      val jdbc_lineitem = if (jdbcSource) {
        sparkSession.read.jdbc(jdbcURL, "LINEITEM", jdbcProperties)
      } else lineitem

      dataSources.put("LINEITEM",
                      Map("parquet"   -> lineitem,
                          "csv"       -> lineitem,
                          "snowflake" -> lineitem,
                          "jdbc"      -> jdbc_lineitem))

      val ordersTiny = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", "ORDERSTINY")
        .option("sfSchema", "TESTSCHEMA")
        .load()

      val jdbc_ordersTiny = if (jdbcSource) {
        sparkSession.read.jdbc(jdbcURL, "ORDERSTINY", jdbcProperties)
      } else ordersTiny

      dataSources.put("ORDERSTINY",
                      Map("parquet"   -> ordersTiny,
                          "csv"       -> ordersTiny,
                          "snowflake" -> ordersTiny,
                          "jdbc"      -> jdbc_ordersTiny))

      val orders = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", "ORDERS")
        .option("sfSchema", "TESTSCHEMA")
        .load()

      val jdbc_orders = if (jdbcSource) {
        sparkSession.read.jdbc(jdbcURL, "ORDERS", jdbcProperties)
        orders
      } else orders

      dataSources.put("ORDERS",
                      Map("parquet"   -> orders,
                          "csv"       -> orders,
                          "snowflake" -> orders,
                          "jdbc"      -> jdbc_orders))
    }
  }

  test("SELECT ALL FROM LINEITEM") {
    testQuery("SELECT * FROM LINEITEM", "LINEITEM all")
  }

  test("AGGREGATE BY C15") {
    testQuery(
      "SELECT C15 AS TYPE, SUM(C2) as SUM_C2, AVG(C3) AS AVG_C3 FROM LINEITEM GROUP BY C15",
      "Aggregate LINEITEM by transport type (C15)")
  }

  test("AGGREGATE BY C14 AND C15") {
    testQuery(
      "SELECT C15 AS TYPE, SUM(C2) as SUM_C2, AVG(C3) AS AVG_C3 FROM LINEITEM GROUP BY C14,C15",
      "Aggregate LINEITEM by delivery status and transport type (C14,C15)")
  }

  test("JOIN ORDERS AND ORDERSTINY") {
    testQuery("SELECT * FROM ORDERS O1 JOIN ORDERSTINY O2 ON O1.C2=O2.C2",
              "Join orders and orderstiny on c2")
  }

  test("JOIN ORDERS AND LINEITEM") {
    testQuery("SELECT * FROM ORDERS O JOIN LINEITEM L ON O.C6=L.C14",
              "Join orders and lineitem on c6 and c14")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    try {} finally {
      super.afterAll()
    }
  }

}
