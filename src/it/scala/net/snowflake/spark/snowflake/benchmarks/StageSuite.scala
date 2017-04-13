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

package net.snowflake.spark.snowflake.benchmarks

import net.snowflake.spark.snowflake.Utils._
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable

class StageSuite extends PerformanceSuite {

  protected final var s3RootDir: String = ""

  protected final val tables: Seq[String] = Seq("TEST_TABLE_5984932164318553871")

  override var requiredParams = {
    val map = new mutable.LinkedHashMap[String, String]
    map.put("TPCDSSuite", "")
    map
  }

  override var acceptedArguments = {
    val map = new mutable.LinkedHashMap[String, Set[String]]
    map.put("TPCDSSuite", Set("*"))
    map
  }

  override protected var dataSources: mutable.LinkedHashMap[
    String,
    Map[String, DataFrame]] =
    new mutable.LinkedHashMap[String, Map[String, DataFrame]]

  private def registerDF(tableName: String): Unit = {

    val sf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName.toUpperCase)
      .load()

    sf.createOrReplaceTempView(tableName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    tables.foreach(registerDF)
  }

  test("TPCDS-Q03") {
    val ss = sparkSession.sql("select * from TEST_TABLE_5984932164318553871")
    ss.show
    ss.write.format(SNOWFLAKE_SOURCE_NAME).options(connectorOptionsNoTable).option("dbtable", "testOut2").mode(SaveMode.Overwrite).save
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
