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

import net.snowflake.spark.snowflake.{Conversions, SnowflakeInputFormat}
import net.snowflake.spark.snowflake.Utils._
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import scala.collection.mutable

class TCPHSuite extends PerformanceSuite {

  override var requiredParams = {
    val map = new mutable.LinkedHashMap[String, String]
    map.put("dummyParam", "-1")
    map
  }
  override var acceptedArguments = {
    val map = new mutable.LinkedHashMap[String, Set[String]]
    map.put("dummyParam", Set("2", "haha"))
    map
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    /*
    val df1 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", "LINEITEM")
      .option("sfSchema", "TESTSCHEMA")
      .load()

    df1.createOrReplaceTempView("df1")
    */
  }

  /*
  test("Conversion performance") {

    val resultSchema = new StructType(
      Array(StructField("C1", DecimalType(38,0), nullable = true),
        StructField("C2", DecimalType(38,0), nullable = true),
        StructField("C3", DecimalType(38,0), nullable = true)))

    val rdd = sqlContext.sparkContext.newAPIHadoopFile(
      "s3n://sfc-dev1/edm/55dc539e-2b65-4b51-b48c-ced1396ae196/",
      classOf[SnowflakeInputFormat],
      classOf[java.lang.Long],
      classOf[Array[String]])
     val r = rdd.values.mapPartitions { iter =>
      val converter: Array[String] => InternalRow = Conversions.createRowConverter[InternalRow](resultSchema)
      iter.map(converter)
    }

    val t1 = System.nanoTime()
    r.collect()
    println((System.nanoTime() - t1) / 1e9d)
  }

  test("First test") {
    testQuery("SELECT C1, C2, C3 FROM df1", "first test")
  }
*/

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    try {
    } finally {
      super.afterAll()
    }
  }

}
