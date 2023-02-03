/*
 * Copyright 2015-2020 Snowflake Computing
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

package net.snowflake.spark.snowflake


import net.snowflake.spark.snowflake.test.{TestHook, TestHookFlag}
import net.snowflake.spark.snowflake._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{Filter, GreaterThan, LessThan}
import org.apache.spark.sql.types._

import scala.util.Random

// scalastyle:off println
class SnowflakeRelationSuite extends IntegrationSuiteBase {
  private val test_table_write: String = s"test_table_write_$randomSuffix"
  private val internal_stage_name = s"test_stage_$randomSuffix"

  private val largeStringValue = Random.alphanumeric take 1024 mkString ""
  private val test_table_large_result: String =
    s"test_table_large_result_$randomSuffix"
  private val LARGE_TABLE_ROW_COUNT = 1000

  private def setupLargeResultTable(sfOptions: Map[String, String]): Unit = {
    val param = Parameters.MergedParameters(sfOptions)
    val connection = TestUtils.getServerConnection(param)

    connection.createStatement.executeQuery(
      s"""create or replace table $test_table_large_result (
         | int_c int, c_string string(1024) )""".stripMargin
    )

    connection.createStatement.executeQuery(
      s"""insert into $test_table_large_result select
      | seq4(), '$largeStringValue'
      | from table(generator(rowcount => $LARGE_TABLE_ROW_COUNT))
      | """.stripMargin
    )

    connection.close()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    setupLargeResultTable(connectorOptionsNoTable)
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop stage if exists $internal_stage_name")

      jdbcUpdate(s"drop table if exists $test_table_large_result")
      jdbcUpdate(s"drop table if exists $test_table_write")
    } finally {
      TestHook.disableTestHook()
      super.afterAll()
    }
  }

  test("test SnowflakeRelation.'lazy schema'") {
    val sfOptions: Map[String, String] =
      replaceOption(
        connectorOptionsNoTable,
        "query",
        s"select * from $test_table_large_result"
      )

    val param = Parameters.mergeParameters(sfOptions)
    val sqlContext = new SQLContext(sc)
    val jdbcWrapper = new JDBCWrapper()
    val snowflakeRelation = new SnowflakeRelation(jdbcWrapper, param, None)(sqlContext)
    // Test lazy schema
    val schema = snowflakeRelation.schema
    assert(schema.size == 2)
  }

  private def createTestDataFrame() : (DataFrame, StructType, Long) = {
    val thisSparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .config("spark.master", "local")
      .getOrCreate()

    def getRandomString(len: Int): String = {
      Random.alphanumeric take len mkString ""
    }

    val partitionCount = 2
    val rowCountPerPartition = 100
    val strValue = getRandomString(512)
    // Create RDD which generates 1 large partition
    val testRDD: RDD[Row] = thisSparkSession.sparkContext
      .parallelize(Seq[Int](), partitionCount)
      .mapPartitions { _ => {
        (1 to rowCountPerPartition).map { x => {
          Row(x, strValue)
        }
        }.iterator
      }
      }
    val schema = StructType(
      List(
        StructField("int_c", IntegerType),
        StructField("string_c", StringType)
      )
    )

    // Convert RDD to DataFrame
    val df = thisSparkSession.createDataFrame(testRDD, schema)
    (df, schema, rowCountPerPartition * partitionCount)
  }

  private def getRowCount(tableName: String): Long = {
    val rs = Utils.runQuery(connectorOptionsNoTable,
      s"select count(*) from $tableName")
    rs.next()
    rs.getLong(1)
  }

  test("test SnowflakeRelation.insert()") {
    val sfOptionsNoTable: Map[String, String] =
      replaceOption(connectorOptionsNoTable, "dbtable", test_table_write)

    val param = Parameters.mergeParameters(sfOptionsNoTable)
    val sqlContext = new SQLContext(sc)
    val jdbcWrapper = new JDBCWrapper()
    val snowflakeRelation = new SnowflakeRelation(jdbcWrapper, param, None)(sqlContext)
    val (df, _, rowCount) = createTestDataFrame()

    // First insert() with overwrite mode
    snowflakeRelation.insert(df, overwrite = true)
    assert(getRowCount(test_table_write) == rowCount)

    // Second insert() with overwrite mode
    snowflakeRelation.insert(df, overwrite = true)
    assert(getRowCount(test_table_write) == rowCount)

    // Third insert() with append mode
    snowflakeRelation.insert(df, overwrite = false)
    assert(getRowCount(test_table_write) == rowCount * 2)
  }

  test("test SnowflakeRelation.buildScanFromSQL() with Schema = None") {
    val sfOptionsNoTable: Map[String, String] =
      replaceOption(connectorOptionsNoTable, "dbtable", test_table_large_result)

    val param = Parameters.mergeParameters(sfOptionsNoTable)
    val sqlContext = new SQLContext(sc)
    val jdbcWrapper = new JDBCWrapper()
    val snowflakeRelation = new SnowflakeRelation(jdbcWrapper, param, None)(sqlContext)
    val snowflakeSQLStatement = new SnowflakeSQLStatement() +
      s"(select * from $test_table_large_result)"
    val resultRDD = snowflakeRelation.buildScanFromSQL(snowflakeSQLStatement, None)
    println(resultRDD.id)
  }

  test("test SnowflakeRelation.buildScan() with requiredColumns = Empty") {
    val sfOptionsNoTable: Map[String, String] =
      replaceOption(connectorOptionsNoTable, "dbtable", test_table_large_result)

    val param = Parameters.mergeParameters(sfOptionsNoTable)
    val sqlContext = new SQLContext(sc)
    val jdbcWrapper = new JDBCWrapper()
    val snowflakeRelation = new SnowflakeRelation(jdbcWrapper, param, None)(sqlContext)
    val filters: Array[Filter] = Array(GreaterThan("INT_C", 10), LessThan("INT_C", 20))
    val requiredColumns: Array[String] = Array.empty
    val resultRDD = snowflakeRelation.buildScan(requiredColumns, filters)
    println(resultRDD.count() == (20 - 10 - 1))
  }

}
// scalastyle:on println
