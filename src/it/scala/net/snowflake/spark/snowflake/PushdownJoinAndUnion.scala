/*
 * Copyright 2015-2019 Snowflake Computing
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

import java.sql._
import java.util.TimeZone
import net.snowflake.spark.snowflake.Utils.{SNOWFLAKE_SOURCE_NAME, SNOWFLAKE_SOURCE_SHORT_NAME}
import net.snowflake.spark.snowflake.test.TestHook
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, MapType, StringType, StructField, StructType}

import scala.reflect.internal.util.TableDef

// scalastyle:off println
class PushdownJoinAndUnion extends IntegrationSuiteBase {
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()
  private val test_left_table = s"test_table_left_$randomSuffix"
  private val test_right_table = s"test_table_right_$randomSuffix"
  private val test_left_table_2 = s"test_table_left_2_$randomSuffix"
  private val test_right_table_2 = s"test_table_right_2_$randomSuffix"
  private val test_temp_schema = s"test_temp_schema_$randomSuffix"

  lazy val localDataFrame = {
    val partitionCount = 1
    val rowCountPerPartition = 1
    // Create RDD which generates data with multiple partitions
    val testRDD: RDD[Row] = sparkSession.sparkContext
      .parallelize(Seq[Int](), partitionCount)
      .mapPartitions { _ => {
        (1 to rowCountPerPartition).map { i => {
          Row(i, "local_value")
        }
        }.iterator
      }
      }

    val schema = StructType(List(
      StructField("key", IntegerType),
      StructField("local_value", StringType)
    ))

    // Convert RDD to DataFrame
    sparkSession.createDataFrame(testRDD, schema)
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_left_table")
      jdbcUpdate(s"drop table if exists $test_right_table")
      jdbcUpdate(s"drop table if exists $test_left_table_2")
      jdbcUpdate(s"drop table if exists $test_right_table_2")
      jdbcUpdate(s"drop schema if exists $test_temp_schema")
    } finally {
      TestHook.disableTestHook()
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
      super.afterAll()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // There is bug for Date.equals() to compare Date with different timezone,
    // so set up the timezone to work around it.
    val gmtTimezone = TimeZone.getTimeZone("GMT")
    TimeZone.setDefault(gmtTimezone)

    connectorOptionsNoTable.foreach(tup => {
      thisConnectorOptionsNoTable += tup
    })

    // Create test tables in sfSchema
    jdbcUpdate(s"create or replace table $test_left_table (key int, left_value string)")
    jdbcUpdate(s"insert into $test_left_table values (1, 'left_in_current_schema')")
    jdbcUpdate(s"create or replace table $test_left_table_2 (key int, left_value string)")
    jdbcUpdate(s"insert into $test_left_table_2 values (1, 'left_in_current_schema')")

    jdbcUpdate(s"create or replace table $test_right_table (key int, right_value string)")
    jdbcUpdate(s"insert into $test_right_table values (1, 'right_in_current_schema')")
    jdbcUpdate(s"create or replace table $test_right_table_2 (key int, right_value string)")
    jdbcUpdate(s"insert into $test_right_table_2 values (1, 'right_in_current_schema')")

    // Create test schema for crossing schema join/union test
    jdbcUpdate(s"create or replace schema $test_temp_schema")

    jdbcUpdate(s"create or replace table $test_temp_schema.$test_right_table" +
      s" (key int, right_value string)")
    jdbcUpdate(s"insert into $test_temp_schema.$test_right_table" +
      s" values (1, 'right_in_another_schema')")
    jdbcUpdate(s"create or replace table $test_temp_schema.$test_right_table_2" +
      s" (key int, right_value string)")
    jdbcUpdate(s"insert into $test_temp_schema.$test_right_table_2" +
      s" values (1, 'right_in_another_schema')")

    jdbcUpdate(s"create or replace table $test_temp_schema.$test_left_table" +
      s" (key int, right_value string)")
    jdbcUpdate(s"insert into $test_temp_schema.$test_left_table" +
      s" values (1, 'left_in_another_schema')")
    jdbcUpdate(s"create or replace table $test_temp_schema.$test_left_table_2" +
      s" (key int, right_value string)")
    jdbcUpdate(s"insert into $test_temp_schema.$test_left_table_2" +
      s" values (1, 'left_in_another_schema')")
  }

  private lazy val sfOptionsWithAnotherSchema =
    connectorOptionsNoTable ++ Map(Parameters.PARAM_SF_SCHEMA -> test_temp_schema)

  test("same schema JOIN is pushdown") {
    // Left DataFrame reads test_left_table in current schema
    val dfLeft = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_left_table)
      .load()
      .select("*")

    // Right DataFrame reads test_right_table in another schema (not current schema)
    val dfRight = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_right_table)
      .load()
      .select("*")

    val dfJoin = dfLeft.join(dfRight, "key")

    val expectedResult = Seq(Row(1, "left_in_current_schema", "right_in_current_schema"))

    testPushdown(
      s"""SELECT ( "SUBQUERY_4"."SUBQUERY_4_COL_0" ) AS "SUBQUERY_5_COL_0" ,
         |       ( "SUBQUERY_4"."SUBQUERY_4_COL_1" ) AS "SUBQUERY_5_COL_1" ,
         |       ( "SUBQUERY_4"."SUBQUERY_4_COL_3" ) AS "SUBQUERY_5_COL_2"
         | FROM (
         |   SELECT ( "SUBQUERY_1"."KEY" ) AS "SUBQUERY_4_COL_0" ,
         |          ( "SUBQUERY_1"."LEFT_VALUE" ) AS "SUBQUERY_4_COL_1" ,
         |          ( "SUBQUERY_3"."KEY" ) AS "SUBQUERY_4_COL_2" ,
         |          ( "SUBQUERY_3"."RIGHT_VALUE" ) AS "SUBQUERY_4_COL_3"
         |       FROM (
         |          SELECT * FROM ( SELECT * FROM ( $test_left_table )
         |              AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |              WHERE ( "SUBQUERY_0"."KEY" IS NOT NULL ) ) AS "SUBQUERY_1"
         |          INNER JOIN (
         |          SELECT * FROM ( SELECT * FROM ( $test_right_table )
         |              AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_2"
         |              WHERE ( "SUBQUERY_2"."KEY" IS NOT NULL ) ) AS "SUBQUERY_3"
         |          ON ( "SUBQUERY_1"."KEY" = "SUBQUERY_3"."KEY" ) ) AS "SUBQUERY_4"
         |""".stripMargin,
      dfJoin,
      expectedResult
    )
  }

  test("same schema UNION is pushdown") {
    // Left DataFrame reads test_left_table in current schema
    val dfLeft = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_left_table)
      .load()
      .select("*")

    // Right DataFrame reads test_right_table in another schema (not current schema)
    val dfRight = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_right_table)
      .load()
      .select("*")

    // union of 2 DataFrame
    testPushdown(
      s"""( SELECT * FROM ( $test_left_table ) AS "SF_CONNECTOR_QUERY_ALIAS" )
         | UNION ALL
         |( SELECT * FROM ( $test_right_table ) AS "SF_CONNECTOR_QUERY_ALIAS" )
         |""".stripMargin,
      dfLeft.union(dfRight),
      Seq(Row(1, "left_in_current_schema"), Row(1, "right_in_current_schema"))
    )

    // union of 3 DataFrame
    testPushdown(
      s"""( SELECT * FROM ( $test_left_table ) AS "SF_CONNECTOR_QUERY_ALIAS" )
         | UNION ALL
         |( SELECT * FROM ( $test_right_table ) AS "SF_CONNECTOR_QUERY_ALIAS" )
         | UNION ALL
         |( SELECT * FROM ( $test_right_table ) AS "SF_CONNECTOR_QUERY_ALIAS" )
         |""".stripMargin,
      dfLeft.union(dfRight).union(dfRight),
      Seq(Row(1, "left_in_current_schema"),
        Row(1, "right_in_current_schema"),
        Row(1, "right_in_current_schema"))
    )
  }

  test("self UNION is pushdown") {
    // Left DataFrame reads test_left_table in current schema
    val dfLeft = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_left_table)
      .load()
      .select("*")

    // union of 2 DataFrame
    testPushdown(
      s"""( SELECT * FROM ( $test_left_table ) AS "SF_CONNECTOR_QUERY_ALIAS" )
         | UNION ALL
         |( SELECT * FROM ( $test_left_table ) AS "SF_CONNECTOR_QUERY_ALIAS" )
         |""".stripMargin,
      dfLeft.union(dfLeft),
      Seq(Row(1, "left_in_current_schema"), Row(1, "left_in_current_schema"))
    )

    // union of 3 DataFrame
    testPushdown(
      s"""( SELECT * FROM ( $test_left_table ) AS "SF_CONNECTOR_QUERY_ALIAS" )
         | UNION ALL
         |( SELECT * FROM ( $test_left_table ) AS "SF_CONNECTOR_QUERY_ALIAS" )
         | UNION ALL
         |( SELECT * FROM ( $test_left_table ) AS "SF_CONNECTOR_QUERY_ALIAS" )
         |""".stripMargin,
      dfLeft.union(dfLeft).union(dfLeft),
      Seq(Row(1, "left_in_current_schema"),
        Row(1, "left_in_current_schema"),
        Row(1, "left_in_current_schema"))
    )
  }

  test("test snowflake DataFrame JOIN with non-snowflake DataFrame - not pushdown") {
    // Left DataFrame reads test_left_table in current schema
    val dfSnowflake = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_left_table)
      .load()
      .select("*")

    // Snowflake DataFrame join local DataFrame
    testPushdown(
      s"don't_check_query",
      dfSnowflake.join(localDataFrame, "key"),
      Seq(Row(1, "left_in_current_schema", "local_value")),
      bypass = true
    )

    // Snowflake DataFrame join local DataFrame
    testPushdown(
      s"don't_check_query",
      localDataFrame.join(dfSnowflake, "key"),
      Seq(Row(1, "local_value", "left_in_current_schema")),
      bypass = true
    )
  }

  test("test snowflake DataFrame UNION with non-snowflake DataFrame - not pushdown") {
    // Left DataFrame reads test_left_table in current schema
    val dfSnowflake = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_left_table)
      .load()
      .select("*")

    dfSnowflake.union(localDataFrame).show(truncate = false)
    // Snowflake DataFrame union local DataFrame
    testPushdown(
      s"don't_check_query",
      dfSnowflake.union(localDataFrame),
      Seq(Row(1, "left_in_current_schema"), Row(1, "local_value")),
      bypass = true
    )

    // Snowflake DataFrame union local DataFrame
    testPushdown(
      s"don't_check_query",
      localDataFrame.union(dfSnowflake),
      Seq(Row(1, "left_in_current_schema"), Row(1, "local_value")),
      bypass = true
    )
  }

  test("test crossing schema join: SNOW-770051") {
    // Left DataFrame reads test_left_table in current schema
    val dfLeft = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_left_table)
      .load()
      .select("*")

    // Right DataFrame reads test_right_table in another schema (not current schema)
    val dfRight = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsWithAnotherSchema)
      .option("dbtable", s"$test_right_table")
      .load()
      .select("*")

    val dfJoin = dfLeft.join(dfRight, "key")

    val expectedResult = Seq(
      Row(1, "left_in_current_schema",
        "right_in_another_schema"))

    testPushdown(
      s"don't_check_query",
      dfJoin,
      expectedResult, bypass = true
    )
  }

  test("test crossing schema union") {
    // Left DataFrame reads test_left_table in current schema
    val dfLeft = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_left_table)
      .load()

    // Right DataFrame reads test_right_table in another schema (not current schema)
    val dfRight = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsWithAnotherSchema)
      .option("dbtable", s"$test_right_table")
      .load()

    val dfUnion = dfLeft.union(dfRight) // .union(dfRight).union(dfRight)

    val expectedResult = Seq(
      Row(1, "left_in_current_schema"),
      Row(1, "right_in_another_schema"))

    testPushdown(
      s"don't_check_query",
      dfUnion,
      expectedResult, bypass = true
    )
  }

  test("test crossing schema union for multiple DataFrames") {
    // Left DataFrame reads test_left_table in current schema
    val dfLeft = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_left_table)
      .load()

    // Right DataFrame reads test_right_table in another schema (not current schema)
    val dfRight = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsWithAnotherSchema)
      .option("dbtable", s"$test_right_table")
      .load()

    // df1 UNION df1 UNION df2
    testPushdown(
      s"don't_check_query",
      dfLeft.union(dfLeft).union(dfRight),
      Seq(
        Row(1, "left_in_current_schema"),
        Row(1, "left_in_current_schema"),
        Row(1, "right_in_another_schema")),
      bypass = true
    )

    // df1 UNION df2 UNION df2
    testPushdown(
      s"don't_check_query",
      dfLeft.union(dfRight).union(dfRight),
      Seq(
        Row(1, "left_in_current_schema"),
        Row(1, "right_in_another_schema"),
        Row(1, "right_in_another_schema")),
      bypass = true
    )

    // df1 UNION df1 UNION df2 UNION df2
    testPushdown(
      s"don't_check_query",
      dfLeft.union(dfLeft).union(dfRight).union(dfRight),
      Seq(
        Row(1, "left_in_current_schema"),
        Row(1, "left_in_current_schema"),
        Row(1, "right_in_another_schema"),
        Row(1, "right_in_another_schema")),
      bypass = true
    )

    // df1 UNION df2 UNION df1 UNION df2
    testPushdown(
      s"don't_check_query",
      dfLeft.union(dfRight).union(dfLeft).union(dfRight),
      Seq(
        Row(1, "left_in_current_schema"),
        Row(1, "right_in_another_schema"),
        Row(1, "left_in_current_schema"),
        Row(1, "right_in_another_schema")),
      bypass = true
    )
  }

  test("test crossing schema union + join") {
    // dfLeftA and dfRightA in current schema
    val dfLeftA = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_left_table)
      .load()
    val dfRightA = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_right_table)
      .load()

    // dfLeftB and dfRightB in another test schema
    val dfLeftB = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsWithAnotherSchema)
      .option("dbtable", test_left_table)
      .load()
    val dfRightB = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(sfOptionsWithAnotherSchema)
      .option("dbtable", test_right_table)
      .load()

    // (dfLeftA JOIN dfRightA) UNION (dfLeftB JOIN dfRightB)
    testPushdown(
      s"don't_check_query",
      (dfLeftA.join(dfRightA, "key")).union(dfLeftB.join(dfRightB, "key")),
      Seq(
        Row(1, "left_in_current_schema", "right_in_current_schema"),
        Row(1, "left_in_another_schema", "right_in_another_schema")),
      bypass = true
    )

    // (dfLeftA UNION dfRightA) JOIN (dfLeftB UNION dfRightB)
    testPushdown(
      s"don't_check_query",
      (dfLeftA.union(dfRightA)).join(dfLeftB.union(dfRightB), "key"),
      Seq(
        Row(1, "left_in_current_schema", "left_in_another_schema"),
        Row(1, "left_in_current_schema", "right_in_another_schema"),
        Row(1, "right_in_current_schema", "left_in_another_schema"),
        Row(1, "right_in_current_schema", "right_in_another_schema")),
      bypass = true
    )

    // (dfLeftA UNION dfRightB) JOIN (dfLeftB UNION dfRightA)
    testPushdown(
      s"don't_check_query",
      (dfLeftA.union(dfRightB)).join(dfLeftB.union(dfRightA), "key"),
      Seq(
        Row(1, "left_in_current_schema", "left_in_another_schema"),
        Row(1, "left_in_current_schema", "right_in_current_schema"),
        Row(1, "right_in_another_schema", "left_in_another_schema"),
        Row(1, "right_in_another_schema", "right_in_current_schema")),
      bypass = true
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }
}
// scalastyle:on println

