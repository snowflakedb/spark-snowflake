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

import java.sql.{Date, Timestamp}
import java.util.TimeZone

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.TestHook
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

import scala.reflect.internal.util.TableDef

// scalastyle:off println
class PushdownEnhancement02 extends IntegrationSuiteBase {
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()
  private val test_table_basic: String = s"test_basic_$randomSuffix"
  private val test_table_rank = s"test_table_rank_$randomSuffix"

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_basic")
      jdbcUpdate(s"drop table if exists $test_table_rank")
    } finally {
      TestHook.disableTestHook()
      super.afterAll()
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
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
  }

  test("test pushdown basic: OR, BIT operation") {
    jdbcUpdate(s"create or replace table $test_table_basic(name String, value1 Integer, value2 Integer)")
    jdbcUpdate(s"insert into $test_table_basic values ('Ray', 1, 9), ('Ray', 2, 8), ('Ray', 3, 7), ('Emily', 4, 6), ('Emily', 5, 5)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()
      // Spark doesn't support these bit operation on Decimal, so we convert them.
      .withColumn("value1", col("value1").cast(IntegerType))
      .withColumn("value2", col("value2").cast(IntegerType))

    tmpDF.printSchema()
    tmpDF.createOrReplaceTempView("test_table_basic")

    val resultDF =
      sparkSession
        .sql(s"select name, value1 as v1, value2 as v2," +
          s"(value1 & value2) as bitand, (value1 | value2) as bitor," +
          s"(value1 ^ value2) as xor , ( ~value1 ) as bitnot" +
          " from test_table_basic where name = 'Ray' or name = 'Emily'")

    resultDF.show(10, false)

    val expectedResult = Seq(
      Row("Ray"  , 1, 9, 1,  9, 8, -2),
      Row("Ray"  , 2, 8, 0, 10, 10, -3),
      Row("Ray"  , 3, 7, 3,  7, 4, -4),
      Row("Emily", 4, 6, 4,  6, 2, -5),
      Row("Emily", 5, 5, 5,  5, 0, -6)
    )

    testPushdown(
      s"""SELECT ( "SUBQUERY_1"."NAME" ) AS "SUBQUERY_2_COL_0" ,
         |( CAST ( "SUBQUERY_1"."VALUE1" AS NUMBER ) ) AS "SUBQUERY_2_COL_1" ,
         |( CAST ( "SUBQUERY_1"."VALUE2" AS NUMBER ) ) AS "SUBQUERY_2_COL_2" ,
         |( BITAND ( CAST ( "SUBQUERY_1"."VALUE1" AS NUMBER ) ,
         |CAST ( "SUBQUERY_1"."VALUE2" AS NUMBER ) ) ) AS "SUBQUERY_2_COL_3" ,
         |( BITOR ( CAST ( "SUBQUERY_1"."VALUE1" AS NUMBER ) ,
         |CAST ( "SUBQUERY_1"."VALUE2" AS NUMBER ) ) ) AS "SUBQUERY_2_COL_4" ,
         |( BITXOR ( CAST ( "SUBQUERY_1"."VALUE1" AS NUMBER ) ,
         |CAST ( "SUBQUERY_1"."VALUE2" AS NUMBER ) ) ) AS "SUBQUERY_2_COL_5" ,
         |( BITNOT ( CAST ( "SUBQUERY_1"."VALUE1" AS NUMBER ) ) )
         |AS "SUBQUERY_2_COL_6" FROM ( SELECT * FROM (
         |SELECT * FROM ( $test_table_basic ) AS "SF_CONNECTOR_QUERY_ALIAS" )
         |AS "SUBQUERY_0" WHERE ( ( "SUBQUERY_0"."NAME" = 'Ray' ) OR
         |( "SUBQUERY_0"."NAME" = 'Emily' ) ) ) AS "SUBQUERY_1"
         |""".stripMargin,
      resultDF,
      expectedResult
    )
  }

  test("test pushdown boolean functions: NOT/Contains/EndsWith/StartsWith") {
    jdbcUpdate(s"create or replace table $test_table_basic(name String, value1 Integer, value2 Integer)")
    jdbcUpdate(s"insert into $test_table_basic values ('Ray', 1, 9), ('Ray', 2, 8), ('Ray', 3, 7), ('Emily', 4, 6), ('Emily', 5, 5)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_basic)
      .load()

    tmpDF.printSchema()
    tmpDF.createOrReplaceTempView("test_table_basic")

    val resultDF =
      sparkSession
        .sql(s"select * " +
          " from test_table_basic where name != 'Ray'" +
          " OR (name like '%ay')" +
          " OR (name like 'Emi%')" +
          " OR (name like '%i%')" +
          " OR (not (value1 >= 5))" +
          " OR (not (value1 <= 6))" +
          " OR (not (value2 >  7))" +
          " OR (not (value2 <  8))" )

    resultDF.show(10, false)

    val expectedResult = Seq(
      Row("Ray"  , 1, 9),
      Row("Ray"  , 2, 8),
      Row("Ray"  , 3, 7),
      Row("Emily", 4, 6),
      Row("Emily", 5, 5)
    )

    testPushdown(
      s"""SELECT * FROM ( SELECT * FROM ( $test_table_basic )
         | AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         | WHERE ( ( ( (
         |    "SUBQUERY_0"."NAME" != 'Ray' )
         | OR "SUBQUERY_0"."NAME" LIKE '%ay' )
         | OR ( "SUBQUERY_0"."NAME" LIKE 'Emi%'
         | OR "SUBQUERY_0"."NAME" LIKE '%i%' ) )
         | OR ( ( ( "SUBQUERY_0"."VALUE1" < 5 )
         | OR ( "SUBQUERY_0"."VALUE1" > 6 ) )
         | OR ( ( "SUBQUERY_0"."VALUE2" <= 7 )
         | OR ( "SUBQUERY_0"."VALUE2" >= 8 ) ) ) )
         |""".stripMargin,
      resultDF,
      expectedResult, false, true
    )
  }

  test("test pushdown WindowExpression: Rank without PARTITION BY") {
    // There is bug to execute rank()/dense_rank() with COPY UNLOAD: SNOW-177604
    if (!params.useCopyUnload) {
      jdbcUpdate(s"create or replace table $test_table_rank" +
        s"(state String, bushels_produced Integer)")
      jdbcUpdate(s"insert into $test_table_rank values" +
        s"('Iowa', 130), ('Iowa', 120), ('Iowa', 120)," +
        s"('Kansas', 100), ('Kansas', 100), ('Kansas', 90)")

      val tmpDF = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_rank)
        .load()

      tmpDF.printSchema()
      tmpDF.createOrReplaceTempView("test_table_rank")

      val resultDF =
        sparkSession
          .sql(s"select state, bushels_produced," +
            " rank() over (order by bushels_produced desc) as total_rank" +
            " from test_table_rank")

      resultDF.show(10, false)

      val expectedResult = Seq(
        Row("Iowa", 130, 1),
        Row("Iowa", 120, 2),
        Row("Iowa", 120, 2),
        Row("Kansas", 100, 4),
        Row("Kansas", 100, 4),
        Row("Kansas", 90, 6)
      )

      testPushdown(
        s"""SELECT ( "SUBQUERY_0"."STATE" ) AS "SUBQUERY_1_COL_0" ,
           |( "SUBQUERY_0"."BUSHELS_PRODUCED" ) AS "SUBQUERY_1_COL_1" ,
           |( RANK ()  OVER ( ORDER BY ( "SUBQUERY_0"."BUSHELS_PRODUCED" ) DESC
           |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) )
           |    AS "SUBQUERY_1_COL_2"
           |FROM ( SELECT * FROM ( $test_table_rank )
           |  AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           |
           |""".stripMargin,
        resultDF,
        expectedResult
      )
    }
  }

  test("test pushdown WindowExpression: Rank with PARTITION BY") {
    // There is bug to execute rank()/dense_rank() with COPY UNLOAD: SNOW-177604
    if (!params.useCopyUnload) {
      jdbcUpdate(s"create or replace table $test_table_rank" +
        s"(state String, bushels_produced Integer)")
      jdbcUpdate(s"insert into $test_table_rank values" +
        s"('Iowa', 130), ('Iowa', 120), ('Iowa', 120)," +
        s"('Kansas', 100), ('Kansas', 100), ('Kansas', 90)")

      val tmpDF = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_rank)
        .load()

      tmpDF.printSchema()
      tmpDF.createOrReplaceTempView("test_table_rank")

      val resultDF =
        sparkSession
          .sql(s"select state, bushels_produced," +
            " rank() over (partition by state " +
            "   order by bushels_produced desc) as group_rank" +
            " from test_table_rank")

      resultDF.show(10, false)

      val expectedResult = Seq(
        Row("Iowa", 130, 1),
        Row("Iowa", 120, 2),
        Row("Iowa", 120, 2),
        Row("Kansas", 100, 1),
        Row("Kansas", 100, 1),
        Row("Kansas", 90, 3)
      )
      testPushdown(
        s"""SELECT ( "SUBQUERY_0"."STATE" ) AS "SUBQUERY_1_COL_0" ,
           |( "SUBQUERY_0"."BUSHELS_PRODUCED" ) AS "SUBQUERY_1_COL_1" ,
           |( RANK ()  OVER ( PARTITION BY "SUBQUERY_0"."STATE"
           |  ORDER BY ( "SUBQUERY_0"."BUSHELS_PRODUCED" ) DESC
           |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) )
           |    AS "SUBQUERY_1_COL_2"
           |FROM ( SELECT * FROM ( $test_table_rank )
           |  AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           |
           |""".stripMargin,
        resultDF,
        expectedResult
      )
    }
  }

  test("test pushdown WindowExpression: DenseRank without PARTITION BY") {
    // There is bug to execute rank()/dense_rank() with COPY UNLOAD: SNOW-177604
    if (!params.useCopyUnload) {
      jdbcUpdate(s"create or replace table $test_table_rank" +
        s"(state String, bushels_produced Integer)")
      jdbcUpdate(s"insert into $test_table_rank values" +
        s"('Iowa', 130), ('Iowa', 120), ('Iowa', 120)," +
        s"('Kansas', 100), ('Kansas', 100), ('Kansas', 90)")

      val tmpDF = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_rank)
        .load()

      tmpDF.printSchema()
      tmpDF.createOrReplaceTempView("test_table_rank")

      val resultDF =
        sparkSession
          .sql(s"select state, bushels_produced," +
            " dense_rank() over (order by bushels_produced desc) as total_rank" +
            " from test_table_rank")

      resultDF.show(10, false)

      val expectedResult = Seq(
        Row("Iowa", 130, 1),
        Row("Iowa", 120, 2),
        Row("Iowa", 120, 2),
        Row("Kansas", 100, 3),
        Row("Kansas", 100, 3),
        Row("Kansas", 90, 4)
      )

      testPushdown(
        s"""SELECT ( "SUBQUERY_0"."STATE" ) AS "SUBQUERY_1_COL_0" ,
           |( "SUBQUERY_0"."BUSHELS_PRODUCED" ) AS "SUBQUERY_1_COL_1" ,
           |( DENSE_RANK ()  OVER ( ORDER BY ( "SUBQUERY_0"."BUSHELS_PRODUCED" ) DESC
           |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) )
           |    AS "SUBQUERY_1_COL_2"
           |FROM ( SELECT * FROM ( $test_table_rank )
           |  AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           |
           |""".stripMargin,
        resultDF,
        expectedResult
      )
    }
  }

  test("test pushdown WindowExpression: DenseRank with PARTITION BY") {
    // There is bug to execute rank()/dense_rank() with COPY UNLOAD: SNOW-177604
    if (!params.useCopyUnload) {
      jdbcUpdate(s"create or replace table $test_table_rank" +
        s"(state String, bushels_produced Integer)")
      jdbcUpdate(s"insert into $test_table_rank values" +
        s"('Iowa', 130), ('Iowa', 120), ('Iowa', 120)," +
        s"('Kansas', 100), ('Kansas', 100), ('Kansas', 90)")

      val tmpDF = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_rank)
        .load()

      tmpDF.printSchema()
      tmpDF.createOrReplaceTempView("test_table_rank")

      val resultDF =
        sparkSession
          .sql(s"select state, bushels_produced," +
            " dense_rank() over (partition by state " +
            "   order by bushels_produced desc) as group_rank" +
            " from test_table_rank")

      resultDF.show(10, false)

      val expectedResult = Seq(
        Row("Iowa", 130, 1),
        Row("Iowa", 120, 2),
        Row("Iowa", 120, 2),
        Row("Kansas", 100, 1),
        Row("Kansas", 100, 1),
        Row("Kansas", 90, 2)
      )
      testPushdown(
        s"""SELECT ( "SUBQUERY_0"."STATE" ) AS "SUBQUERY_1_COL_0" ,
           |( "SUBQUERY_0"."BUSHELS_PRODUCED" ) AS "SUBQUERY_1_COL_1" ,
           |( DENSE_RANK ()  OVER ( PARTITION BY "SUBQUERY_0"."STATE"
           |  ORDER BY ( "SUBQUERY_0"."BUSHELS_PRODUCED" ) DESC
           |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) )
           |    AS "SUBQUERY_1_COL_2"
           |FROM ( SELECT * FROM ( $test_table_rank )
           |  AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           |
           |""".stripMargin,
        resultDF,
        expectedResult
      )
    }
  }

  /*
   * PercentRank can't be pushdown to snowflake, because Snowflake's percent_rank only
   * support window frame type: RANGE. But, PercentRank in Spark only supports window
   * frame type: ROWS. If a window frame is not specified, a default ROWS window frame
   * is added by spark.
   * https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html
   *
   * Doc on Snowflake:
   * PERCENT_RANK supports range-based cumulative window frames,
   * but not other types of window frames.
   * https://docs.snowflake.com/en/sql-reference/functions/percent_rank.html
   */
  ignore("test pushdown WindowExpression: PercentRank without PARTITION BY") {
    // There is bug to execute rank()/dense_rank() with COPY UNLOAD: SNOW-177604
    if (!params.useCopyUnload) {
      jdbcUpdate(s"create or replace table $test_table_rank" +
        s"(state String, bushels_produced Integer)")
      jdbcUpdate(s"insert into $test_table_rank values" +
        s"('Iowa', 130), ('Iowa', 120), ('Iowa', 120)," +
        s"('Kansas', 100), ('Kansas', 100), ('Kansas', 90)")

      val tmpDF = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(thisConnectorOptionsNoTable)
        .option("dbtable", test_table_rank)
        .load()

      tmpDF.printSchema()
      tmpDF.createOrReplaceTempView("test_table_rank")

    /*
     * With useRangeWindow = true, spark raises below exception.
     *   Window Frame specifiedwindowframe(RangeFrame, unboundedpreceding$(), currentrow$())
     *   must match the required frame
     *   specifiedwindowframe(RowFrame, unboundedpreceding$(), currentrow$())
     *
     * With useRangeWindow = false, snowflake raises below exception.
     *   SQL compilation error: error line 1 at position 442
     *   Cumulative window frame unsupported for function PERCENT_RANK
     */
      val useRangeWindow = false
      val resultDF = if (useRangeWindow) {
        sparkSession
          .sql(s"select state, bushels_produced," +
            " percent_rank() over" +
            "  ( order by bushels_produced desc" +
            "    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)" +
            "  as total_rank" +
            " from test_table_rank")
      } else {
        sparkSession
          .sql(s"select state, bushels_produced," +
            " percent_rank() over" +
            "  ( order by bushels_produced desc)" +
            "  as total_rank" +
            " from test_table_rank")
      }
      resultDF.show(10, false)

      val expectedResult = Seq(
        Row("Iowa", 130, 0.0),
        Row("Iowa", 120, 0.2),
        Row("Iowa", 120, 0.2),
        Row("Kansas", 100, 0.6),
        Row("Kansas", 100, 0.6),
        Row("Kansas", 90, 1.0)
      )

      testPushdown(
        s"""SELECT ( "SUBQUERY_0"."STATE" ) AS "SUBQUERY_1_COL_0" ,
           |( "SUBQUERY_0"."BUSHELS_PRODUCED" ) AS "SUBQUERY_1_COL_1" ,
           |( PERCENT__RANK ()  OVER ( ORDER BY ( "SUBQUERY_0"."BUSHELS_PRODUCED" ) DESC
           |  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) )
           |    AS "SUBQUERY_1_COL_2"
           |FROM ( SELECT * FROM ( $test_table_rank )
           |  AS "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
           |
           |""".stripMargin,
        resultDF,
        expectedResult, false, true
      )
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }
}
// scalastyle:on println
