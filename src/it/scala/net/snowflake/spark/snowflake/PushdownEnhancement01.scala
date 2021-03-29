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

// scalastyle:off println
class PushdownEnhancement01 extends IntegrationSuiteBase {
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()
  private val test_table_length: String = s"test_length_$randomSuffix"
  private val test_table_ts_trunc: String = s"test_ts_trunc_$randomSuffix"
  private val test_table_date_trunc: String = s"test_date_trunc_$randomSuffix"
  private val test_table_decimal: String = s"test_decimal_$randomSuffix"
  private val test_table_union_1: String = s"test_union_1_$randomSuffix"
  private val test_table_union_2: String = s"test_union_2_$randomSuffix"
  private val test_table_case_when_1: String = s"test_case_when_1_$randomSuffix"
  private val test_table_case_when_2: String = s"test_case_when_2_$randomSuffix"
  private val test_table_left_semi_join_left: String = s"test_table_left_semi_join_left_$randomSuffix"
  private val test_table_left_semi_join_right: String = s"test_table_left_semi_join_right_$randomSuffix"
  private val test_table_shift_left: String = s"test_table_shift_left_$randomSuffix"
  private val test_table_shift_right: String = s"test_table_shift_right_$randomSuffix"
  private val test_table_in: String = s"test_table_in_$randomSuffix"
  private val test_table_in_set: String = s"test_table_in_set_$randomSuffix"
  private val test_table_cast: String = s"test_table_cast_$randomSuffix"
  private val test_table_coalesce = s"test_table_coalesce_$randomSuffix"

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_length")
      jdbcUpdate(s"drop table if exists $test_table_ts_trunc")
      jdbcUpdate(s"drop table if exists $test_table_date_trunc")
      jdbcUpdate(s"drop table if exists $test_table_decimal")
      jdbcUpdate(s"drop table if exists $test_table_union_1")
      jdbcUpdate(s"drop table if exists $test_table_union_2")
      jdbcUpdate(s"drop table if exists $test_table_case_when_1")
      jdbcUpdate(s"drop table if exists $test_table_case_when_2")
      jdbcUpdate(s"drop table if exists $test_table_left_semi_join_left")
      jdbcUpdate(s"drop table if exists $test_table_left_semi_join_right")
      jdbcUpdate(s"drop table if exists $test_table_shift_left")
      jdbcUpdate(s"drop table if exists $test_table_shift_right")
      jdbcUpdate(s"drop table if exists $test_table_in")
      jdbcUpdate(s"drop table if exists $test_table_in_set")
      jdbcUpdate(s"drop table if exists $test_table_cast")
      jdbcUpdate(s"drop table if exists $test_table_coalesce")
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

    // Set max_retry_count to cover the use case:
    // read data without caching data for use_copy_unload = true
    thisConnectorOptionsNoTable = replaceOption(
      connectorOptionsNoTable, "max_retry_count", "1")
  }

  test("test dropDuplicates") {
    import org.apache.spark.sql.functions._

    jdbcUpdate(s""" create or replace table nation
    (N_REGIONKEY String, N_NATIONKEY String, N_NAME String)""")
    val df_nation = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", "nation")
      .load()

    val df1 = df_nation.dropDuplicates("N_REGIONKEY")
    df1.explain()
    df1.collect()
  }

  test("test groupby with min") {
    import org.apache.spark.sql.functions._

    jdbcUpdate(s""" create or replace table nation
    (N_REGIONKEY String, N_NATIONKEY String, N_NAME String)""")
    val df_nation = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", "nation")
      .load()

    val df2 = df_nation.groupBy("N_REGIONKEY").agg(min("N_NATIONKEY"), min("N_NAME"))
    df2.explain()
    df2.collect()
  }

  test("test groupby with min + alias") {
    import org.apache.spark.sql.functions._

    jdbcUpdate(s""" create or replace table nation
    (N_REGIONKEY String, N_NATIONKEY String, N_NAME String)""")
    val df_nation = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", "nation")
      .load()

    val df2 = df_nation.groupBy(col("N_REGIONKEY").as("RUI_ALIAS")).agg(min("N_NATIONKEY"), min("N_NAME"))
    df2.explain()
    df2.collect()
  }

  test("test pushdown length() function") {
    jdbcUpdate(s"create or replace table $test_table_length(c1 char(10), c2 varchar(10), c3 string)")
    jdbcUpdate(s"insert into $test_table_length values ('', 'abc', null)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_length)
      .load()

    tmpDF.createOrReplaceTempView("test_table_length")

    val result = sparkSession.sql(
      "select length(c1), length(c2), length(c3) from test_table_length")
    val expectedResult = Seq(Row(0, 3, null))

    testPushdown(
      s""" SELECT ( LENGTH ( "SUBQUERY_0"."C1" ) ) AS "SUBQUERY_1_COL_0" ,
         |( LENGTH ( "SUBQUERY_0"."C2" ) ) AS "SUBQUERY_1_COL_1" ,
         |( LENGTH ( "SUBQUERY_0"."C3" ) ) AS "SUBQUERY_1_COL_2"
         |FROM ( SELECT * FROM ( $test_table_length ) AS
         |"SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" """.stripMargin,
      result,
      expectedResult,
      testPushdownOff = false
    )
  }

  test("test pushdown date_trunc function") {
    jdbcUpdate(s"create or replace table $test_table_ts_trunc(ts timestamp_ntz)")
    jdbcUpdate(s"insert into $test_table_ts_trunc values ('2019-08-21 12:34:56.789')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_ts_trunc)
      .load()

    tmpDF.createOrReplaceTempView("test_table_ts_trunc")

    //  ["YEAR", "YYYY", "YY", "MON", "MONTH", "MM", "DAY", "DD",
    //  "HOUR", "MINUTE", "SECOND", "WEEK", "QUARTER"]
    val result =
      sparkSession.sql(
        s"""select date_trunc('YEAR', ts), date_trunc('YYYY', ts),
           | date_trunc('YY', ts), date_trunc('MON', ts),
           | date_trunc('MONTH', ts), date_trunc('MM', ts),
           | date_trunc('DAY', ts), date_trunc('DD', ts),
           | date_trunc('HOUR', ts), date_trunc('MINUTE', ts),
           | date_trunc('WEEK', ts), date_trunc('QUARTER', ts)
           | from test_table_ts_trunc""".stripMargin)

    val expectedResult = Seq(Row(
      Timestamp.valueOf("2019-01-01 00:00:00.000"),
      Timestamp.valueOf("2019-01-01 00:00:00.000"),
      Timestamp.valueOf("2019-01-01 00:00:00.000"),
      Timestamp.valueOf("2019-08-01 00:00:00.000"),
      Timestamp.valueOf("2019-08-01 00:00:00.000"),
      Timestamp.valueOf("2019-08-01 00:00:00.000"),
      Timestamp.valueOf("2019-08-21 00:00:00.000"),
      Timestamp.valueOf("2019-08-21 00:00:00.000"),
      Timestamp.valueOf("2019-08-21 12:00:00.000"),
      Timestamp.valueOf("2019-08-21 12:34:00.000"),
      Timestamp.valueOf("2019-08-19 00:00:00.000"),
      Timestamp.valueOf("2019-07-01 00:00:00.000")
    ))

    result.printSchema()
    result.show()

    testPushdown(
      s"""select(date_trunc('year',"subquery_0"."ts"))as"subquery_1_col_0",
         |(date_trunc('yyyy',"subquery_0"."ts"))as"subquery_1_col_1",
         |(date_trunc('yy',"subquery_0"."ts"))as"subquery_1_col_2",
         |(date_trunc('mon',"subquery_0"."ts"))as"subquery_1_col_3",
         |(date_trunc('month',"subquery_0"."ts"))as"subquery_1_col_4",
         |(date_trunc('mm',"subquery_0"."ts"))as"subquery_1_col_5",
         |(date_trunc('day',"subquery_0"."ts"))as"subquery_1_col_6",
         |(date_trunc('dd',"subquery_0"."ts"))as"subquery_1_col_7",
         |(date_trunc('hour',"subquery_0"."ts"))as"subquery_1_col_8",
         |(date_trunc('minute',"subquery_0"."ts"))as"subquery_1_col_9",
         |(date_trunc('week',"subquery_0"."ts"))as"subquery_1_col_10",
         |(date_trunc('quarter',"subquery_0"."ts"))as"subquery_1_col_11"
         |from(select*from($test_table_ts_trunc )as
         |"sf_connector_query_alias")as"subquery_0"""".stripMargin,
      result,
      expectedResult
    )
  }

  test("test pushdown trunc() function") {
    jdbcUpdate(s"create or replace table $test_table_date_trunc(dt date)")
    jdbcUpdate(s"insert into $test_table_date_trunc values ('2019-08-21')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_date_trunc)
      .load()

    tmpDF.createOrReplaceTempView("test_table_date_trunc")

    //  ["year", "yyyy", "yy", "mon", "month", "mm"]
    val result =
    sparkSession.sql(
      s"""select trunc(dt, 'YEAR'), trunc(dt, 'YYYY'),
         | trunc(dt, 'YY'), trunc(dt, 'MON'),
         | trunc(dt, 'MONTH'), trunc(dt, 'MM')
         | from test_table_date_trunc""".stripMargin)

    val expectedResult = Seq(Row(
      Date.valueOf("2019-01-01"), Date.valueOf("2019-01-01"),
      Date.valueOf("2019-01-01"), Date.valueOf("2019-08-01"),
      Date.valueOf("2019-08-01"), Date.valueOf("2019-08-01")
    ))

    result.printSchema()
    result.show()

    testPushdown(
      s"""select(trunc("subquery_0"."dt",'year'))as"subquery_1_col_0",
         |(trunc("subquery_0"."dt",'yyyy'))as"subquery_1_col_1",
         |(trunc("subquery_0"."dt",'yy'))as"subquery_1_col_2",
         |(trunc("subquery_0"."dt",'mon'))as"subquery_1_col_3",
         |(trunc("subquery_0"."dt",'month'))as"subquery_1_col_4",
         |(trunc("subquery_0"."dt",'mm'))as"subquery_1_col_5"
         |from(select*from($test_table_date_trunc )as
         |"sf_connector_query_alias")as"subquery_0
         |"""".stripMargin,
      result,
      expectedResult
    )
  }

  test("test pushdown MakeDecimal() function") {
    jdbcUpdate(s"create or replace table $test_table_decimal(c1 string, c2 string)")
    jdbcUpdate(s"insert into $test_table_decimal values ('123.456', null)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_decimal)
      .load()

    tmpDF.createOrReplaceTempView("test_table_decimal")

    val result = sparkSession.sql(
      "SELECT sum(cast(c1 AS DECIMAL(5, 0))), sum(cast(c2 AS DECIMAL(5, 0))) FROM test_table_decimal")
    val expectedResult = Seq(Row(123, null))

    testPushdown(
      s"""select(to_decimal((sum((cast("subquery_0"."c1"asdecimal(5,0))
         |*pow(10,0)))/pow(10,0)),15,0))as"subquery_1_col_0",
         |(to_decimal((sum((cast("subquery_0"."c2"asdecimal(5,0))
         |*pow(10,0)))/pow(10,0)),15,0))as"subquery_1_col_1"
         |FROM ( SELECT * FROM ( $test_table_decimal ) AS
         |"SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0" limit 1""".stripMargin,
      result,
      expectedResult
    )
  }

  test("test pushdown union() and unionAll() function") {
    jdbcUpdate(s"create or replace table $test_table_union_1(c1 varchar(10))")
    jdbcUpdate(s"create or replace table $test_table_union_2(c1 varchar(10))")
    jdbcUpdate(s"insert into $test_table_union_1 values ('row1')")
    jdbcUpdate(s"insert into $test_table_union_1 values ('row2')")
    jdbcUpdate(s"insert into $test_table_union_2 values ('row2')")
    jdbcUpdate(s"insert into $test_table_union_2 values ('row3')")

    val tmpDF1 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_union_1)
      .load()

    tmpDF1.createOrReplaceTempView("test_union_1")

    val tmpDF2 = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", test_table_union_2)
      .load()

    tmpDF2.createOrReplaceTempView("test_union_2")

    val df1 = sparkSession.sql("select c1 from test_union_1")
    val df2 = sparkSession.sql("select c1 from test_union_2")
    val resultUnion = df1.union(df2)

    val expectedResult = Seq(
      Row("row1"),
      Row("row2"),
      Row("row2"),
      Row("row3"))

    resultUnion.printSchema()
    resultUnion.show()

    testPushdown(
      s"""(select * from($test_table_union_1)as"sf_connector_query_alias")
         |union all
         |(select * from($test_table_union_2)as"sf_connector_query_alias")
         |""".stripMargin,
      resultUnion,
      expectedResult
    )

    val resultUnionAll = df1.unionAll(df2)

    testPushdown(
      s"""(select * from($test_table_union_1)as"sf_connector_query_alias")
         |union all
         |(select * from($test_table_union_2)as"sf_connector_query_alias")
         |""".stripMargin,
      resultUnionAll,
      expectedResult
    )
  }

  test("test pushdown casewhen() function with other") {
    jdbcUpdate(s"create or replace table $test_table_case_when_1(gender string)")
    jdbcUpdate(s"insert into $test_table_case_when_1 values (null), ('M'), ('F'), ('MMM')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_case_when_1)
      .load()

    val result = tmpDF.withColumn("new_gender",
      functions.when(tmpDF("gender") === "M", "Male")
        .when(tmpDF("gender") === "F", "Female")
        .otherwise("Other"))

    result.show()

    val expectedResult = Seq(
      Row("F", "Female"),
      Row("M", "Male"),
      Row("MMM", "Other"),
      Row(null, "Other")
    )

    testPushdown(
      s""" select ("subquery_0"."gender") as "subquery_1_col_0",(case
         |when ("subquery_0"."gender"='m') then 'male'
         |when ("subquery_0"."gender"='f' ) then 'female'
         |else 'other' end)
         |as "subquery_1_col_1" from ( select * from ($test_table_case_when_1)
         |as "sf_connector_query_alias" ) as "subquery_0" """.stripMargin,
      result,
      expectedResult
    )
  }

  test("test pushdown casewhen() function without other") {
    jdbcUpdate(s"create or replace table $test_table_case_when_2(gender string)")
    jdbcUpdate(s"insert into $test_table_case_when_2 values (null), ('M'), ('F'), ('MMM')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_case_when_2)
      .load()

    val resultNoOther = tmpDF.withColumn("new_gender",
      functions.when(tmpDF("gender") === "M", "Male"))

    resultNoOther.show()

    val expectedResultNoOther = Seq(
      Row("F", null),
      Row("M", "Male"),
      Row("MMM", null),
      Row(null, null)
    )

    testPushdown(
      s""" select ("subquery_0"."gender") as "subquery_1_col_0",(case
         |when ("subquery_0"."gender"='m') then 'male' end )
         |as "subquery_1_col_1" from ( select * from ($test_table_case_when_2)
         |as "sf_connector_query_alias" ) as "subquery_0" """.stripMargin,
      resultNoOther,
      expectedResultNoOther
    )
  }

  test("test pushdown left semi join and left anti join function") {
    jdbcUpdate(s"create or replace table $test_table_left_semi_join_left(id int, gender string)")
    jdbcUpdate(s"insert into $test_table_left_semi_join_left values (1, null), (2, 'M'), (2, 'F'), (4, 'MMM')")

    jdbcUpdate(s"create or replace table $test_table_left_semi_join_right(id int, name string)")
    jdbcUpdate(s"insert into $test_table_left_semi_join_right values (1, 'test'), (2, 'allen'), (3, 'apple'), (3, 'join')")

    val tmpDFLeft = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_left_semi_join_left)
      .load()

    val tmpDFRight = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_left_semi_join_right)
      .load()

    val resultSemi = tmpDFLeft.join(tmpDFRight, tmpDFLeft("id") === tmpDFRight("id"), "leftsemi")

    resultSemi.show()

    val expectedResultSemi = Seq(
      Row(1, null),
      Row(2, "M"),
      Row(2, "F")
    )

    testPushdown(
      s"""select ("subquery_1"."id") as "subquery_5_col_0" ,
         |("subquery_1"."gender") as "subquery_5_col_1" from (
         |  select * from (
         |    select * from ($test_table_left_semi_join_left) as "sf_connector_query_alias"
         |  ) as "subquery_0" where ("subquery_0"."id" is not null)
         |) as "subquery_1" where exists (
         |  select * from (
         |    select ("subquery_3"."id") as "subquery_4_col_0" from (
         |      select * from (
         |        select * from ($test_table_left_semi_join_right) as "sf_connector_query_alias"
         |      ) as "subquery_2" where ("subquery_2"."id" is not null)
         |    ) as "subquery_3"
         |  ) as "subquery_4" where("subquery_1"."id" = "subquery_4"."subquery_4_col_0")
         |)""".stripMargin,
      resultSemi,
      expectedResultSemi
    )

    val resultAnti = tmpDFLeft.join(tmpDFRight, tmpDFLeft("id") === tmpDFRight("id"), "leftanti")

    resultAnti.show()

    val expectedResultAnti = Seq(
      Row(4, "MMM")
    )

    testPushdown(
      s"""select ("subquery_0"."id") as "subquery_4_col_0",
         |("subquery_0"."gender") as "subquery_4_col_1" from (
         |  select * from ($test_table_left_semi_join_left) as "sf_connector_query_alias"
         |) as "subquery_0" where not exists (
         |  select * from (
         |    select ("subquery_2"."id") as "subquery_3_col_0" from (
         |      select * from (
         |        select * from ($test_table_left_semi_join_right) as "sf_connector_query_alias"
         |      ) as "subquery_1" where ("subquery_1"."id" is not null)
         |    ) as "subquery_2"
         |  ) as "subquery_3" where ("subquery_0"."id" = "subquery_3"."subquery_3_col_0")
         |)""".stripMargin,
      resultAnti,
      expectedResultAnti
    )
  }

  test("test pushdown ShiftLeft() function") {
    jdbcUpdate(s"create or replace table $test_table_shift_left(value int)")
    jdbcUpdate(s"insert into $test_table_shift_left values (null), (-5), (-1), (0), (1), (5)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_shift_left)
      .load()

    tmpDF.createOrReplaceTempView("test_table_shift_left")

    val result = sparkSession.sql("SELECT shiftleft(value, 1) from test_table_shift_left")

    result.show()

    val expectedResult = Seq(
      Row(null),
      Row(-10),
      Row(-2),
      Row(0),
      Row(2),
      Row(10)
    )

    testPushdown(
      s"""select( bitshiftleft( cast("subquery_0"."value" as number), 1)) as "subquery_1_col_0" from (
         |  select * from ($test_table_shift_left) as "sf_connector_query_alias"
         |  ) as "subquery_0" """.stripMargin,
      result,
      expectedResult
    )
  }

  test("test pushdown ShiftRight() function") {
    jdbcUpdate(s"create or replace table $test_table_shift_right(value int)")
    jdbcUpdate(s"insert into $test_table_shift_right values (null), (-5), (-1), (0), (1), (5)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_shift_right)
      .load()

    tmpDF.createOrReplaceTempView("test_table_shift_right")

    val result = sparkSession.sql("SELECT shiftright(value, 1) from test_table_shift_right")

    result.show()

    val expectedResult = Seq(
      Row(null),
      Row(-3),
      Row(-1),
      Row(0),
      Row(0),
      Row(2)
    )

    testPushdown(
      s"""select( bitshiftright( cast("subquery_0"."value" as number), 1)) as "subquery_1_col_0" from (
         |  select * from ($test_table_shift_right) as "sf_connector_query_alias"
         |  ) as "subquery_0" """.stripMargin,
      result,
      expectedResult
    )

    val resultShift2 = sparkSession.sql("SELECT shiftright(value, 2) from test_table_shift_right")

    resultShift2.show()

    val expectedResultShift2 = Seq(
      Row(null),
      Row(-2),
      Row(-1),
      Row(0),
      Row(0),
      Row(1)
    )

    testPushdown(
      s"""select( bitshiftright( cast("subquery_0"."value" as number), 2)) as "subquery_1_col_0" from (
         |  select * from ($test_table_shift_right) as "sf_connector_query_alias"
         |  ) as "subquery_0" """.stripMargin,
      resultShift2,
      expectedResultShift2
    )
  }

  test("test pushdown IN() function") {
    jdbcUpdate(s"create or replace table $test_table_in(value int)")
    jdbcUpdate(s"insert into $test_table_in values (null), (-5), (-1), (0), (1), (5)")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_in)
      .load()

    tmpDF.createOrReplaceTempView("test_table_in")

    val result = sparkSession.sql("SELECT value from test_table_in where value in (-5, 123, 'test', " +
      "(select value from test_table_in where value > 4))")

    result.show()

    val expectedResult = Seq(
      Row(-5),
      Row(5)
    )

    testPushdown(
      s"""select * from (
         |  select * from ($test_table_in) as "sf_connector_query_alias"
         |) as "subquery_0" where (
         |  cast ("subquery_0"."value" as varchar) in (
         |    '-5','123','test',cast (
         |      (
         |        select * from (
         |          select * from ($test_table_in) as" sf_connector_query_alias"
         |        ) as "subquery_0" where (
         |          ("subquery_0"."value"is not null) and ("subquery_0"."value">4)
         |        )
         |      ) as varchar
         |    )
         |  )
         |)
         |""".stripMargin,
      result,
      expectedResult
    )
  }

  test("test pushdown INSET() function") {
    jdbcUpdate(s"create or replace table $test_table_in_set(value int, name string)")
    jdbcUpdate(s"insert into $test_table_in_set values (null, 'test'), (-5, 'test1'), " +
      s"(-1, 'test2'), (0, 'test3'), (1, 'test4'), (5, 'test5')")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_in_set)
      .load()

    tmpDF.createOrReplaceTempView("test_table_in_set")

    // if number of values is greater than 10, spark will convert IN to INSET
    val result = sparkSession.sql("SELECT value from test_table_in_set where value in " +
      "(-5, 1,2,3,4,5,6,7,8,9,10,11,12,13,14) and name in " +
      "('test1','test2','test3','test4','test6','test7','test8','test9','1','2','3','4','5','6')")

    result.show()

    val expectedResult = Seq(
      Row(-5),
      Row(1)
    )

    // Not sure whether the order of the values in the IN cluster changes.
    testPushdown(
      s"""select("subquery_1"."value") as "subquery_2_col_0" from (
         |  select * from (
         |    select * from ($test_table_in_set) as "sf_connector_query_alias"
         |  ) as "subquery_0" where (
         |    "subquery_0"."value" in (3,8,11,13,9,5,2,12,10,6,1,4,7,14,-5)
         |    and
         |    "subquery_0"."name" in ('test4','test3','3','5','test2','test7','test1','6','test8','1','test6','4','2','test9')
         |  )
         |) as "subquery_1"
         |""".stripMargin,
      result,
      expectedResult
    )
  }

  test("test pushdown INSET() function with timestamps and double") {
    jdbcUpdate(s"create or replace table $test_table_in_set(value double, cur_time timestamp)")
    jdbcUpdate(s"insert into $test_table_in_set values " +
      s"(null, TO_TIMESTAMP_NTZ('2014-01-01 16:00:22')), " +
      s"(-5.1, TO_TIMESTAMP_NTZ('2014-01-01 16:00:33')), " +
      s"(1.1, TO_TIMESTAMP_NTZ('2014-01-01 16:00:00')), " +
      s"(0.1, TO_TIMESTAMP_NTZ('2014-01-01 16:00:00'))")

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_in_set)
      .load()

    tmpDF.createOrReplaceTempView("test_table_in_set")

    // if number of values is greater than 10, spark will convert IN to INSET
    val result = sparkSession.sql("SELECT value, cur_time from test_table_in_set where value in " +
      "(-5.1, 1.1,2,3,4,5,6,7,8,9,10,11,12,13,14.14) and cur_time in " +
      "('2014-01-01 16:00:00.000', '2014-01-01 16:00:01.000', '2014-01-01 16:00:02.000'," +
      " '2014-01-01 16:00:03.000', '2014-01-01 16:00:04.000', '2014-01-01 16:00:05.000'," +
      " '2014-01-01 16:00:06.000', '2014-01-01 16:00:07.000', '2014-01-01 16:00:08.000'," +
      " '2014-01-01 16:00:09.000', '2014-01-01 16:00:10.000', '2014-01-01 16:00:11.000')")

    result.show(truncate=false)

    val expectedResult = Seq(
      Row(1.1, Timestamp.valueOf("2014-01-01 16:00:00"))
    )

    testPushdown(
      s"""select * from (select * from ($test_table_in_set) as "sf_connector_query_alias") as "subquery_0"
         |where( "subquery_0"."value" in
         |  (12.0,14.14,3.0,4.0,13.0,1.1,7.0,5.0,11.0,8.0,-5.1,2.0,6.0,9.0,10.0) and cast
         |  ("subquery_0"."cur_time"as varchar) in
         |  ('2014-01-0116:00:08.000','2014-01-0116:00:07.000','2014-01-0116:00:04.000','2014-01-0116:00:05.000',
         |  '2014-01-0116:00:00.000','2014-01-0116:00:10.000','2014-01-0116:00:09.000','2014-01-0116:00:03.000',
         |  '2014-01-0116:00:06.000','2014-01-0116:00:01.000','2014-01-0116:00:02.000','2014-01-0116:00:11.000')
         |)
         |""".stripMargin,
      result,
      expectedResult,
      testPushdownOff = false
    )
  }

  test("test pushdown COALESCE()") {
    jdbcUpdate(s"create or replace table $test_table_coalesce(c1 int, c2 int, c3 int)")
    jdbcUpdate(s"""insert into $test_table_coalesce values
               | (1,    2,    3   ),
               | (null, 2,    3   ),
               | (null, null, 3   ),
               | (null, null, null),
               | (1,    null, 3   ),
               | (1,    null, null),
               | (1,    2,    null)
               |""".stripMargin)

    val tmpDF = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_coalesce)
      .load()

    tmpDF.createOrReplaceTempView("test_table_coalesce")

    val result = sparkSession.sql(
      "SELECT c1, c2, c3, COALESCE(c1, c2, c3), COALESCE(c1, 6), COALESCE(-6, c2) from test_table_coalesce")

    result.show(truncate=false)

    val expectedResult = Seq(
      Row(1, 2, 3, 1, 1, -6),
      Row(null, 2, 3, 2, 6, -6),
      Row(null, null, 3, 3, 6, -6),
      Row(null, null, null, null, 6, -6),
      Row(1, null, 3, 1, 1, -6),
      Row(1, null, null, 1, 1, -6),
      Row(1, 2, null, 1, 1, -6)
    )

    testPushdown(
      s"""SELECT ( "SUBQUERY_0"."C1" ) AS "SUBQUERY_1_COL_0" ,
         |( "SUBQUERY_0"."C2" ) AS "SUBQUERY_1_COL_1" ,
         |( "SUBQUERY_0"."C3" ) AS "SUBQUERY_1_COL_2" ,
         |( COALESCE( "SUBQUERY_0"."C1" ,
         |            "SUBQUERY_0"."C2" ,
         |            "SUBQUERY_0"."C3" ) ) AS "SUBQUERY_1_COL_3" ,
         | ( COALESCE ( "SUBQUERY_0"."C1" , 6 ) ) AS "SUBQUERY_1_COL_4" ,
         | ( COALESCE ( -6 , "SUBQUERY_0"."C2" ) ) AS "SUBQUERY_1_COL_5"
         | FROM ( SELECT * FROM ( $test_table_coalesce ) AS
         | "SF_CONNECTOR_QUERY_ALIAS" ) AS "SUBQUERY_0"
         |""".stripMargin,
      result,
      expectedResult
    )
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }
}
// scalastyle:on println

