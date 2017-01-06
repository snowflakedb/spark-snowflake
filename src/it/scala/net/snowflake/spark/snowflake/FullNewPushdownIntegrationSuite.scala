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

package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.util.Random

class FullNewPushdownIntegrationSuite extends IntegrationSuiteBase {

  private val test_table: String  = s"test_table_1_$randomSuffix"
  private val test_table2: String = s"test_table_2_$randomSuffix"
  private val table_placeholder   = "table_placeholder"

  private val numRows1 = 50
  private val numRows2 = 50

  override def beforeAll(): Unit = {
    super.beforeAll()

    SnowflakeConnectorUtils.enablePushdownSession(sparkSession)

    val st1 = new StructType(
      Array(StructField("id", IntegerType, nullable = true),
            StructField("randInt", IntegerType, nullable = true),
            StructField("randStr", StringType, nullable = true),
            StructField("randBool", BooleanType, nullable = true),
            StructField("randLong", LongType, nullable = true)))

    val st2 = new StructType(
      Array(StructField("id", IntegerType, nullable = true),
            StructField("randStr2", StringType, nullable = true),
            StructField("randStr3", StringType, nullable = true),
            StructField("randInt2", IntegerType, nullable = true)))

    val df1_spark = sqlContext
      .createDataFrame(sc.parallelize(1 to numRows1)
                         .map[Row](value => {
                           val rand = new Random(System.nanoTime())
                           Row(value,
                               rand.nextInt(),
                               rand.nextString(10),
                               rand.nextBoolean(),
                               rand.nextLong())
                         }),
                       st1)
      .cache()

    // Contains some nulls
    val df2_spark = sqlContext
      .createDataFrame(
        sc.parallelize(1 to numRows2)
          .map[Row](value => {
            val rand = new Random(System.nanoTime())
            Row(value, rand.nextString(10), rand.nextString(5), {
              val r = rand.nextInt()
              if (r % 5 == 2) null
              else r
            })
          }),
        st2)
      .cache()

    try {
      df1_spark.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", test_table)
        .mode(SaveMode.Overwrite)
        .save()

      df2_spark.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", test_table2)
        .mode(SaveMode.Overwrite)
        .save()

      val df1_snowflake = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", s"$test_table")
        .load()

      val df2_snowflake = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", s"$test_table2")
        .load()

      df1_spark.createOrReplaceTempView("df_spark1")
      df2_spark.createOrReplaceTempView("df_spark2")
      df1_snowflake.createOrReplaceTempView("df_snowflake1")
      df2_snowflake.createOrReplaceTempView("df_snowflake2")
    } catch {
      case e: Exception =>
        jdbcUpdate(s"drop table if exists $test_table")
        jdbcUpdate(s"drop table if exists $test_table2")
        throw (e)
    }
  }

  test("Select all columns.") {
    testDF(sql = s"""SELECT * FROM ${table_placeholder}1""",
           ref = s"""SELECT * FROM $test_table""")
  }

  test("Join") {
    testDF(
      sql = s"""SELECT b.id, a.randInt from ${table_placeholder}1 as a
         INNER JOIN ${table_placeholder}2 as b on a.randBool = ISNULL(b.randInt2)""".stripMargin,
      ref =
        s"""SELECT ("subquery_5"."subquery_5_col_2") AS "subquery_6_col_0", ("subquery_5"."subquery_5_col_0") AS "subquery_6_col_1" FROM
	(SELECT ("subquery_2"."subquery_2_col_0") AS "subquery_5_col_0", ("subquery_2"."subquery_2_col_1") AS "subquery_5_col_1", ("subquery_4"."subquery_4_col_0") AS "subquery_5_col_2", ("subquery_4"."subquery_4_col_1") AS "subquery_5_col_3" FROM
		(SELECT ("subquery_1"."RANDINT") AS "subquery_2_col_0", ("subquery_1"."RANDBOOL") AS "subquery_2_col_1" FROM
			(SELECT * FROM
				(SELECT * FROM $test_table
			) AS "subquery_0"
		 WHERE ("subquery_0"."RANDBOOL" IS NOT NULL)
		) AS "subquery_1"
	) AS "subquery_2"
 INNER JOIN
	(SELECT ("subquery_3"."ID") AS "subquery_4_col_0", ("subquery_3"."RANDINT2") AS "subquery_4_col_1" FROM
		(SELECT * FROM $test_table2
		) AS "subquery_3"
	) AS "subquery_4"
 ON ("subquery_2"."subquery_2_col_1" = ("subquery_4"."subquery_4_col_1" IS NULL))
) AS "subquery_5"""")
  }

  test("Concatenation and LPAD") {
    testDF(
      sql =
        s"""SELECT concat(randStr2, randStr3) as c, lpad(randStr2, 5, '%') as l from ${table_placeholder}2""",
      ref =
        s"""SELECT (CONCAT("subquery_0"."RANDSTR2", "subquery_0"."RANDSTR3")) AS "subquery_1_col_0", (LPAD("subquery_0"."RANDSTR2", 5, '%')) AS "subquery_1_col_1" FROM
	(SELECT * FROM $test_table2
) AS "subquery_0"""")
  }

  test("Translate") {
    testDF(
      sql =
        s"""SELECT translate(randStr2, 'sd', 'po') as l from ${table_placeholder}2""",
      ref =
        s"""SELECT (TRANSLATE("subquery_0"."RANDSTR2", 'sd', 'po')) AS "subquery_1_col_0" FROM
	(SELECT * FROM $test_table2
) AS "subquery_0"""")
  }

  test("Join and Max Aggregation") {
    testDF(
      sql =
        s"""SELECT a.id, max(b.randInt2) from ${table_placeholder}1 as a INNER JOIN
         ${table_placeholder}2 as b on cast(a.randInt/5 as integer) = cast(b.randInt2/5 as integer) group by a.id""".stripMargin,
      ref =
        s"""SELECT ("subquery_5"."subquery_5_col_0") AS "subquery_6_col_0", (MAX("subquery_5"."subquery_5_col_1")) AS "subquery_6_col_1" FROM
	(SELECT ("subquery_4"."subquery_4_col_0") AS "subquery_5_col_0", ("subquery_4"."subquery_4_col_2") AS "subquery_5_col_1" FROM
		(SELECT ("subquery_1"."subquery_1_col_0") AS "subquery_4_col_0", ("subquery_1"."subquery_1_col_1") AS "subquery_4_col_1", ("subquery_3"."subquery_3_col_0") AS "subquery_4_col_2" FROM
			(SELECT ("subquery_0"."ID") AS "subquery_1_col_0", ("subquery_0"."RANDINT") AS "subquery_1_col_1" FROM
				(SELECT * FROM $test_table
			) AS "subquery_0"
		) AS "subquery_1"
	 INNER JOIN
		(SELECT ("subquery_2"."RANDINT2") AS "subquery_3_col_0" FROM
			(SELECT * FROM $test_table2
			) AS "subquery_2"
		) AS "subquery_3"
	 ON (CAST(("subquery_1"."subquery_1_col_1" / 5) AS NUMBER) = CAST(("subquery_3"."subquery_3_col_0" / 5) AS NUMBER))
	) AS "subquery_4"
) AS "subquery_5"
 GROUP BY "subquery_5"."subquery_5_col_0"""")
  }

  /** Below tests bypass query check because pushdowns may sometimes swap ordering of multiple join and groupBy
    * predicates, causing failure. The pushdown queries are otherwise correct (as of the writing of this comment).
    */
  test("Join on multiple conditions") {
    testDF(sql = s"""SELECT b.randStr2 from ${table_placeholder}1 as a
         INNER JOIN ${table_placeholder}2 as b on ISNULL(b.randInt2) = a.randBool AND a.randStr=b.randStr2""".stripMargin,
           ref = s"""SELECT "subquery_6"."RANDSTR2" FROM
	(SELECT * FROM
		(SELECT "subquery_1"."RANDSTR", "subquery_1"."RANDBOOL" FROM
			(SELECT * FROM
				(SELECT * FROM $test_table
			) AS "subquery_0"
		 WHERE (("subquery_0"."RANDSTR" IS NOT NULL) AND ("subquery_0"."RANDBOOL" IS NOT NULL))
		) AS "subquery_1"
	) AS "subquery_2"
 INNER JOIN
	(SELECT "subquery_4"."RANDSTR2", "subquery_4"."RANDINT2" FROM
		(SELECT * FROM
			(SELECT * FROM $test_table2
			) AS "subquery_3"
		 WHERE ("subquery_3"."RANDSTR2" IS NOT NULL)
		) AS "subquery_4"
	) AS "subquery_5"
 ON ((("subquery_5"."RANDINT2" IS NULL) = "subquery_2"."RANDBOOL") AND ("subquery_2"."RANDSTR" = "subquery_5"."RANDSTR2"))
) AS "subquery_6"""",
           bypassQueryCheck = true)
  }

  test("Aggregate by multiple columns") {
    testDF(
      sql =
        s"""SELECT max(randLong) as m from ${table_placeholder}1 group by randInt,randBool""",
      ref = s"""SELECT (max("subquery_1"."RANDLONG")) AS "m" FROM
	(SELECT "subquery_0"."RANDINT", "subquery_0"."RANDBOOL", "subquery_0"."RANDLONG" FROM
		(SELECT * FROM $test_table
	) AS "subquery_0"
) AS "subquery_1"
 GROUP BY "subquery_1"."RANDINT", "subquery_1"."RANDBOOL"""",
      bypassQueryCheck = true)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table")
      jdbcUpdate(s"drop table if exists $test_table2")
    } finally {
      super.afterAll()
      SnowflakeConnectorUtils.disablePushdownSession(sqlContext.sparkSession)
    }
  }

  def testDF(sql: String, ref: String, bypassQueryCheck: Boolean = false) = {

    val df_spark =
      sparkSession.sql(sql.replaceAll(s"""$table_placeholder""", "df_spark"))
    val df_snowflake =
      sparkSession.sql(
        sql.replaceAll(s"""$table_placeholder""", "df_snowflake"))

    testPushdown(ref, df_snowflake, df_spark.collect, bypassQueryCheck)
  }
}
