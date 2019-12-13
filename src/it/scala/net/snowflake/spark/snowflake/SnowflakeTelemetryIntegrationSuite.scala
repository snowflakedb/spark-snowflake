package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._

import scala.util.Random

/**
  * This test suite is unstable, ignored by default, for manual test only.
  */
class SnowflakeTelemetryIntegrationSuite extends IntegrationSuiteBase {

  private val test_table: String = s"test_table_1_$randomSuffix"
  private val test_table2: String = s"test_table_2_$randomSuffix"

  private val numRows1 = 50
  private val numRows2 = 50

  private val mapper = new ObjectMapper

  override def beforeAll(): Unit = {
    super.beforeAll()

    SnowflakeConnectorUtils.enablePushdownSession(sparkSession)

    val st1 = new StructType(
      Array(
        StructField("id", IntegerType, nullable = true),
        StructField("randInt", IntegerType, nullable = true),
        StructField("randStr", StringType, nullable = true),
        StructField("randBool", BooleanType, nullable = true),
        StructField("randLong", LongType, nullable = true)
      )
    )

    val st2 = new StructType(
      Array(
        StructField("id", IntegerType, nullable = true),
        StructField("randStr2", StringType, nullable = true),
        StructField("randStr3", StringType, nullable = true),
        StructField("randInt2", IntegerType, nullable = true)
      )
    )

    val df1_spark = sqlContext
      .createDataFrame(
        sc.parallelize(1 to numRows1)
          .map[Row](value => {
            val rand = new Random(System.nanoTime())
            Row(
              value,
              rand.nextInt(),
              rand.nextString(10),
              rand.nextBoolean(),
              rand.nextLong()
            )
          }),
        st1
      )
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
        st2
      )
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
        throw e
    }
  }

  // these test suits are unstable because the properties of json is unordered.
  // todo: create a method the compare two json strings.

  ignore("LogicalRelation, Filter, Project, Join") {
    checker(s"""
         |SELECT b.randStr2
         |from df_snowflake1 as a INNER JOIN df_snowflake2 as b
         |on ISNULL(b.randInt2) = a.randBool AND a.randStr=b.randStr2
         |""".stripMargin, s"""
         |{"type":"spark_plan","source":"spark_connector","data":{"action":
         |"ReturnAnswer","args":"","children":[{"action":"Project","args":{
         |"fields":[{"source":"AttributeReference","type":"string"}]},"children"
         |:[{"action":"Join","args":{"type":"Inner","conditions":{"operator":
         |"And","parameters":[{"operator":"EqualTo","parameters":[{"operator":
         |"IsNull","parameters":[{"source":"AttributeReference","type":
         |"decimal(38,0)"}]},{"source":"AttributeReference","type":"boolean"}]},
         |{"operator":"EqualTo","parameters":[{"source":"AttributeReference",
         |"type":"string"},{"source":"AttributeReference","type":"string"}]}]}},
         |"children":[{"action":"Project","args":{"fields":[{"source":
         |"AttributeReference","type":"string"},{"source":"AttributeReference",
         |"type":"boolean"}]},"children":[{"action":"Filter","args":{"conditions":
         |{"operator":"And","parameters":[{"operator":"IsNotNull","parameters":
         |[{"source":"AttributeReference","type":"boolean"}]},{"operator":
         |"IsNotNull","parameters":[{"source":"AttributeReference","type":
         |"string"}]}]}},"children":[{"action":"SnowflakeRelation","args":
         |{"schema":["decimal(38,0)","decimal(38,0)","string","boolean",
         |"decimal(38,0)"]},"children":[]}]}]},{"action":"Project","args":
         |{"fields":[{"source":"AttributeReference","type":"string"},{"source":
         |"AttributeReference","type":"decimal(38,0)"}]},"children":[{"action":
         |"Filter","args":{"conditions":{"operator":"IsNotNull","parameters":[
         |{"source":"AttributeReference","type":"string"}]}},"children":[{"action"
         |:"SnowflakeRelation","args":{"schema":["decimal(38,0)","string",
         |"string","decimal(38,0)"]},"children":[]}]}]}]}]}]}}""".stripMargin)

  }
  ignore("aggregation") {
    checker(s"""
         |SELECT a.id, max(b.randInt2)
         |from df_snowflake1 as a INNER JOIN df_snowflake2 as b
         |on cast(a.randInt/5 as integer) = cast(b.randInt2/5 as integer)
         |group by a.id""".stripMargin, s"""
         |{"type":"spark_plan","source":"spark_connector","data":{"action":
         |"ReturnAnswer","args":"","children":[{"action":"Aggregate","args":
         |{"field":[{"source":"AttributeReference","type":"decimal(38,0)"},
         |{"operator":"Alias","parameters":[{"operator":"AggregateExpression",
         |"parameters":[{"operator":"Max","parameters":[{"source":
         |"AttributeReference","type":"decimal(38,0)"}]}]}]}],"group":
         |[{"source":"AttributeReference","type":"decimal(38,0)"}]},"children":
         |[{"action":"Project","args":{"fields":[{"source":"AttributeReference",
         |"type":"decimal(38,0)"},{"source":"AttributeReference","type":
         |"decimal(38,0)"}]},"children":[{"action":"Join","args":{"type":"Inner",
         |"conditions":{"operator":"EqualTo","parameters":[{"operator":
         |"Cast","parameters":[{"operator":"CheckOverflow","parameters":
         |[{"operator":"Divide","parameters":[{"operator":"PromotePrecision",
         |"parameters":[{"source":"AttributeReference","type":"decimal(38,0)"}]},
         |{"source":"Literal","type":"decimal(38,0)"}]}]}]},{"operator":
         |"Cast","parameters":[{"operator":"CheckOverflow","parameters":[{
         |"operator":"Divide","parameters":[{"operator":"PromotePrecision",
         |"parameters":[{"source":"AttributeReference","type":"decimal(38,0)"}]},
         |{"source":"Literal","type":"decimal(38,0)"}]}]}]}]}},"children":[{
         |"action":"Project","args":{"fields":[{"source":"AttributeReference",
         |"type":"decimal(38,0)"},{"source":"AttributeReference","type":
         |"decimal(38,0)"}]},"children":[{"action":"SnowflakeRelation","args":
         |{"schema":["decimal(38,0)","decimal(38,0)","string","boolean",
         |"decimal(38,0)"]},"children":[]}]},{"action":"Project","args":
         |{"fields":[{"source":"AttributeReference","type":"decimal(38,0)"}]},
         |"children":[{"action":"SnowflakeRelation","args":{"schema":
         |["decimal(38,0)","string","string","decimal(38,0)"]},"children":
         |[]}]}]}]}]}]}}
       """.stripMargin)

  }
  ignore("limit, sort") {
    checker(s"""
         |select *
         |from df_snowflake1
         |order by randInt desc limit 1
       """.stripMargin, s"""
         |{"type":"spark_plan","source":"spark_connector","data":{"action":
         |"ReturnAnswer","args":"","children":[{"action":"GlobalLimit"
         |,"args":{"condition":{"source":"Literal","type":"integer"}},"children"
         |:[{"action":"LocalLimit","args":{"condition":{"source":"Literal",
         |"type":"integer"}},"children":[{"action":"Sort","args":{"global":
         |true,"order":[{"operator":"SortOrder","parameters":[{"source":
         |"AttributeReference","type":"decimal(38,0)"}]}]},"children":[{"action"
         |:"SnowflakeRelation","args":{"schema":["decimal(38,0)","decimal(38,0)",
         |"string","boolean","decimal(38,0)"]},"children":[]}]}]}]}]}}
       """.stripMargin)
  }
  ignore("window") {
    checker(s"""
         |SELECT id, randStr,
         |avg(randInt) over(),
         |max(randLong) over(partition by id order by randStr)
         |from df_snowflake1
       """.stripMargin, s"""
         |{"type":"spark_plan","source":"spark_connector","data":{"action":
         |"ReturnAnswer","args":"","children":[{"action":"Project",
         |"args":{"fields":[{"source":"AttributeReference","type":"decimal(38,0)"}
         |,{"source":"AttributeReference","type":"string"},{"source":
         |"AttributeReference","type":"decimal(38,4)"},{"source":"AttributeReference"
         |,"type":"decimal(38,0)"}]},"children":[{"action":"Window","args":
         |{"expression":[{"operator":"Alias","parameters":[{"operator":
         |"WindowExpression","parameters":[{"operator":"AggregateExpression",
         |"parameters":[{"operator":"Max","parameters":[{"source":
         |"AttributeReference","type":"decimal(38,0)"}]}]},{"operator":
         |"WindowSpecDefinition","parameters":[{"source":"AttributeReference",
         |"type":"decimal(38,0)"},{"operator":"SortOrder","parameters":[{"source"
         |:"AttributeReference","type":"string"}]},{"operator":"SpecifiedWindowFrame"
         |,"parameters":[{"source":"UnboundedPreceding$$","type":"null"},{"source"
         |:"CurrentRow$$","type":"null"}]}]}]}]}]},"children":[{"action":"Project",
         |"args":{"fields":[{"source":"AttributeReference","type":"decimal(38,0)"},
         |{"source":"AttributeReference","type":"string"},{"source":"AttributeReference"
         |,"type":"decimal(38,0)"},{"source":"AttributeReference","type":
         |"decimal(38,4)"}]},"children":[{"action":"Window","args":{"expression":
         |[{"operator":"Alias","parameters":[{"operator":"WindowExpression",
         |"parameters":[{"operator":"AggregateExpression","parameters":[{"operator"
         |:"Average","parameters":[{"source":"AttributeReference","type":
         |"decimal(38,0)"}]}]},{"operator":"WindowSpecDefinition","parameters":
         |[{"operator":"SpecifiedWindowFrame","parameters":[{"source":
         |"UnboundedPreceding$$","type":"null"},{"source":"UnboundedFollowing$$"
         |,"type":"null"}]}]}]}]}]},"children":[{"action":"Project","args":
         |{"fields":[{"source":"AttributeReference","type":"decimal(38,0)"},
         |{"source":"AttributeReference","type":"string"},{"source":
         |"AttributeReference","type":"decimal(38,0)"},{"source":"AttributeReference"
         |,"type":"decimal(38,0)"}]},"children":[{"action":"SnowflakeRelation",
         |"args":{"schema":["decimal(38,0)","decimal(38,0)","string","boolean",
         |"decimal(38,0)"]},"children":[]}]}]}]}]}]}]}}""".stripMargin)
  }
  ignore("Union") {
    checker(s"""
         |select id from df_snowflake1
         |union
         |select id from df_snowflake2
       """.stripMargin, s"""
         |{"type":"spark_plan","source":"spark_connector","data":{"action":
         |"ReturnAnswer","args":"","children":[{"action":"Aggregate",
         |"args":{"field":[{"source":"AttributeReference","type":"decimal(38,0)"
         |}],"group":[{"source":"AttributeReference","type":"decimal(38,0)"}]},
         |"children":[{"action":"Union","args":"","children":[{"action":"Project"
         |,"args":{"fields":[{"source":"AttributeReference","type":"decimal(38,0)"
         |}]},"children":[{"action":"SnowflakeRelation","args":{"schema":[
         |"decimal(38,0)","decimal(38,0)","string","boolean","decimal(38,0)"]}
         |,"children":[]}]},{"action":"Project","args":{"fields":[{"source":
         |"AttributeReference","type":"decimal(38,0)"}]},"children":[{"action":
         |"SnowflakeRelation","args":{"schema":["decimal(38,0)","string","string"
         |,"decimal(38,0)"]},"children":[]}]}]}]}]}}""".stripMargin)
  }

  private def checker(sql: String, expectation: String): Unit = {

    sparkSession.sql(sql).collect()

    val result = SnowflakeTelemetry.output

    assert(result.equals(mapper.readTree(expectation)))
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

}
