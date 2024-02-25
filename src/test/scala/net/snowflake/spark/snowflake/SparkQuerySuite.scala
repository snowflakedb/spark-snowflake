package net.snowflake.spark.snowflake

import net.snowflake.spark.snowflake.pushdowns.SnowflakeScanExec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{ExplainMode, FormattedMode}
import org.scalatest.{BeforeAndAfter, FunSuite}

class SparkQuerySuite extends FunSuite with BeforeAndAfter {
  private var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
  }

  after {
    spark.stop()
  }

  test("pushdown scan to snowflake") {
    spark.sql(
      """
         CREATE TABLE student(name string)
                USING net.snowflake.spark.snowflake
              OPTIONS (dbtable 'default.student',
                       sfdatabase 'sf-db',
                       tempdir '/tmp/dir',
                       sfurl 'accountname.snowflakecomputing.com:443',
                       sfuser 'alice',
                       sfpassword 'hello-snowflake')
         """).show()

    val df = spark.sql(
      """
        |SELECT *
        |  FROM student
        |""".stripMargin)
    val plan = df.queryExecution.executedPlan

    assert(plan.isInstanceOf[SnowflakeScanExec])
    val sfPlan = plan.asInstanceOf[SnowflakeScanExec]
    assert(sfPlan.snowflakeSQL.toString ==
      """SELECT * FROM ( default.student ) AS "SF_CONNECTOR_QUERY_ALIAS""""
        .stripMargin)

    // explain plan
    val planString = df.queryExecution.explainString(FormattedMode)
    val expectedString =
      """== Physical Plan ==
        |SnowflakeScan (1)
        |
        |
        |(1) SnowflakeScan
        |Output [1]: [name#1]
        |Arguments: [name#1], SELECT * FROM ( default.student ) AS "SF_CONNECTOR_QUERY_ALIAS", SnowflakeRelation
        """.stripMargin
    assert(planString.trim == expectedString.trim)
  }

}
