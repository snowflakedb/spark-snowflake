/*
 * Copyright 2015-2016 Snowflake Computing
 * Copyright 2015 Databricks
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
import net.snowflake.spark.snowflake.pushdowns.SnowflakeStrategy
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.util.matching.Regex

/**
  * Base class for writing integration tests which run against a real Snowflake cluster.
  */
trait IntegrationSuiteBase
    extends IntegrationEnv
    with QueryTest {

  private val log = LoggerFactory.getLogger(getClass)

  def getAzureURL(input: String): String = {
    val azure_url = "wasbs?://([^@]+)@([^.]+)\\.([^/]+)/(.+)?".r
    input match {
      case azure_url(container, account, endpoint, _) =>
        s"fs.azure.sas.$container.$account.$endpoint"
      case _ => throw new IllegalArgumentException(s"invalid wasb url: $input")
    }
  }

  /**
    * Verify that the pushdown was done by looking at the generated SQL,
    * and check the results are as expected
    */
  private def testPushdownBasic(reference: String,
                                result: DataFrame,
                                expectedAnswer: Seq[Row],
                                bypass: Boolean = false,
                                printSqlText: Boolean = false): Unit = {

    // Verify the query issued is what we expect
    checkAnswer(result, expectedAnswer)

    // It is used to retrieve expected query text.
    if (printSqlText) {
      println(Utils.getLastSelect)
    }

    if (!bypass) {
      assert(
        Utils.getLastSelect.replaceAll("\\s+", "").toLowerCase == reference.trim
          .replaceAll("\\s+", "")
          .toLowerCase
      )
    }
  }

  /**
    * Verify that the pushdown was done by looking at the generated SQL,
    * and check the results are as expected.
    * It also reads the DataFrame after disabling the pushdown.
    * The test result should be as expected too.
    */
  def testPushdown(reference: String,
                   result: DataFrame,
                   expectedAnswer: Seq[Row],
                   bypass: Boolean = false,
                   printSqlText: Boolean = false,
                   testPushdownOff: Boolean = true): Unit = {
    testPushdownBasic(reference, result, expectedAnswer, bypass, printSqlText)

    // Disable pushdown and rerun the dataframe, the result should match
    if (testPushdownOff && isPushdownEnabled(result.sparkSession)) {
      try {
        SnowflakeConnectorUtils.disablePushdownSession(result.sparkSession)
        // Re-read the DataFrame but don't check the executed query text.
        // 'result' has been compiled, so spark plan with pushdown could have been cached.
        // So, use 'result.select("*")' to make sure snowflake pushdown is not used.
        testPushdownBasic(reference, result.select("*"), expectedAnswer, bypass = true, printSqlText)
      } catch {
        case th: Throwable => {
          println(s"Fail to read DataFrame with pushdown disabled. ${th.getMessage}")
          throw th
        }
      } finally {
        SnowflakeConnectorUtils.enablePushdownSession(result.sparkSession)
      }
    }
  }

  private def isPushdownEnabled(session: SparkSession): Boolean = {
    session.experimental.extraStrategies.exists( s => s.isInstanceOf[SnowflakeStrategy])
  }

  /**
    * Save the given DataFrame to Snowflake, then load the results back into a DataFrame and check
    * that the returned DataFrame matches the one that we saved.
    *
    * @param tableName               the table name to use
    * @param df                      the DataFrame to save
    * @param expectedSchemaAfterLoad if specified, the expected schema after loading the data back
    *                                from Snowflake. This should be used in cases where you expect
    *                                the schema to differ due to reasons like case-sensitivity.
    * @param saveMode                the [[SaveMode]] to use when writing data back to Snowflake
    */
  def testRoundtripSaveAndLoad(
    tableName: String,
    df: DataFrame,
    expectedSchemaAfterLoad: Option[StructType] = None,
    saveMode: SaveMode = SaveMode.ErrorIfExists
  ): Unit = {
    try {
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .mode(saveMode)
        .save()
      assert(DefaultJDBCWrapper.tableExists(params, tableName))
      val loadedDf = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptions)
        .option("dbtable", tableName)
        .load()
      assert(loadedDf.schema === expectedSchemaAfterLoad.getOrElse(df.schema))
      checkAnswer(loadedDf, df.collect())
    } finally {
      conn.createStatement.executeUpdate(s"drop table if exists $tableName")
      conn.commit()
    }
  }



  // Utility function to drop some garbage test tables.
  // Be careful to use this function which drops a bunch of tables.
  // Suggest you to run with "printOnly = true" to make sure the tables are correct.
  // And then run with "printOnly = false"
  // For example, dropTestTables(".*TEST_TABLE_.*\\d+".r, true)
  def dropTestTables(regex: Regex, printOnly: Boolean): Unit = {
    val statement = conn.createStatement()

    statement.execute("show tables")
    val resultset = statement.getResultSet
    while (resultset.next()) {
      val tableName = resultset.getString(2)
      tableName match {
        case regex() =>
          if (printOnly) {
            // scalastyle:off println
            println(s"will drop table: $tableName")
            // scalastyle:on println
          } else {
            jdbcUpdate(s"drop table $tableName")
          }
        case _ => None
      }
    }
  }
}
