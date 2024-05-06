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

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.test.TestHook
import org.apache.spark.sql.{DataFrame, SaveMode}

// scalastyle:off println
class CloudStorageSuite extends IntegrationSuiteBase {

  import testImplicits._

  private val test_table1: String = s"test_temp_table_$randomSuffix"
  private val test_table_large_result: String =
    s"test_table_large_result_$randomSuffix"
  private val test_table_write: String = s"test_table_write_$randomSuffix"
  private lazy val localDF: DataFrame = Seq((1000, "str11"), (2000, "str22")).toDF("c1", "c2")

  private val largeStringValue =
    s"""spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |""".stripMargin.filter(_ >= ' ')
  private val LARGE_TABLE_ROW_COUNT = 900000
  lazy val setupLargeResultTable = {
    jdbcUpdate(
      s"""create or replace table $test_table_large_result (
         | int_c int, c_string string(1024) )""".stripMargin)

    jdbcUpdate(
      s"""insert into $test_table_large_result select
         | row_number() over (order by seq4()) - 1, '$largeStringValue'
         | from table(generator(rowcount => $LARGE_TABLE_ROW_COUNT))""".stripMargin)
    true
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table1")
      jdbcUpdate(s"drop table if exists $test_table_large_result")
      jdbcUpdate(s"drop table if exists $test_table_write")
    } finally {
      TestHook.disableTestHook()
      super.afterAll()
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"create or replace table $test_table1(c1 int, c2 string)")
    jdbcUpdate(s"insert into $test_table1 values (100, 'str1'),(200, 'str2')")
  }

  private def getHashAgg(tableName: String): java.math.BigDecimal =
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"select HASH_AGG(*) from $tableName")
      .load()
      .collect()(0).getDecimal(0)

  private def getRowCount(tableName: String): Long =
    sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("dbtable", tableName)
      .load()
      .count()

  // manual test for the proxy protocol
  ignore ("snow-1359291") {
    val spark = sparkSession
    import spark.implicits._
    val df = Seq(1, 2, 3, 4).toDF()
    df.show()
    val proxyConfig = Map[String, String](
      "use_proxy" -> "true",
      "proxy_host" -> "127.0.0.1",
      "proxy_port" -> "8080",
      "proxy_protocol" -> "http"
    )
    df.write.format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .options(proxyConfig)
      .option("dbtable", "test12345")
      .mode(SaveMode.Overwrite)
      .save()
  }

  test("write a small DataFrame to GCS with down-scoped-token") {
    // Only run this test on GCS
    if ("gcp".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"))) {
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table1)
        .load()

      // write a small DataFrame to a snowflake table
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table_write)
        // GCS_USE_DOWNSCOPED_CREDENTIAL is not a public parameter, user can't set it.
        // The default value of GCS_USE_DOWNSCOPED_CREDENTIAL will be true from Dec 2023
        // The option set can be removed after Dec 2023
        .option("GCS_USE_DOWNSCOPED_CREDENTIAL", "true")
        .mode(SaveMode.Overwrite)
        .save()

      // Check the source table and target table has same agg_hash.
      assert(getHashAgg(test_table1) == getHashAgg(test_table_write))
    } else {
      println("skip test for non-GCS platform: " +
        "write a small DataFrame to GCS with down-scoped-token")
    }
  }

  test("write a big DataFrame to GCS with down-scoped-token") {
    // Only run this test on GCS
    if ("gcp".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"))) {
      setupLargeResultTable
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("partition_size_in_mb", 1) // generate multiple partitions
        .option("dbtable", test_table_large_result)
        .load()

      // write a small DataFrame to a snowflake table
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table_write)
        // GCS_USE_DOWNSCOPED_CREDENTIAL is not a public parameter, user can't set it.
        // The default value of GCS_USE_DOWNSCOPED_CREDENTIAL will be true from Dec 2023
        // The option set can be removed after Dec 2023
        .option("GCS_USE_DOWNSCOPED_CREDENTIAL", "true")
        .mode(SaveMode.Overwrite)
        .save()

      // Check the source table and target table has same agg_hash.
      assert(getHashAgg(test_table_large_result) == getHashAgg(test_table_write))
    } else {
      println("skip test for non-GCS platform: " +
        "write a big DataFrame to GCS with down-scoped-token")
    }
  }

  test("write a empty DataFrame to GCS with down-scoped-token") {
    // Only run this test on GCS
    if ("gcp".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"))) {
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", s"select * from $test_table1 where 1 = 2")
        .load()

      // write a small DataFrame to a snowflake table
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table_write)
        // GCS_USE_DOWNSCOPED_CREDENTIAL is not a public parameter, user can't set it.
        // The default value of GCS_USE_DOWNSCOPED_CREDENTIAL will be true from Dec 2023
        // The option set can be removed after Dec 2023
        .option("GCS_USE_DOWNSCOPED_CREDENTIAL", "true")
        .mode(SaveMode.Overwrite)
        .save()

      // Check the source table and target table has same agg_hash.
      assert(getRowCount(test_table_write) == 0)
    } else {
      println("skip test for non-GCS platform: " +
        "write a empty DataFrame to GCS with down-scoped-token")
    }
  }

  // GCS_USE_DOWNSCOPED_CREDENTIAL is not a public parameter, user can't set it.
  // Only the snowflake test account can set it for testing purpose.
  // From Dec 2023, GCS_USE_DOWNSCOPED_CREDENTIAL may be configured as true for all deployments
  // and this test case can be removed at that time.
  test("write a small DataFrame to GCS with presigned-url (Can be removed by Dec 2023)") {
    // Only run this test on GCS
    if ("gcp".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"))) {
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table1)
        .load()

      // write a small DataFrame to a snowflake table
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table_write)
        // GCS_USE_DOWNSCOPED_CREDENTIAL is not a public parameter, user can't set it.
        // The default value of GCS_USE_DOWNSCOPED_CREDENTIAL will be true from Dec 2023
        // The option set can be removed after Dec 2023
        .option("GCS_USE_DOWNSCOPED_CREDENTIAL", "false")
        .mode(SaveMode.Overwrite)
        .save()

      // Check the source table and target table has same agg_hash.
      assert(getHashAgg(test_table1) == getHashAgg(test_table_write))
    } else {
      println("skip test for non-GCS platform: " +
        "write a small DataFrame to GCS with presigned-url (Can be removed by Dec 2023)")
    }
  }

  // GCS_USE_DOWNSCOPED_CREDENTIAL is not a public parameter, user can't set it.
  // Only the snowflake test account can set it for testing purpose.
  // From Dec 2023, GCS_USE_DOWNSCOPED_CREDENTIAL may be configured as true for all deployments
  // and this test case can be removed at that time.
  test("write a big DataFrame to GCS with presigned-url (Can be removed by Dec 2023)") {
    // Only run this test on GCS
    if ("gcp".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"))) {
      setupLargeResultTable
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("partition_size_in_mb", 1) // generate multiple partitions
        .option("dbtable", test_table_large_result)
        .load()

      // write a small DataFrame to a snowflake table
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table_write)
        // GCS_USE_DOWNSCOPED_CREDENTIAL is not a public parameter, user can't set it.
        // The default value of GCS_USE_DOWNSCOPED_CREDENTIAL will be true from Dec 2023
        // The option set can be removed after Dec 2023
        .option("GCS_USE_DOWNSCOPED_CREDENTIAL", "false")
        .mode(SaveMode.Overwrite)
        .save()

      // Check the source table and target table has same agg_hash.
      assert(getHashAgg(test_table_large_result) == getHashAgg(test_table_write))
    } else {
      println("skip test for non-GCS platform: " +
        "write a big DataFrame to GCS with presigned-url (Can be removed by Dec 2023)")
    }
  }

  // GCS_USE_DOWNSCOPED_CREDENTIAL is not a public parameter, user can't set it.
  // Only the snowflake test account can set it for testing purpose.
  // From Dec 2023, GCS_USE_DOWNSCOPED_CREDENTIAL may be configured as true for all deployments
  // and this test case can be removed at that time.
  test("write a empty DataFrame to GCS with presigned-url (Can be removed by Dec 2023)") {
    // Only run this test on GCS
    if ("gcp".equals(System.getenv("SNOWFLAKE_TEST_ACCOUNT"))) {
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", s"select * from $test_table1 where 1 = 2")
        .load()

      // write a small DataFrame to a snowflake table
      df.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table_write)
        // GCS_USE_DOWNSCOPED_CREDENTIAL is not a public parameter, user can't set it.
        // The default value of GCS_USE_DOWNSCOPED_CREDENTIAL will be true from Dec 2023
        // The option set can be removed after Dec 2023
        .option("GCS_USE_DOWNSCOPED_CREDENTIAL", "false")
        .mode(SaveMode.Overwrite)
        .save()

      // Check the source table and target table has same agg_hash.
      assert(getRowCount(test_table_write) == 0)
    } else {
      println("skip test for non-GCS platform: " +
        "write a empty DataFrame to GCS with presigned-url (Can be removed by Dec 2023)")
    }
  }
}
// scalastyle:on println
