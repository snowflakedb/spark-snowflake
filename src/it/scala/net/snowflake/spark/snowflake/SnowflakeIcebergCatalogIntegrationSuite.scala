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

import net.snowflake.spark.snowflake.Utils.{SNOWFLAKE_SOURCE_NAME, SNOWFLAKE_SOURCE_SHORT_NAME}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row, SaveMode}

import java.sql.SQLException

/**
  * End-to-end tests which run against a real Snowflake cluster.
  */
class SnowflakeIcebergCatalogIntegrationSuite extends IntegrationSuiteBase {

  // test_table_tmp should be used inside of one test function.
  private val test_table_tmp: String = s"test_table_tmp_$randomSuffix"
  private val test_table: String = s"test_table_$randomSuffix"
  private val test_table2: String = s"test_table2_$randomSuffix"
  private val test_table3: String = s"test_table3_$randomSuffix"

  test("Test basic catalog") {
    sparkSession.sessionState.catalogManager.setCurrentCatalog("snowlog");
    val rdd1 = sparkSession.sql("show databases");
    assert(rdd1.count() == 20);
  }

}
