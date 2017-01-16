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

import org.apache.spark.sql.Row

import Utils.SNOWFLAKE_SOURCE_NAME

/**
 * Integration tests for decimal support.
 * For a reference on Snowflake's DECIMAL type, see
 * https://docs.snowflake.net/manuals/sql-reference/data-types.html
 */
class DecimalIntegrationSuite extends IntegrationSuiteBase {

  private def testReadingDecimals(precision: Int, scale: Int, decimalStrings: Seq[String]): Unit = {
    test(s"reading DECIMAL($precision, $scale)") {
      val tableName = s"reading_decimal_${precision}_${scale}_$randomSuffix"
      val expectedRows =
        decimalStrings.map(d => Row(if (d == null) null else Conversions.parseDecimal(d, false)))
      try {
        conn.createStatement().executeUpdate(
          s"CREATE TABLE $tableName (x DECIMAL($precision, $scale))")
        for (x <- decimalStrings) {
          conn.createStatement().executeUpdate(s"INSERT INTO $tableName VALUES ($x)")
        }
        conn.commit()
        assert(DefaultJDBCWrapper.tableExists(conn, tableName))
        val loadedDf = sqlContext.read
          .format(SNOWFLAKE_SOURCE_NAME)
          .options(connectorOptions)
          .option("dbtable", tableName)
          .load()
        checkAnswer(loadedDf, expectedRows)
        checkAnswer(loadedDf.selectExpr("x + 0"), expectedRows)
      } finally {
        conn.prepareStatement(s"drop table if exists $tableName").executeUpdate()
        conn.commit()
      }
    }
  }

  testReadingDecimals(19, 0, Seq(
     "9223372036854775807", // 2^63 - 1
    "-9223372036854775807",
     "9999999999999999999",
    "-9999999999999999999",
    "0",
    "12345678910",
    null
  ))

  testReadingDecimals(19, 4, Seq(
     "922337203685477.5807",
    "-922337203685477.5807",
     "999999999999999.9999",
    "-999999999999999.9999",
    "0",
    "1234567.8910",
    null
  ))

  testReadingDecimals(38, 4, Seq(
     "922337203685477.5808",
     "9999999999999999999999999999999999.9999",
    "-9999999999999999999999999999999999.9999",
    "0",
    "1234567.8910",
    null
  ))
}
