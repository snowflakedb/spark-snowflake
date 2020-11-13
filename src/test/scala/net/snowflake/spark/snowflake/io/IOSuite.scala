/*
 * Copyright 2015-2020 Snowflake Computing
 * Copyright 2015 TouchType Ltd
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

package net.snowflake.spark.snowflake.io

import org.scalatest.{FunSuite, Matchers}

class IOSuite extends FunSuite with Matchers {

  test("test generate stage table name") {
    // Original table name --> database_schema
    // NOTE: in this context, NORMAL means that the name doesn't include '.'
    val tableNamePair = Seq (
      // normal table name
      ("test_table", None),
      ("test_table!@#$%^&^", None),
      // normal table name with normal schema or database
      ("My_Schema.Table_1", Option("My_Schema.")),
      ("My_DB.My_Schema.Table_1", Option("My_DB.My_Schema.")),
      // normal table name with abnormal schema or database name
      ("\"My.Schema\".Table_1", Option("\"My.Schema\".")),
      ("\"My.DB\".\"My.Schema\".Table_1", Option("\"My.DB\".\"My.Schema\".")),
      // abnormal table name with abnormal schema or database name
      ("\"test_table_.'!@#$%^&* 2611611920364743726\"", None),
      ("\"Table.1\"", None),
      ("\"My.Schema\".\"Table.1\"", Option("\"My.Schema\".")),
      ("\"My.DB\".\"My.Schema\".\"Table.1\"", Option("\"My.DB\".\"My.Schema\".")),
      // mixed
      ("My_Schema.\"Table.1\"", Option("My_Schema.")),
      ("My_DB.\"My.Schema\".\"Table.1\"", Option("My_DB.\"My.Schema\".")),
      ("\"My.DB\".My_Schema.\"Table.1\"", Option("\"My.DB\".My_Schema."))
    )

    val debugPrint = true
    tableNamePair.foreach(
      pair => {
        val autoGeneratePrefix = "spark_stage_table_"
        val stageTableName = StageWriter.getStageTableName(pair._1)
        assert(stageTableName.startsWith(s"${pair._2.getOrElse("")}$autoGeneratePrefix"))
        if (debugPrint) {
          println(s"${pair._1}  --->  $stageTableName")
          println("======================================================")
        }
      }
    )
  }
}
