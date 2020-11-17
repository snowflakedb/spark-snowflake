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
    // Original table name (String), is_table_name_quoted (boolean)
    // NOTE: in this context, NORMAL means that the name doesn't include '.'
    val tableNamePair = Seq (
      // normal table name
      ("test_table", false),
      ("test_table!@#$%^&^", false),
      // normal table name with normal schema or database
      ("My_Schema.Table_1", false),
      ("My_DB.My_Schema.Table_1", false),
      // normal table name with abnormal schema or database name
      ("\"My.Schema\".Table_1", false),
      ("\"My.DB\".\"My.Schema\".Table_1",false),
      // abnormal table name with abnormal schema or database name
      ("\"test_table_.'!@#$%^&* 2611611920364743726\" ", true),
      ("\"Table.1\"", true),
      ("\"My.Schema\".\"Table.1\"", true),
      ("\"My.Schema\".\"Table.1\" ", true), // Extra space in the name
      ("\"My.DB\".\"My.Schema\".\"Table.1\"", true),
      // mixed
      ("My_Schema.\"Table.1\"", true),
      ("My_DB.\"My.Schema\".\"Table.1\"", true),
      ("\"My.DB\".My_Schema.\"Table.1\"", true),
      // table name may include QUOTE
      ("My_Schema.\"Table\"\"1\"", true),
      ("\"My\"\"Schema\".\"Table\"\"\"\"2\"", true),
      ("My_DB.\"My.Schema\".\"Table\"\"1\"", true),
      ("My_DB.\"My\"\"Schema\".\"Table\"\"\"\"2\"", true),
      ("My_DB.\"My\"\"Schema\".\"Table\"\"2\"\"\"", true)
    )

    val debugPrint = true
    tableNamePair.foreach(
      pair => {
        val stagePostfix = "_staging_"
        val tableName = pair._1
        val stageTableName = StageWriter.getStageTableName(tableName)
        if (pair._2) {
          assert(stageTableName.startsWith(
            s"${tableName.substring(0, tableName.lastIndexOf("\""))}$stagePostfix"))
          assert(stageTableName.endsWith("\""))
        } else {
          assert(stageTableName.startsWith(s"${tableName.trim}$stagePostfix"))
          assert(!stageTableName.endsWith("\""))
        }
        if (debugPrint) {
          println(s"${pair._1}  --->  $stageTableName")
        }
      }
    )
  }
}
