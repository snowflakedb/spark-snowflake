/*
 * Copyright 2015-2018 Snowflake Computing
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

/**
  * Wrapper class for representing the name of a Snowflake table.
  *
  * Note, we don't do any escaping/unescaping for Snowflake tables,
  * we expect the user to do it.
  */
private[snowflake] case class TableName(name: String) {
  override def toString: String = name

  def toStatement: Identifier = Identifier(name)
}
