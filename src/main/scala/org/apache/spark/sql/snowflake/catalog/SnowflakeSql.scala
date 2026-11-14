/*
 * Copyright 2015-2025 Snowflake Computing
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
package org.apache.spark.sql.snowflake.catalog

import net.snowflake.spark.snowflake.Utils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.ArrayBuffer

/** The Snowflake statements a Snowflake-backed Iceberg table runs; stubbed in unit tests. */
private[catalog] trait SnowflakeSql {
  def execute(sql: String): Unit = query(sql)

  /** Rows as strings (null stays null). */
  def query(sql: String): Seq[Seq[String]]

  /** The columns Snowflake returns for a read with these connector parameters. */
  def schema(readParams: Map[String, String]): StructType =
    new FallbackDefaultSource().createRelation(SparkSession.active.sqlContext, readParams).schema
}

/** Runs statements over JDBC with the catalog's connection parameters. */
private[catalog] class JdbcSnowflakeSql(params: Map[String, String]) extends SnowflakeSql {
  override def query(sql: String): Seq[Seq[String]] = {
    val rs = Utils.runQuery(params, sql)
    try {
      val columns = rs.getMetaData.getColumnCount
      val rows = ArrayBuffer[Seq[String]]()
      while (rs.next()) rows += (1 to columns).map(rs.getString)
      rows.toList
    } finally {
      rs.close()
    }
  }
}
