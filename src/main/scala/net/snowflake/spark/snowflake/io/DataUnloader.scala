/*
 * Copyright 2017 - 2018 Snowflake Computing
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

import java.sql.Connection

import net.snowflake.spark.snowflake.{JDBCWrapper, SnowflakeTelemetry, Utils}
import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.spark.sql.SQLContext

/**
  * Created by ema on 3/28/17.
  */
private[io] trait DataUnloader {

  val log: org.slf4j.Logger
  val jdbcWrapper: JDBCWrapper
  val params: MergedParameters
  val sqlContext: SQLContext

  @transient def setup(preStatements: Seq[String] = Seq.empty, sql: String, conn: Connection, keepOpen: Boolean = false)
    : Long = {
    try {
      // Prologue
      val prologueSql = Utils.genPrologueSql(params)
      log.debug(Utils.sanitizeQueryText(prologueSql))
      jdbcWrapper.executeInterruptibly(conn, prologueSql)

      Utils.executePreActions(jdbcWrapper, conn, params, params.table.get)

      // Run the unload query
      log.debug(Utils.sanitizeQueryText(sql))

      preStatements.foreach { stmt =>
        jdbcWrapper.executeInterruptibly(conn, stmt)
      }
      val res = jdbcWrapper.executeQueryInterruptibly(conn, sql)

      // Verify it's the expected format
      val sch = res.getMetaData
      assert(sch.getColumnCount == 3)
      assert(sch.getColumnName(1) == "rows_unloaded")
      assert(sch.getColumnTypeName(1) == "NUMBER") // First record must be in
      val first = res.next()
      assert(first)
      val numRows = res.getLong(1) // There can be no more records
      val second  = res.next()
      assert(!second)

      Utils.executePostActions(jdbcWrapper, conn, params, params.table.get)
      numRows
    } finally {
      SnowflakeTelemetry.send(jdbcWrapper.getTelemetry(conn))
      if (!keepOpen) conn.close()
    }
  }

  @transient
  def buildUnloadStmt(query: String,
                      location: String,
                      compression: String,
                      credentialsString: Option[String]): String = {

    val credentials = credentialsString.getOrElse("")

    // Save the last SELECT so it can be inspected
    Utils.setLastCopyUnload(query)

    /** TODO(etduwx): Refactor this to be a collection of different options, and use a mapper
    function to individually set each file_format and copy option. */

    s"""
       |COPY INTO '$location'
       |FROM ($query)
       |$credentials
       |FILE_FORMAT = (
       |    TYPE=CSV
       |    COMPRESSION='$compression'
       |    FIELD_DELIMITER='|'
       |    /*ESCAPE='\\\\'*/
       |    /*TIMESTAMP_FORMAT='YYYY-MM-DD HH24:MI:SS.FF3 TZHTZM'*/
       |    FIELD_OPTIONALLY_ENCLOSED_BY='"'
       |    NULL_IF= ()
       |  )
       |MAX_FILE_SIZE = ${params.s3maxfilesize}
       |""".stripMargin.trim
  }
}
