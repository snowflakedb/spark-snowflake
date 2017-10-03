package net.snowflake.spark.snowflake

import java.sql.Connection

import net.snowflake.spark.snowflake.Parameters.MergedParameters
import org.apache.spark.sql.SQLContext

/**
  * Created by ema on 3/28/17.
  */
private[snowflake] trait DataUnloader {

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

      Utils.executePreActions(jdbcWrapper, conn, params)

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

      Utils.executePostActions(jdbcWrapper, conn, params)
      numRows
    } finally {
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
    Utils.setLastSelect(query)

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
