package net.snowflake.spark.snowflake_beta.streaming

import net.snowflake.spark.snowflake_beta.{ConstantString, IntegrationSuiteBase}
import net.snowflake.spark.snowflake_beta.DefaultJDBCWrapper.DataBaseOperations

class SinkUtilsSuite extends IntegrationSuiteBase{

  val pipeName = s"test_pipe_$randomSuffix"
  val stageName = s"test_stage_$randomSuffix"
  val tableName = s"test_table_$randomSuffix"

  override def beforeAll(): Unit = {
    super.beforeAll()
    jdbcUpdate(s"create table $tableName(num int)")
    jdbcUpdate(s"create stage $stageName")
  }

  override def afterAll(): Unit = {
    jdbcUpdate(s"drop pipe if exists $pipeName")
    jdbcUpdate(s"drop stage if exists $stageName")
    jdbcUpdate(s"drop table if exists $tableName")
    super.afterAll()
  }

  test("verifyPipe"){

    val copy = s"copy into $tableName from @$stageName"
    conn.createPipe(pipeName, ConstantString(copy) !, true )

    assert(verifyPipe(conn, pipeName, copy))

    assert(!verifyPipe(conn, pipeName, copy + "something"))
  }

}
