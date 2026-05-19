package net.snowflake.spark.snowflake

import org.apache.spark.sql.{SQLContext, SQLImplicits}

/** Spark 3.x: [[SQLImplicits]] is wired via [[SQLContext]]. */
trait IntegrationSuiteTestImplicits {
  self: IntegrationEnv with QueryTest =>

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = sparkSession.sqlContext
  }
}
