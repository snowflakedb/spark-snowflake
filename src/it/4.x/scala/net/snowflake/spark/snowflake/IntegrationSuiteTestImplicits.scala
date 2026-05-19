package net.snowflake.spark.snowflake

import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}

/** Spark 4.x: classic [[org.apache.spark.sql.classic.SQLImplicits]] requires [[ClassicSparkSession]]. */
trait IntegrationSuiteTestImplicits {
  self: IntegrationEnv with QueryTest =>

  protected object testImplicits extends org.apache.spark.sql.classic.SQLImplicits {
    protected override def session: ClassicSparkSession =
      sparkSession.asInstanceOf[ClassicSparkSession]
  }
}
