package net.snowflake.spark.snowflake.iceberg

import net.snowflake.spark.snowflake.IntegrationSuiteBase

import java.sql.ResultSet

trait IcebergSuiteBase extends IntegrationSuiteBase{
  protected val test_database: String = s"test_icebergDB_$randomSuffix"
  protected val test_schema_1: String = s"test_icebergSchema1_$randomSuffix"
  protected var externalVolume: String = _
  protected var icebergOptions: Map[String, String] = _

  // Load configs for iceberg, preferring file values over env ones
  protected def loadIcebergConfig(): Map[String, String] =
    loadConfigFromFile("iceberg_config").getOrElse(configsFromFile) ++ configsFromEnv
  override def beforeAll(): Unit = {
    super.beforeAll();
    icebergOptions = loadIcebergConfig()
    externalVolume = icebergOptions.get("externalvolume").get
    sparkSession.sessionState.catalogManager.setCurrentCatalog("snowlog");
    conn.createStatement().executeUpdate(s"alter account set enable_external_volumes = true," +
      " enable_iceberg_tables = true, enable_iceberg_table_migration = true, " +
      " QA_EXTERNAL_VOLUME_DEPLOYMENT_REGION='us-west-2',QA_EXTERNAL_VOLUME_DEPLOYMENT_TYPE='AWS'");
    conn.createStatement().executeUpdate(s"CREATE DATABASE IF NOT EXISTS $test_database");
    conn.createStatement().executeUpdate(s"CREATE SCHEMA IF NOT EXISTS $test_schema_1");
  }

  override def afterAll(): Unit = {
    try {
      conn.createStatement.executeUpdate(s"drop database if exists $test_database")
    } finally {
      super.afterAll()
    }
  }

  def results[T](resultSet: ResultSet)(f: ResultSet => T): Iterator[T] = {
    new Iterator[T] {
      def hasNext = resultSet.next()

      def next() = f(resultSet)
    }
  }
}
