package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.spark.snowflake.SnowflakeTelemetry.mapper
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.SparkEnv
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.util.Random

class SnowflakeTelemetrySuite extends IntegrationSuiteBase {

  private val test_table: String = s"test_table_1_$randomSuffix"
  private val test_table2: String = s"test_table_2_$randomSuffix"

  private val mapper = new ObjectMapper

  override def beforeAll(): Unit = {
    super.beforeAll()

    SnowflakeConnectorUtils.enablePushdownSession(sparkSession)
  }

  test("unit test: SnowflakeTelemetry.detectSparkLanguage") {
    val sparkConf = SparkEnv.get.conf.clone()
    assert(SnowflakeTelemetry.detectSparkLanguage(sparkConf).equals("Scala"))
    // Manually set pyspark
    sparkConf.set("spark.pyspark.python", "/usr/bin/python3")
    assert(SnowflakeTelemetry.detectSparkLanguage(sparkConf).equals("python3"))
    // Manually set R language
    sparkConf.set("spark.r.command", "R")
    assert(SnowflakeTelemetry.detectSparkLanguage(sparkConf).equals("R"))
  }

  test("unit test: SnowflakeTelemetry.addCommonFields") {
    val metric: ObjectNode = mapper.createObjectNode()
    SnowflakeTelemetry.addCommonFields(metric)
    assert(metric.size() == 1 &&
      metric.get(TelemetryFieldNames.SPARK_APPLICATION_ID).asText().startsWith("local-"))
  }

  test("IT test: common fields are added") {
    try {
      // Enable dummy sending telemetry message.
      val fakeMessageSender = mutable.ArrayBuffer[ObjectNode]()
      SnowflakeTelemetry.setFakeMessageSender(fakeMessageSender)

      // A basis dataframe read
      val df1 = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", "select current_timestamp()")
        .load()
      df1.collect()

      fakeMessageSender.foreach { x =>
        val typeName = x.get("type").asText()
        val source = x.get("source").asText()
        assert(source.equals("spark_connector"))
        val data = x.get("data")
        assert(data.get(TelemetryFieldNames.SPARK_APPLICATION_ID).asText().startsWith("local-"))
        if (typeName.equals("spark_client_info")) {
          // Spark language is added for spark_client_info
          assert(data.get(TelemetryFieldNames.SPARK_LANGUAGE).asText().equals("Scala"))
        }
      }
    } finally {
      // Reset to send telemetry normally
      SnowflakeTelemetry.setFakeMessageSender(null)
    }
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table")
      jdbcUpdate(s"drop table if exists $test_table2")
    } finally {
      super.afterAll()
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
    }
  }

}
