package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.client.jdbc.telemetry.Telemetry
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.SparkEnv
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types._

import scala.collection.mutable
import scala.util.Random

private[snowflake] class MockTelemetryMessageSender(buffer: mutable.ArrayBuffer[ObjectNode])
  extends TelemetryMessageSender {
  // the telemetry messages are appended into the buffer instead of sending to snowflake,
  // So that the test case can check the message to be sent.
  override def send(telemetry: Telemetry, logs: List[(ObjectNode, Long)]): Unit = {
    logs.foreach {
      case (log, _) => buffer.append(log)
    }
  }
}

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
    assert(SnowflakeTelemetry.detectSparkLanguage(sparkConf).equals("Python"))
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
    // Enable dummy sending telemetry message.
    val messageBuffer = mutable.ArrayBuffer[ObjectNode]()
    val oldSender = SnowflakeTelemetry.setTelemetryMessageSenderForTest(
      new MockTelemetryMessageSender(messageBuffer))
    try {
      // A basis dataframe read
      val df1 = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", "select current_timestamp()")
        .load()
      df1.collect()

      messageBuffer.foreach { x =>
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
      // Reset to the real Telemetry message sender
      SnowflakeTelemetry.setTelemetryMessageSenderForTest(oldSender)
    }
  }

  test("IT test: egress and ingress message") {
    // Enable dummy sending telemetry message.
    val messageBuffer = mutable.ArrayBuffer[ObjectNode]()
    val oldSender = SnowflakeTelemetry.setTelemetryMessageSenderForTest(
      new MockTelemetryMessageSender(messageBuffer))
    try {
      // A basis dataframe read
      val df1 = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", "select seq4(), 'test_data' from table(generator(rowcount => 100))")
        .load()

      df1.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table)
        .save()

      // Check SPARK_EGRESS to be sent
      assert(messageBuffer.count(_.get("type").asText().equals("spark_egress")) == 1)
      val egressMessage = messageBuffer.filter(_.get("type").asText().equals("spark_egress")).head
      assert(egressMessage.get("data").get("row_count").asLong() == 100)
      assert(egressMessage.get("data").get("output_bytes").asLong() > 0)
      assert(egressMessage.get("data").get("query_id").asText().nonEmpty)
      assert(egressMessage.get("data").get("spark_application_id").asText().nonEmpty)

      // Check SPARK_INGRESS to be sent
      assert(messageBuffer.count(_.get("type").asText().equals("spark_ingress")) == 1)
      val ingressMessage = messageBuffer.filter(_.get("type").asText().equals("spark_ingress")).head
      assert(ingressMessage.get("data").get("row_count").asLong() == 100)
      assert(ingressMessage.get("data").get("input_bytes").asLong() > 0)
      assert(ingressMessage.get("data").get("query_id").asText().nonEmpty)
      assert(ingressMessage.get("data").get("spark_application_id").asText().nonEmpty)
    } finally {
      // Reset to the real Telemetry message sender
      SnowflakeTelemetry.setTelemetryMessageSenderForTest(oldSender)
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
