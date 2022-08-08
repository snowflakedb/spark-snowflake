package net.snowflake.spark.snowflake

import java.nio.file.Files

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.client.jdbc.telemetry.Telemetry
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.SparkEnv
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

import scala.collection.mutable
import scala.reflect.io.Directory

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

  private def nodeContains(node: JsonNode, value: String): Boolean = {
    val iterator = node.iterator()
    while (iterator.hasNext) {
      if (iterator.next().asText().equals(value)) return true
    }
    false
  }

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

  test("unit test: SnowflakeTelemetry.getSparkLibraries") {
    val metric: ObjectNode = mapper.createObjectNode()
    val arrayNode = metric.putArray("spark_libraries")
    val libraries = SnowflakeTelemetry.getSparkLibraries
    assert(libraries.nonEmpty)
    libraries.foreach(arrayNode.add)
    assert(arrayNode.size() == libraries.size)
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
        .mode(SaveMode.Overwrite)
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

  test("IT test: add spark libraries to SPARK_CLIENT_INFO") {
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

      val clientInfoMessages = messageBuffer
        .filter(_.get("type").asText().equals("spark_client_info"))
      assert(clientInfoMessages.nonEmpty)
      clientInfoMessages.foreach { x =>
        val sparkLibraries = x.get("data").get(TelemetryFieldNames.LIBRARIES)
        assert(sparkLibraries.isArray && sparkLibraries.size() > 0)
        assert(nodeContains(sparkLibraries, "org.scalatest"))
        assert(nodeContains(sparkLibraries, "org.apache.spark.sql"))
      }
    } finally {
      // Reset to the real Telemetry message sender
      SnowflakeTelemetry.setTelemetryMessageSenderForTest(oldSender)
    }
  }

  test("IT test: PLAN_STATISTIC: read from snowflake") {
    // Enable dummy sending telemetry message.
    val messageBuffer = mutable.ArrayBuffer[ObjectNode]()
    val oldSender = SnowflakeTelemetry.setTelemetryMessageSenderForTest(
      new MockTelemetryMessageSender(messageBuffer))
    try {
      // A basis dataframe read
      val df1 = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", "select 123 as A")
        .load()
      df1.collect()

      val planStatisticMessages = messageBuffer
        .filter(_.get("type").asText().equals("spark_plan_statistic"))
      assert(planStatisticMessages.nonEmpty)
      planStatisticMessages.foreach { x =>
        val planStatistics = x.get("data").get(TelemetryFieldNames.STATISTIC_INFO)
        assert(planStatistics.isArray && planStatistics.size() > 0)
        assert(nodeContains(planStatistics, "LogicalRelation:SnowflakeRelation"))
        assert(!nodeContains(planStatistics, "SaveIntoDataSourceCommand:DefaultSource"))
      }
    } finally {
      // Reset to the real Telemetry message sender
      SnowflakeTelemetry.setTelemetryMessageSenderForTest(oldSender)
    }
  }

  test("IT test: PLAN_STATISTIC: read from snowflake and write to snowflake") {
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

      df1.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table)
        .mode(SaveMode.Overwrite)
        .save()

      val planStatisticMessages = messageBuffer
        .filter(_.get("type").asText().equals("spark_plan_statistic"))
        .filter { x =>
          val planStatistics = x.get("data").get(TelemetryFieldNames.STATISTIC_INFO)
          assert(planStatistics.isArray && planStatistics.size() > 0)
          nodeContains(planStatistics, "LogicalRelation:SnowflakeRelation") &&
            nodeContains(planStatistics, "SaveIntoDataSourceCommand:DefaultSource")
        }
      assert(planStatisticMessages.length == 1)
    } finally {
      // Reset to the real Telemetry message sender
      SnowflakeTelemetry.setTelemetryMessageSenderForTest(oldSender)
    }
  }

  test("IT test: PLAN_STATISTIC: snowflake -> file, file -> snowflake") {
    // Enable dummy sending telemetry message.
    val messageBuffer = mutable.ArrayBuffer[ObjectNode]()
    val oldSender = SnowflakeTelemetry.setTelemetryMessageSenderForTest(
      new MockTelemetryMessageSender(messageBuffer))
    val tempDir = Files.createTempDirectory("spark_connector_test").toFile
    try {
      // A basis dataframe read
      val df1 = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", "select 123 as A")
        .load()
      // write to a CSV file
      df1.write.mode("Overwrite").csv(tempDir.getPath)

      // Check messages for: Snowflake -> file
      val planStatisticMessages = messageBuffer
        .filter(_.get("type").asText().equals("spark_plan_statistic"))
        .filter { x =>
          val planStatistics = x.get("data").get(TelemetryFieldNames.STATISTIC_INFO)
          assert(planStatistics.isArray && planStatistics.size() > 0)
          nodeContains(planStatistics, "LogicalRelation:SnowflakeRelation") &&
            nodeContains(planStatistics,
              "org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand")
        }
      assert(planStatisticMessages.length == 1)

      // Clear message buffer for next test
      messageBuffer.clear()

      val df2 = sparkSession.read.csv(tempDir.getPath)
      df2.write
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table)
        .mode(SaveMode.Overwrite)
        .save()
      // Check messages for: file -> Snowflake
      val planStatisticMessages2 = messageBuffer
        .filter(_.get("type").asText().equals("spark_plan_statistic"))
        .filter { x =>
          val planStatistics = x.get("data").get(TelemetryFieldNames.STATISTIC_INFO)
          assert(planStatistics.isArray && planStatistics.size() > 0)
          nodeContains(planStatistics, "SaveIntoDataSourceCommand:DefaultSource") &&
            nodeContains(planStatistics,
              "LogicalRelation:org.apache.spark.sql.execution.datasources.HadoopFsRelation")
        }
      assert(planStatisticMessages2.length == 1)
    } finally {
      // Reset to the real Telemetry message sender
      SnowflakeTelemetry.setTelemetryMessageSenderForTest(oldSender)
      // Remove temp directory
      new Directory(tempDir).deleteRecursively()
    }
  }

  test("IT test: PLAN_STATISTIC: Use Scala UDF") {
    // Enable dummy sending telemetry message.
    val messageBuffer = mutable.ArrayBuffer[ObjectNode]()
    val oldSender = SnowflakeTelemetry.setTelemetryMessageSenderForTest(
      new MockTelemetryMessageSender(messageBuffer))
    try {
      // A basis dataframe read
      val df1 = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", "select 123 as A")
        .load()

      // Using with scala udf DataFrame
      val doubler = (value: Int) => value + value

      val doublerUDF = udf(doubler)
      df1.select(col("A"), doublerUDF(col("A")).as("A2")).collect()

      val planStatisticMessages = messageBuffer
        .filter(_.get("type").asText().equals("spark_plan_statistic"))
      assert(planStatisticMessages.nonEmpty)
      planStatisticMessages.foreach { x =>
        val planStatistics = x.get("data").get(TelemetryFieldNames.STATISTIC_INFO)
        assert(planStatistics.isArray && planStatistics.size() > 0)
        // Scala UDF is used
        assert(nodeContains(planStatistics, "org.apache.spark.sql.catalyst.expressions.ScalaUDF"))
        assert(nodeContains(planStatistics, "LogicalRelation:SnowflakeRelation"))
      }
    } finally {
      // Reset to the real Telemetry message sender
      SnowflakeTelemetry.setTelemetryMessageSenderForTest(oldSender)
    }
  }

  // To run this test manually, need to start postgresql server
  ignore("manual test: read from postgresql with JDBC") {
    SnowflakeConnectorUtils.enablePushdownSession(sparkSession)
    try {
      val jdbcDF = sparkSession.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost/mrui?user=mrui&password=password")
        .option("query", "select 123")
        .load()

      jdbcDF.collect()
    } catch {
      case _: Throwable =>
    }
  }

  // To run this test manually, need to start postgresql server
  ignore("manual test: write to postgresql with JDBC") {
    SnowflakeConnectorUtils.enablePushdownSession(sparkSession)
    try {
      val df = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", "select 123 as A")
        .load()

      // Saving data to a JDBC source
      df.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://localhost/mrui?user=mrui&password=password")
        .option("dbtable", "mrui.t1")
        .save()
    } catch {
      case _: Throwable =>
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
