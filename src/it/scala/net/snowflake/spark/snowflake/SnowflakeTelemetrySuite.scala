package net.snowflake.spark.snowflake

import java.nio.file.Files

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode
import net.snowflake.client.jdbc.telemetry.Telemetry
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.SparkEnv
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

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
    val arrayNode = metric.putArray("libraries")
    val libraries = SnowflakeTelemetry.getSparkLibraries
    assert(libraries.nonEmpty)
    libraries.foreach(arrayNode.add)
    assert(arrayNode.size() == libraries.size)
  }

  test("unit test: SnowflakeTelemetry.addSparkClusterStatistics") {
    val metric: ObjectNode = mapper.createObjectNode()
    SnowflakeTelemetry.addSparkClusterStatistics(metric)
    assert(metric.get("spark_default_parallelism").asInt() == 1)
    assert(metric.get("cluster_node_count").asInt() == 1)
    assert(metric.get("deploy_mode").asText().equals("client"))
  }

  test("SnowflakeTelemetry.getSparkDependencies") {
    // by default, dependencies is empty
    assert(SnowflakeTelemetry.getSparkDependencies.isEmpty)
    // Enable dummy sending telemetry message.
    val messageBuffer = mutable.ArrayBuffer[ObjectNode]()
    val oldSender = SnowflakeTelemetry.setTelemetryMessageSenderForTest(
      new MockTelemetryMessageSender(messageBuffer))
    try {
      sparkSession.stop()

      val pythonTestFile = System.getProperty("user.dir") + "/src/test/python/unittest.py"
      val dummyArchiveFile = System.getProperty("user.dir") +
        "/ClusterTest/src/main/python/ClusterTest.py#environment"

      sparkSession = SparkSession.builder
        .master("local")
        .appName("SnowflakeSourceSuite")
        .config("spark.sql.shuffle.partitions", "6")
        // "spark.sql.legacy.timeParserPolicy = LEGACY" is added to allow
        // spark 3.0 to support legacy conversion for unix_timestamp().
        // It may not be necessary for spark 2.X.
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.files", pythonTestFile)
        .config("spark.archives", dummyArchiveFile)
        .getOrCreate()

      // unit test
      val metric: ObjectNode = mapper.createObjectNode()
      val arrayNode = metric.putArray("dependencies")
      val dependencies = SnowflakeTelemetry.getSparkDependencies
      assert(dependencies.length == 2)
      assert(dependencies.contains(pythonTestFile))
      assert(dependencies.contains(dummyArchiveFile))

      // Integration test
      // A basis dataframe read
      val df1 = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("query", "select current_timestamp()")
        // Disable PARAM_SUPPORT_SHARE_CONNECTION to make sure spark_client_info is sent for test
        .option(Parameters.PARAM_SUPPORT_SHARE_CONNECTION, false)
        .load()
      df1.collect()

      val clientInfoMessages = messageBuffer
        .filter(_.get("type").asText().equals("spark_client_info"))
      assert(clientInfoMessages.nonEmpty)
      clientInfoMessages.foreach { x =>
        val sparkDependencies = x.get("data").get(TelemetryFieldNames.DEPENDENCIES)
        assert(sparkDependencies.isArray && sparkDependencies.size() == 2)
        assert(nodeContains(sparkDependencies, pythonTestFile))
        assert(nodeContains(sparkDependencies, dummyArchiveFile))
      }
    } finally {
      // reset default SparkSession
      sparkSession.stop()
      sparkSession = createDefaultSparkSession
      // Reset to the real Telemetry message sender
      SnowflakeTelemetry.setTelemetryMessageSenderForTest(oldSender)
    }
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
          // Spark cluster node/executor count and deployMode are added
          assert(data.get("spark_default_parallelism").asInt() == 1)
          assert(data.get("cluster_node_count").asInt() == 1)
          assert(data.get("deploy_mode").asText().equals("client"))
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
        // Disable PARAM_SUPPORT_SHARE_CONNECTION to make sure spark_client_info is sent for test
        .option(Parameters.PARAM_SUPPORT_SHARE_CONNECTION, false)
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
      }
    } finally {
      // Reset to the real Telemetry message sender
      SnowflakeTelemetry.setTelemetryMessageSenderForTest(oldSender)
    }
  }

  // Create this test case from a simple Spark ML example
  // https://spark.apache.org/docs/latest/ml-statistics.html
  test("IT test: add spark libraries to SPARK_CLIENT_INFO (Spark ML)") {
    // Enable dummy sending telemetry message.
    val messageBuffer = mutable.ArrayBuffer[ObjectNode]()
    val oldSender = SnowflakeTelemetry.setTelemetryMessageSenderForTest(
      new MockTelemetryMessageSender(messageBuffer))
    try {
      jdbcUpdate(s"create or replace table $test_table (c1 double, c2 double, c3 double)")
      jdbcUpdate(
        s"""insert into $test_table values
           | (0.0, 0.5, 10.0),
           | (0.0, 1.5, 20.0),
           | (1.0, 1.5, 30.0),
           | (0.0, 3.5, 30.0),
           | (0.0, 3.5, 40.0),
           | (1.0, 3.5, 40.0) """.stripMargin)
      val dfTable = sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        // Disable PARAM_SUPPORT_SHARE_CONNECTION to make sure spark_client_info is sent for test
        .option(Parameters.PARAM_SUPPORT_SHARE_CONNECTION, false)
        .option("dbtable", test_table)
        .load()

      // Using with scala udf DataFrame
      val toVector = (firstValue: Double, otherValues: Double) =>
        Vectors.dense(firstValue, otherValues)
      val toVectorUDF = udf(toVector)

      val df = dfTable.select(col("c1").as("label"),
        toVectorUDF(col("c2"), col("c3")).as("features"))

      val chi = ChiSquareTest.test(df, "features", "label").head
      // scalastyle:off println
      println(s"pValues = ${chi.getAs[Vector](0)}")
      println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
      println(s"statistics ${chi.getAs[Vector](2)}")
      // scalastyle:on println

      val planStatisticMessages = messageBuffer
        .filter(_.get("type").asText().equals("spark_client_info"))
        .filter { x =>
          val planStatistics = x.get("data").get(TelemetryFieldNames.LIBRARIES)
          nodeContains(planStatistics, "org.apache.spark.ml.stat")
        }
      assert(planStatisticMessages.nonEmpty)
    } finally {
      // Some spark plans are cached in SnowflakeTelemetry.logs, but not sent yet.
      // Perform a DataFrame read to clear them to avoid affecting next test case.
      sparkSession.read
        .format(SNOWFLAKE_SOURCE_NAME)
        .options(connectorOptionsNoTable)
        .option("dbtable", test_table)
        .load()
        .count()
      // Reset to the real Telemetry message sender
      SnowflakeTelemetry.setTelemetryMessageSenderForTest(oldSender)
    }
  }

  test("IT test: CLIENT_INFO: shared connection") {
    // close all connections to trigger create JDBC connections
    ServerConnection.closeAllCachedConnections
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
      val clientInfoMessages = messageBuffer
        .filter(_.get("type").asText().equals("spark_client_info"))
      assert(clientInfoMessages.nonEmpty)
      clientInfoMessages.foreach { x =>
        val shared = x.get("data").get(TelemetryFieldNames.SHARED)
        assert(shared.asBoolean())
      }
    } finally {
      // Reset to the real Telemetry message sender
      SnowflakeTelemetry.setTelemetryMessageSenderForTest(oldSender)
    }
  }

  // To run this test manually, need to start postgresql server
  ignore("manual test: read from postgresql with JDBC") {
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
    }
  }

}
