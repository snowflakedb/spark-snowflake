package net.snowflake.spark.snowflake.streaming

import java.io.{DataOutputStream, File}
import java.net.ServerSocket
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.io.{CloudStorage, CloudStorageOperations}
import net.snowflake.spark.snowflake.{DefaultJDBCWrapper, IntegrationSuiteBase, Utils}
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.scalactic.source.Position
import org.scalatest.Tag

import scala.util.Random

class StreamingSuite extends IntegrationSuiteBase {

  override def test(testName: String, testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    if (extraTestForCoverage) {
      super.test(testName, testTags: _*)(testFun)(pos)
    } else {
      println(s"Skip test StreamingSuite.'$testName'.")
    }
  }

  val streamingTable = s"test_table_streaming_$randomSuffix"
  val streamingStage = s"streaming_test_stage_$randomSuffix"
  // Random port number [10001-60000]
  val testServerPort = Random.nextInt(50000) + 10001

  private class NetworkService(val port: Int,
                               val data: Seq[String],
                               val sleepBeforeAll: Int = 10,
                               val sleepAfterAll: Int = 1,
                               val sleepAfterEach: Int = 5)
      extends Runnable {
    val serverSocket = new ServerSocket(port)

    def run(): Unit = {
      val socket = serverSocket.accept()
      val output = new DataOutputStream(socket.getOutputStream)
      Thread.sleep(sleepBeforeAll * 1000)
      data.foreach(d => {
        // scalastyle:off println
        println(s"send message: $d")
        s"$d\n".getBytes(Charset.forName("UTF-8")).foreach(x => output.write(x))
        // scalastyle:on println
        Thread.sleep(sleepAfterEach * 1000)
      })
      Thread.sleep(sleepAfterAll * 1000)
      socket.close()

    }
  }

  private class NetworkService2(val port: Int,
                                val times: Int = 1000,
                                val sleepBeforeAll: Int = 10,
                                val sleepAfterEach: Int = 5)
      extends Runnable {
    val serverSocket = new ServerSocket(port)

    def run(): Unit = {
      val socket = serverSocket.accept()
      val output = new DataOutputStream(socket.getOutputStream)
      Thread.sleep(sleepBeforeAll * 1000)

      (0 until times).foreach(index => {
        val word = Random.alphanumeric.take(10).mkString("")
        // scalastyle:off println
        println(s"send message: $index $word")
        // scalastyle:on println
        s"$word\n"
          .getBytes(Charset.forName("UTF-8"))
          .foreach(x => output.write(x))
        Thread.sleep(sleepAfterEach * 1000)
      })

      socket.close()

    }
  }

  def removeDirectory(dir: File): Unit = {
    if (dir.isDirectory) {
      val files = dir.listFiles
      files.foreach(removeDirectory)
      dir.delete
    } else dir.delete
  }

  def checkTestTable(expectedAnswer: Seq[Row]): Unit = {
    val loadedDf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"select * from $streamingTable order by value")
      .load()
    checkAnswer(loadedDf, expectedAnswer)
  }

  override def afterAll(): Unit = {
    conn.dropTable(streamingTable)
    conn.dropStage(streamingStage)
    super.afterAll()
  }

  // manual test only
  ignore("test") {
    val spark = sparkSession
    import spark.implicits._

    DefaultJDBCWrapper.executeQueryInterruptibly(
      conn,
      "create or replace table stream_test_table (value string)"
    )

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", testServerPort)
      .load()
    val words = lines.as[String].flatMap(_.split(" "))

    val checkpoint = "check"
    removeDirectory(new File(checkpoint))

    new Thread(
      new NetworkService2(testServerPort, sleepBeforeAll = 5, sleepAfterEach = 5)
    ).start()

    val query = words.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpoint)
      .options(connectorOptionsNoTable)
      .option("dbtable", "stream_test_table")
      .option("streaming_stage", "streaming_test_stage")
      .format(SNOWFLAKE_SOURCE_NAME)
      .start()

    query.awaitTermination()

  }

  // Wait for spark streaming write done.
  def waitForWriteDone(tableName: String,
                       expectedRowCount: Int,
                       maxWaitTimeInMs: Int,
                       intervalInMs: Int): Boolean = {
    var result = false
    Thread.sleep(intervalInMs)
    var sleepTime = intervalInMs
    while (sleepTime < maxWaitTimeInMs && !result) {
      val rs = Utils.runQuery(connectorOptionsNoExternalStageNoTable,
        query = s"select count(*) from $tableName")
      rs.next()
      val rowCount = rs.getLong(1)
      println(s"Get row count: $rowCount : retry ${sleepTime/intervalInMs}")
      if (rowCount >= expectedRowCount) {
        // Sleep done.
        result = true
      } else {
        sleepTime += intervalInMs
        Thread.sleep(intervalInMs)
      }
    }
    result
  }

  test("Test streaming writer") {

    val spark = sparkSession
    import spark.implicits._

    conn.createStage(name = streamingStage, overwrite = true)

    DefaultJDBCWrapper.executeQueryInterruptibly(
      conn,
      s"create or replace table $streamingTable (value string)"
    )

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", testServerPort)
      .load()
    val words = lines.as[String].flatMap(_.split(" "))

    val output = words.toDF("VALUE")

    val checkpoint = "check"
    removeDirectory(new File(checkpoint))

    new Thread(
      new NetworkService(
        testServerPort,
        Seq(
          "one two",
          "three four five",
          "six seven eight night ten",
          "1 2 3 4 5",
          "6 7 8 9 0"
        ),
        sleepBeforeAll = 10,
        sleepAfterAll = 1,
        sleepAfterEach = 1
      )
    ).start()

    val query = output.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpoint)
      .options(connectorOptionsNoExternalStageNoTable)
      .option("dbtable", streamingTable)
      .option("streaming_stage", streamingStage)
      .format(SNOWFLAKE_SOURCE_NAME)
      .start()

    // Max wait time: 5 minutes, retry interval: 30 seconds.
    if (!waitForWriteDone(streamingTable, 20, 300000, 30000)) {
      println(s"Don't find expected row count in target table. " +
        s"The root cause could be the snow pipe is too slow. " +
        "It is a SC issue, only if it is reproduced consistently.")
    }
    query.stop()

    checkTestTable(
      Seq(
        Row("0"),
        Row("1"),
        Row("2"),
        Row("3"),
        Row("4"),
        Row("5"),
        Row("6"),
        Row("7"),
        Row("8"),
        Row("9"),
        Row("eight"),
        Row("five"),
        Row("four"),
        Row("night"),
        Row("one"),
        Row("seven"),
        Row("six"),
        Row("ten"),
        Row("three"),
        Row("two")
      )
    )

  }

  ignore("kafka") {

    val spark = sparkSession

    conn.createStage(name = streamingStage, overwrite = true)

    DefaultJDBCWrapper.executeQueryInterruptibly(
      conn,
      s"create or replace table $streamingTable (key string, value string)"
    )

    val checkpoint = "check"
    removeDirectory(new File(checkpoint))

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    val query = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", "check")
      .options(connectorOptionsNoTable)
      .option("dbtable", streamingTable)
      .option("streaming_stage", streamingStage)
      .format(SNOWFLAKE_SOURCE_NAME)
      .start()

    query.awaitTermination()

  }

  ignore("kafka1") {

    val spark = sparkSession
    val streamingTable = "streaming_test_table"

    conn.createStage(name = streamingStage, overwrite = true)

    DefaultJDBCWrapper.executeQueryInterruptibly(
      conn,
      s"create or replace table $streamingTable (key string, value string)"
    )

    val checkpoint = "check"
    removeDirectory(new File(checkpoint))

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    // scalastyle:off println
    println("-------------------------------------------------")
    // scalastyle:on println

    var query = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", "check")
      .options(connectorOptionsNoTable)
      .option("dbtable", streamingTable)
      .option("streaming_stage", streamingStage)
      .format(SNOWFLAKE_SOURCE_NAME)
      .trigger(Trigger.ProcessingTime(1000))
      .start()

    Thread.sleep(20000)

    // scalastyle:off println
    println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
    // scalastyle:on println
    query.stop()

    query = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .option("checkpointLocation", "check")
      .options(connectorOptionsNoTable)
      .option("dbtable", streamingTable)
      .option("streaming_stage", streamingStage)
      .format(SNOWFLAKE_SOURCE_NAME)
      .trigger(Trigger.ProcessingTime(1000))
      .start()

    Thread.sleep(600000)
    spark.close()

  }

  test("test context") {
    val tempStage = false
    val storage: CloudStorage = CloudStorageOperations
      .createStorageClient(
        params,
        conn,
        tempStage,
        Some("spark_streaming_test_stage")
      )
      ._1
    // val log = IngestLogManager.readIngestList(storage, conn)

    val failed = IngestContextManager.readFailedFileList(0, storage, conn)
    failed.addFiles(List("a", "b", "c"))

    val failed1 = IngestContextManager.readFailedFileList(0, storage, conn)

    // scalastyle:off println
    println(failed1.toString)
    // scalastyle:on println

  }
  ignore("kafka2") {

    import org.apache.spark.sql.functions._
    val KafkaLoggingTopic = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    val spark = sparkSession
    import spark.implicits._

    val KafkaLoggingTopicDF =
      KafkaLoggingTopic.select($"value".cast("string")).as("event")

    val loggingSchema = new StructType()
      .add("url", StringType)
      .add("method", StringType)
      .add("action", StringType)
      .add("timestamp", StringType)
      .add("esbTransactionId", IntegerType)
      .add("esbTransactionGuid", StringType)
      .add("executionTime", IntegerType)
      .add("serverName", StringType)
      .add("sourceIp", StringType)
      .add("eventName", StringType)
      .add("operationName", StringType)
      .add(
        "header",
        new StructType()
          .add("request", StringType)
          .add("response", StringType)
      )
      .add(
        "body",
        new StructType()
          .add("request", StringType)
          .add("response", StringType)
      )
      .add(
        "indexed",
        new StructType()
          .add("openTimeout", StringType)
          .add("readTimeout", StringType)
          .add("threadId", StringType)
      )
      .add(
        "details",
        new StructType()
          .add("soapAction", StringType)
          .add("artifactType", StringType)
          .add("reason", StringType)
          .add("requestId", StringType)
          .add("success", StringType)
          .add("backendOwner", StringType)
      )
      .add(
        "syslog",
        new StructType()
          .add("appName", StringType)
          .add("facility", StringType)
          .add("host", StringType)
          .add("priority", StringType)
          .add("severity", StringType)
          .add("timestamp", StringType)
      )

    val loggingSchemaDF = KafkaLoggingTopicDF
      .select(from_json('value, loggingSchema) as 'event)
      .select("event.*")

    loggingSchemaDF.writeStream
      .outputMode("append")
      .options(connectorOptionsNoTable)
      .option("checkpointLocation", "check")
      // .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
      .option("dbtable", "streaming_test")
      .option("streaming_stage", "streaming_test")
      .format(SNOWFLAKE_SOURCE_NAME)
      .start()
      .awaitTermination()
  }

  ignore("kafka 3") {

    import org.apache.spark.sql.functions._
    val KafkaLoggingTopic = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()

    val spark = sparkSession
    import spark.implicits._

    val KafkaLoggingTopicDF =
      KafkaLoggingTopic.select($"value".cast("string")).as("event")

    val loggingSchema = new StructType()
      .add("url", StringType)
      .add(
        "header",
        new StructType()
          .add("request", StringType)
          .add("response", StringType)
      )

    val loggingSchemaDF = KafkaLoggingTopicDF
      .select(from_json('value, loggingSchema) as 'event)
      .select("event.*")

    loggingSchemaDF.writeStream
      .outputMode("append")
      .options(connectorOptionsNoTable)
      .option("checkpointLocation", "check")
      .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
      .option("dbtable", "streaming_test")
      .option("column_mapping", "name")
      .option("streaming_stage", "streaming_test")
      .format(SNOWFLAKE_SOURCE_NAME)
      .start()
      .awaitTermination()
  }

}
