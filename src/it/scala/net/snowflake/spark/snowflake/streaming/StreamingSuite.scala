package net.snowflake.spark.snowflake.streaming

import java.io.{DataOutputStream, File}
import java.net.ServerSocket
import java.nio.charset.Charset

import net.snowflake.spark.snowflake.DefaultJDBCWrapper.DataBaseOperations
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import net.snowflake.spark.snowflake.io.{CloudStorage, CloudStorageOperations}
import net.snowflake.spark.snowflake.{DefaultJDBCWrapper, IntegrationSuiteBase}
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import scala.util.Random

class StreamingSuite extends IntegrationSuiteBase {

  val table = s"test_table_${Random.alphanumeric take 10 mkString ""}"

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
    val loadedDf = sqlContext.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(connectorOptionsNoTable)
      .option("query", s"select * from $table order by value")
      .load()
    checkAnswer(loadedDf, expectedAnswer)
  }

  override def afterAll(): Unit = {
    conn.dropTable(table)
    super.afterAll()
  }

  // manual test only
  ignore("test") {
    val spark = sqlContext.sparkSession
    import spark.implicits._

    DefaultJDBCWrapper.executeQueryInterruptibly(
      conn,
      "create or replace table stream_test_table (value string)"
    )

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 5678)
      .load()
    val words = lines.as[String].flatMap(_.split(" "))

    val checkpoint = "check"
    removeDirectory(new File(checkpoint))

    new Thread(
      new NetworkService2(5678, sleepBeforeAll = 5, sleepAfterEach = 5)
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

  ignore("Test streaming writer") {

    val spark = sqlContext.sparkSession
    import spark.implicits._
    val streamingStage = "streaming_test_stage"

    conn.createStage(name = streamingStage, overwrite = true)

    DefaultJDBCWrapper.executeQueryInterruptibly(
      conn,
      s"create or replace table $table (value string)"
    )

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 5678)
      .load()
    val words = lines.as[String].flatMap(_.split(" "))

    val output = words.toDF("VALUE")

    val checkpoint = "check"
    removeDirectory(new File(checkpoint))

    new Thread(
      new NetworkService(
        5678,
        Seq(
          "one two",
          "three four five",
          "six seven eight night ten",
          "1 2 3 4 5",
          "6 7 8 9 0"
        ),
        sleepBeforeAll = 5,
        sleepAfterAll = 10,
        sleepAfterEach = 5
      )
    ).start()

    val query = output.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpoint)
      .options(connectorOptionsNoExternalStageNoTable)
      .option("dbtable", table)
      .option("streaming_stage", streamingStage)
      .format(SNOWFLAKE_SOURCE_NAME)
      .start()

    query.awaitTermination(150000)

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

    val spark = sqlContext.sparkSession
    val streamingStage = "streaming_test_stage"

    conn.createStage(name = streamingStage, overwrite = true)

    DefaultJDBCWrapper.executeQueryInterruptibly(
      conn,
      s"create or replace table $table (key string, value string)"
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
      .option("dbtable", table)
      .option("streaming_stage", streamingStage)
      .format(SNOWFLAKE_SOURCE_NAME)
      .start()

    query.awaitTermination()

  }

  ignore("kafka1") {

    val spark = sqlContext.sparkSession
    val streamingStage = "streaming_test_stage"
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

  ignore("test context") {
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
      // .trigger(ProcessingTime("1 seconds"))
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
       .trigger(ProcessingTime("1 seconds"))
      .option("dbtable", "streaming_test")
      .option("column_mapping", "name")
      .option("streaming_stage", "streaming_test")
      .format(SNOWFLAKE_SOURCE_NAME)
      .start()
      .awaitTermination()
  }

}
