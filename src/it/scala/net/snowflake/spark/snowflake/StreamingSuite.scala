package net.snowflake.spark.snowflake

import java.io.{DataOutputStream, File}
import java.net.ServerSocket
import java.nio.charset.Charset

import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.Row

class StreamingSuite extends IntegrationSuiteBase {

  private class NetworkService(
                                val port: Int,
                                val data: Seq[String],
                                val sleepBeforeAll: Int = 10,
                                val sleepAfterAll: Int = 1,
                                val sleepAfterEach: Int = 5) extends Runnable {
    val serverSocket = new ServerSocket(port)

    def run(): Unit = {
      val socket = serverSocket.accept()
      val output = new DataOutputStream(socket.getOutputStream)
      Thread.sleep(sleepBeforeAll * 1000)
      data.foreach(d => {
        println(s"send message: $d")
        s"$d\n".getBytes(Charset.forName("UTF-8")).foreach(x => output.write(x))
        Thread.sleep(sleepAfterEach * 1000)
      })
      Thread.sleep(sleepAfterAll * 1000)
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
      .option("query", s"select * from streaming_test order by value")
      .load()
    checkAnswer(loadedDf, expectedAnswer)
  }

  test("Test streaming writer") {

    val spark = sqlContext.sparkSession
    import spark.implicits._

//    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", params.awsAccessKey.get)
//    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", params.awsSecretKey.get)


    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 5678)
      .load()
    val words = lines.as[String].flatMap(_.split(" "))

    val checkpoint = "check"
    removeDirectory(new File(checkpoint))

    new Thread(new NetworkService(
      5678,
      Seq("one two",
        "three four five",
        "six seven eight night ten",
        "1 2 3 4 5",
        "6 7 8 9 0"),
      sleepBeforeAll = 5,
      sleepAfterAll = 10,
      sleepAfterEach = 5
    )).start()


    val query = words.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpoint)
      .options(connectorOptionsNoTable)
      .option("dbtable", "streaming_test")
      .format(SNOWFLAKE_SOURCE_NAME)
      .start()


    query.awaitTermination(100000)

    checkTestTable(Seq(
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
    ))

  }
}