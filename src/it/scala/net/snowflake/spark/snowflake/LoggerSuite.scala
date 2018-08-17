package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper

class LoggerSuite extends IntegrationSuiteBase {

  val mapper = new ObjectMapper()

  test("test") {
//    val log = StreamingBatchLog(0, List("a", "b"))
//    log.setFailedFiles(List("a"))
//    log.addFailedFiles(List("b"))
//    println(log)

//    val node = mapper.readTree("{\"abc\":\"123\"}")
//    println(node.get("abc").asText())

//    print(SnowflakeLogManager.getLog("{\"batchId\":0,\"logType\":\"STREAMING_BATCH_LOG\",\"fileNames\":[\"a\",\"b\"],\"failedFileNames\":[\"a\",\"b\"]}"))
  }
}
