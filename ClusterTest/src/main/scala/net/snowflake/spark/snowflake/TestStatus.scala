package net.snowflake.spark.snowflake

private[snowflake] case class TestStatus(var testName: String) {
  var testStatus: String = TestUtils.TEST_RESULT_STATUS_NOT_STARTED
  var reason: Option[String] = None
  var taskStartTime: Long = 0
  var taskEndTime: Long = 0
}
