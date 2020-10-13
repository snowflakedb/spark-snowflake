package net.snowflake.spark.snowflake

import java.io.File
import org.apache.log4j.PropertyConfigurator
import net.snowflake.client.jdbc.internal.apache.commons.io.FileUtils
import net.snowflake.spark.snowflake.Utils.SNOWFLAKE_SOURCE_NAME
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory
import scala.io.Source
import scala.util.matching.Regex

class SecuritySuite extends IntegrationSuiteBase {

  // The TEST_LOG_FILE_NAME needs to be configured in TEST_LOG4J_PROPERTY
  private val TEST_LOG_FILE_NAME = "spark_connector.log"
  private val TEST_LOG4J_PROPERTY = "src/it/resources/log4j_file.properties"
  private val log = LoggerFactory.getLogger(getClass)

  // Add some options for default for testing.
  private var thisConnectorOptionsNoTable: Map[String, String] = Map()

  private val test_table_large_result: String =
    s"test_table_large_result_$randomSuffix"
  private val test_table_write: String = s"test_table_write_$randomSuffix"

  private val largeStringValue =
    s"""spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |spark_connector_test_large_result_1234567890
       |""".stripMargin.filter(_ >= ' ')

  private def setupLargeResultTable(): Unit = {
    jdbcUpdate(s"""create or replace table $test_table_large_result (
                  | int_c int, c_string string(1024) )""".stripMargin)

    jdbcUpdate(s"""insert into $test_table_large_result select
                  | seq4(), '$largeStringValue'
                  | from table(generator(rowcount => 100000))""".stripMargin)

    val tmpdf = sparkSession.read
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", s"$test_table_large_result")
      .load()

    tmpdf.createOrReplaceTempView("test_table_large_result")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    connectorOptionsNoTable.foreach(tup => {
      thisConnectorOptionsNoTable += tup
    })

    // Setup special options for this test
    thisConnectorOptionsNoTable += ("use_copy_unload" -> "false")
    thisConnectorOptionsNoTable += ("partition_size_in_mb" -> "20")

    setupLargeResultTable()
    // clean up the test log file in beforeAll() not afterAll()
    // so that we can check result when there is error.
    FileUtils.deleteQuietly(new File(TEST_LOG_FILE_NAME))
  }

  test("verify pre-signed URL are not logged for read & write") {
    log.info("Reconfigure to log into file")
    // Reconfigure log file to output all logging entries.
    reconfigureLogFile(TEST_LOG4J_PROPERTY)

    // Read from one snowflake table and write to another snowflake table
    sparkSession
      .sql("select * from test_table_large_result order by int_c")
      .write
      .format(SNOWFLAKE_SOURCE_NAME)
      .options(thisConnectorOptionsNoTable)
      .option("dbtable", test_table_write)
      .mode(SaveMode.Overwrite)
      .save()

    // Check pre-signed is used for the test
    assert(searchInLogFile(".*Spark Connector.*"))

    // Check pre-signed URL are NOT printed in the log
    // by searching the pre-signed URL domain name.
    assert(!searchInLogFile(".*https?://.*amazonaws.com.*"))
    assert(!searchInLogFile(".*https?://.*core.windows.net.*"))
    assert(!searchInLogFile(".*https?://.*googleapis.com.*"))

    // Reconfigure back to the default log file.
    reconfigureLogFile(DEFAULT_LOG4J_PROPERTY)

    log.info("Restore back to log into STDOUT")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterAll(): Unit = {
    try {
      jdbcUpdate(s"drop table if exists $test_table_large_result")
      jdbcUpdate(s"drop table if exists $test_table_write")
    } finally {
      super.afterAll()
      SnowflakeConnectorUtils.disablePushdownSession(sparkSession)
    }
  }

  private def searchInLogFile(regex: String): Boolean = {
    val bufferedSource = Source.fromFile(TEST_LOG_FILE_NAME)
    var found = false
    var i = 0
    for (line <- bufferedSource.getLines) {
      if (line.matches(regex)) {
        found = true
        // println(s"$regex matches line $i: $line")
      }
      i += 1
    }
    bufferedSource.close()
    found
  }
}
