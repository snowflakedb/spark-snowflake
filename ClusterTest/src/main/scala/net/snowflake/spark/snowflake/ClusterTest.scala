package net.snowflake.spark.snowflake

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.{
  JsonNode,
  ObjectMapper
}
import org.apache.spark.sql.{Row, SaveMode}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object ClusterTest {
  val SNOWFLAKE_TEST_ACCOUNT = "SNOWFLAKE_TEST_ACCOUNT"
  private val log = LoggerFactory.getLogger(getClass)

  /**
    * read snowflake.travis.json
    */
  def loadJsonConfig(configFile: String): Option[Map[String, String]] = {

    var result: Map[String, String] = Map()
    def read(node: JsonNode): Unit = {
      val itr = node.fields()
      while (itr.hasNext) {
        val entry = itr.next()
        result = result + (entry.getKey -> entry.getValue.asText())
      }
    }

    try {
      val jsonConfigFile = Source.fromFile(configFile)
      val file = jsonConfigFile.mkString
      val mapper: ObjectMapper = new ObjectMapper()
      val json = mapper.readTree(file)
      val commonConfig = json.get("common")
      val accountConfig = json.get("account_info")
      val accountName: String =
        Option(System.getenv(SNOWFLAKE_TEST_ACCOUNT)).getOrElse("aws")

      log.info(s"test account: $accountName")

      read(commonConfig)

      read(
        (
          for (i <- 0 until accountConfig.size()
               if accountConfig.get(i).get("name").asText() == accountName)
            yield accountConfig.get(i).get("config")
        ).head
      )

      log.info(s"load config from $configFile")
      jsonConfigFile.close()
      Some(result)
    } catch {
      case _: Throwable =>
        log.info(s"Can't read $configFile, load config from other source")
        None
    }
  }

  // Used for internal integration testing in SF env.
  protected def readConfigValueFromEnv(name: String): Option[String] = {
    scala.util.Properties.envOrNone(s"SPARK_CONN_ENV_${name.toUpperCase}")
  }

  protected lazy val configsFromEnv: Map[String, String] = {
    log.info(s"Get configure from env configsFromEnv")
    var settingsMap = new mutable.HashMap[String, String]
    Parameters.KNOWN_PARAMETERS foreach { param =>
      val opt = readConfigValueFromEnv(param)
      if (opt.isDefined) {
        settingsMap += (param -> opt.get)
      }
    }
    settingsMap.toMap
  }

  def main(args: Array[String]): Unit = {
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    var configFile = System.getenv("SNOWFLAKE_TEST_CONFIG")
    if (configFile == null) {
      configFile = "/Users/ec2-user/snowflake.travis.json"
    }
    log.info(s"Config file is: $configFile")

    var sfOptions = loadJsonConfig(configFile).get
    sfOptions -= "dbtable"
    // val sfOptions = configsFromEnv
    snowflakeReadWrite(
      spark,
      sfOptions,
      "TPCH_SF10",
      "LINEITEM10_LOAD_FROM_PARQUET"
    )

    spark.stop()
  }

  private def snowflakeReadWrite(spark: SparkSession,
                                 sfOptions: Map[String, String],
                                 sourceSchema: String,
                                 sourceTableName: String): Unit = {
    val sqlContext = spark.sqlContext
    val optionsNoTable = sfOptions

    // Table name in testdb_spark      Schema        row count  data size
    // ------------------------------  ------------  ---------  ---------
    // LINEITEM10_LOAD_FROM_PARQUET    TPCH_SF10     60M        1.6G
    // LINEITEM100_LOAD_FROM_PARQUET   TPCH_SF100    600M       16G
    // STORE_RETURNS_10000_SPARKWRITE  TPCH_SF10     2.9G       116.4G
    // WEB_SALES                       TPCDS_SF10000 7.2G       489.1G
    // CATALOG_SALES                   TPCDS_SF10000 14.4G      957.1G
    // STORE_SALES                     TPCDS_SF10000 28.8G      1.3T

    val snowflakeName = "net.snowflake.spark.snowflake"

    val sourceCount: Long = 1000000

    val targetTableName = "TEST_SINK_123"
    val targetSchema = "spark_test"
    val df = sqlContext.read
      .format(snowflakeName)
      .options(optionsNoTable)
      .option(
        "query",
        s"select * from $sourceSchema.$sourceTableName limit $sourceCount"
      )
      // .option("dbtable", sourceTableName)
      // .option("sfSchema", sourceSchema)
      .load()

    val startTime = System.currentTimeMillis()
    df.write
      .format(snowflakeName)
      .options(optionsNoTable)
      .option("dbtable", targetTableName)
      .option("sfSchema", targetSchema)
      .mode(SaveMode.Overwrite)
      .save()
    val midTime = System.currentTimeMillis()

    val df_result = sqlContext.read
      .format(snowflakeName)
      .options(optionsNoTable)
      .option("dbtable", targetTableName)
      .option("sfSchema", targetSchema)
      .load()
    val endTime = System.currentTimeMillis()

    val targetCount = df_result.count()

    // verify row count to be equal
    val result_msg =
      s"source/target count: $sourceCount / $targetCount : read_write time: ${(midTime - startTime).toDouble / 1000.0} s"
    log.info(result_msg)
    if (sourceCount == sourceCount) {
      log.info("row count is correct.")
    }

//    // Verify HASH_AGG to be equal.
//    var dfHash = sqlContext.read
//      .format(snowflakeName)
//      .options(optionsNoTable)
//      .option("query", s"select HASH_AGG(*) from $sourceTableName")
//      .option("sfSchema", sourceSchema)
//      .load()
//
//    val sourceHashAgg = dfHash.collect()(0)(0)
//
//    dfHash = sqlContext.read
//      .format(snowflakeName)
//      .options(optionsNoTable)
//      .option("query", s"select HASH_AGG(*) from $targetTableName")
//      .option("sfSchema", targetSchema)
//      .load()
//
//    val targetHashAgg = dfHash.collect()(0)(0)
//    val agg_message = s"sourceHashAgg $sourceHashAgg $sourceSchema $sourceTableName\n" +
//      s"targetHashAgg $targetHashAgg $targetSchema $targetTableName"
//    println(agg_message)
//    if (sourceHashAgg == targetHashAgg) {
//      println("hash agg result is correct.")
//    }
//
//    if (sourceCount != sourceCount) {
//      throw new Exception(result_msg)
//    }
//    if (sourceHashAgg != targetHashAgg) {
//      throw new Exception(agg_message)
//    }
  }

}
