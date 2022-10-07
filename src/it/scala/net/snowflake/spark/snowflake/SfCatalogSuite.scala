package net.snowflake.spark.snowflake

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}

class SfCatalogSuite extends SnowflakeIntegrationSuite {
  private val test_table2: String = s"test_table2_$randomSuffix"

  protected def defaultSfCatalogParams: Map[String, String] = Map(
    "spark.sql.catalog.snowflake" -> "net.snowflake.spark.snowflake.catalog.SfCatalog",
    "spark.sql.catalog.snowflake.sfURL" -> "account.snowflakecomputing.com:443",
    "spark.sql.catalog.snowflake.sfUser" -> "username",
    "spark.sql.catalog.snowflake.sfPassword" -> "password"
  )

  test("Test SfCatalog Params") {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SfCatalogSuite")
    conf.setAll(defaultSfCatalogParams)

    val sc = SparkContext.getOrCreate(conf)

    assert(
      sc.getConf
        .get("spark.sql.catalog.snowflake")
        .equals("net.snowflake.spark.snowflake.catalog.SfCatalog")
    )

    assert(
      sc.getConf
        .get("spark.sql.catalog.snowflake.sfURL")
        .equals("account.snowflakecomputing.com:443")
    )

    assert(
      sc.getConf
        .get("spark.sql.catalog.snowflake.sfUser")
        .equals("username")
    )

    assert(
      sc.getConf
        .get("spark.sql.catalog.snowflake.sfPassword")
        .equals("password")
    )

  }
  /*
  test("Test SfCatalog working") {
    val df = sparkSession.sql(s"select * from snowflake.$test_table2")
    checkAnswer(
      df.select("\"testint1\"", "\"!#TES#STRI?NG\"", "testint2"),
      Seq(Row(1, "Unicode", 42), Row(2, "Mario", 3), Row(3, "Luigi", 42))
    )
  }*/
}
