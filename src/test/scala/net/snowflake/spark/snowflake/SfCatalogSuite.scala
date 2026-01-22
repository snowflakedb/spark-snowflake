package net.snowflake.spark.snowflake

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class SfCatalogSuite extends FunSuite {

  protected def defaultSfCatalogParams: Map[String, String] = Map(
    "spark.sql.catalog.snowflake" -> "net.snowflake.spark.snowflake.catalog.SfCatalog",
    "spark.sql.catalog.snowflake.sfURL" -> "account.snowflakecomputing.com:443",
    "spark.sql.catalog.snowflake.sfUser" -> "username",
    "spark.sql.catalog.snowflake.sfPassword" -> "password"
  )

  test("SfCatalog Params") {
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

}
