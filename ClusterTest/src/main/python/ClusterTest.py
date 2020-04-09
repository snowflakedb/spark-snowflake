from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import udf
import json

sc = SparkContext("local", "Simple App")
spark = SQLContext(sc)
spark_conf = SparkConf().setMaster('local').setAppName('mruifirstsparkapp')

# Below options are only used by external stage
# sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "<YOUR_AWS_KEY>")
# sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "<YOUR_AWS_SECRET>")

# Utility function to get configration info from snowflake.travis.json
def getConfig():
    # with open('/Users/mrui/spark/spark-snowflake/snowflake.travis.json') as f:
    with open('snowflake.travis.json') as f:
        snowflake_travis_json = json.load(f)

    sfOptions = {}
    sfOptions["sfCompress"] = snowflake_travis_json["common"]["sfCompress"]
    sfOptions["sfSSL"] = snowflake_travis_json["common"]["sfSSL"]
    sfOptions["dbtable"] = snowflake_travis_json["common"]["dbtable"]
    sfOptions["runOption"] = snowflake_travis_json["common"]["runOption"]
    sfOptions["sfTimeZone"] = snowflake_travis_json["common"]["sfTimeZone"]
    sfOptions["sfDatabase"] = snowflake_travis_json["common"]["sfDatabase"]
    sfOptions["sfSchema"] = snowflake_travis_json["common"]["sfSchema"]
    sfOptions["sfWarehouse"] = snowflake_travis_json["common"]["sfWarehouse"]
    sfOptions["sfUser"] = snowflake_travis_json["common"]["sfUser"]
    sfOptions["pem_private_key"] = snowflake_travis_json["common"]["pem_private_key"]

    for item in snowflake_travis_json["account_info"] :
        if item["name"] == 'aws' :
            sfOptions["sfURL"] = item["config"]["sfURL"]
            data = item["config"]["sfURL"].split(".")
            sfOptions["sfAccount"] = data[0]

    return sfOptions

# UDF for testing
def squared(s):
  return s * s

sfOptions = getConfig()

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

spark.udf.register("squaredWithPython", squared)

tmpdf = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
      .options(**sfOptions) \
      .option("dbtable", "TEST_TABLE_LARGE_RESULT_2518893414841183039") \
      .load()

tmpdf.createOrReplaceTempView("mrui_large_table")

df = spark.sql("select int_c, c_string, squaredWithPython(int_c) from mrui_large_table")

df.show(100)
