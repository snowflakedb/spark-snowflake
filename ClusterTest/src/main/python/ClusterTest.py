from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import udf
import json
import os
import datetime
import sys

# Below options are only used by external stage
# sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "<YOUR_AWS_KEY>")
# sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "<YOUR_AWS_SECRET>")

# Utility function to get configration info from snowflake.travis.json
def getConfig(filename):
    with open(filename) as f:
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

    for item in snowflake_travis_json["account_info"]:
        if item["name"] == 'aws':
            sfOptions["sfURL"] = item["config"]["sfURL"]
            data = item["config"]["sfURL"].split(".")
            sfOptions["sfAccount"] = data[0]

    return sfOptions


# UDF for testing
def udf_plus_one(s):
    return s + 1


def writeResult(row):
    row_seq = [row]
    fields = [StructField("testCaseName", StringType(), True), \
              StructField("testStatus", StringType(), True), \
              StructField("githubRunId", StringType(), True), \
              StructField("commitID", StringType(), True), \
              StructField("testType", StringType(), True), \
              StructField("startTime", StringType(), True), \
              StructField("testRunTime", StringType(), True), \
              StructField("reason", StringType(), True), \
              ]
    schema = StructType(fields)
    df = spark.createDataFrame(row_seq, schema)
    df.write \
        .format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", targetTableName) \
        .option("sfSchema", targetSchema) \
        .mode('append') \
        .save()


# Retrieve file from env SNOWFLAKE_TEST_CONFIG.
filename = os.environ.get('SNOWFLAKE_TEST_CONFIG')
githubsha = os.environ.get('GITHUB_SHA')
githubrunid = os.environ.get('GITHUB_RUN_ID')
targetTableName = 'CLUSTER_TEST_RESULT_TABLE'
targetSchema = 'SPARK_TEST'
startDate = str(datetime.datetime.today())
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

# sc = SparkContext("local", "Simple App")
# spark = SQLContext(sc)
# spark_conf = SparkConf().setMaster('local').setAppName('PythonClusterTest')
spark_session_builder = SparkSession.builder \
    .appName("PythonClusterTest")

local_agv = sys.argv[1]
if local_agv.lower() == 'local':
    spark_session_builder = spark_session_builder.master('local')

spark = spark_session_builder.getOrCreate()

sfOptions = getConfig(filename)


spark.udf.register("udfPlusOne", udf_plus_one)

try:
    testdf = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
        .options(**sfOptions) \
        .option("dbtable", "ORDERS") \
        .option("sfSchema", "TPCH_SF1") \
        .load()

    testdf.createOrReplaceTempView("test_table_python")

    row_count = spark.sql("select count(O_TOTALPRICE) from test_table_python").collect()[0][0]
    origin_sum = spark.sql("select sum(O_TOTALPRICE) from test_table_python").collect()[0][0]
    plus_one_sum = spark.sql("select sum(udfPlusOne(O_TOTALPRICE)) from test_table_python").collect()[0][0]

    print ("row_count ", row_count)
    print ("origin_sum ", origin_sum)
    print ("plus_one_sum ", plus_one_sum)

    expect_result = int(origin_sum + row_count)
    actual_result = int(plus_one_sum)

    print ("expect_result ", expect_result)
    print ("actual_result ", actual_result)

    if expect_result == actual_result:
        print("test is successful")
        result_row = Row("ClusterTest.py", "Success", githubrunid, githubsha, 'Python', startDate, "not collected",
                         "success")
        writeResult(result_row)
    else:
        reason = "result sum is incorrect, expect_result=" + str(expect_result) + " actual_result=" + str(actual_result)
        print("test is fail: ", reason)
        result_row = Row("ClusterTest.py", "Fail", githubrunid, githubsha, 'Python', startDate, "not collected", reason)
        writeResult(result_row)

    # raise Exception('test exception')

except Exception as e:
    reason = str(e)
    print("test raise exception: ", reason)
    result_row = Row("ClusterTest.py", "Exception", githubrunid, githubsha, 'Python', startDate, "not collected",
                     reason)
    writeResult(result_row)
