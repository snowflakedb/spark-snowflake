#!/bin/bash

echo "This script run inside of the docker container"

echo "GITHUB_RUN_ID=$GITHUB_RUN_ID"
echo "GITHUB_SHA=$GITHUB_SHA"

export SPARK_HOME=/users/spark
export SPARK_WORKDIR=/users/spark/work

# Decrept profile json
cd $SPARK_WORKDIR
bash decrypt_secret.sh $SNOWFLAKE_TEST_CONFIG $ENCRYPTED_SNOWFLAKE_TEST_CONFIG 

echo "Important: if new test cases are added, script .github/docker/check_result.sh MUST be updated"

# Run pyspark test
# python3 has been installed in the container
$SPARK_HOME/bin/spark-submit \
      --conf "spark.pyspark.python=python3" --conf "spark.pyspark.driver.python=python3" \
      --jars $SPARK_WORKDIR/spark-snowflake_2.11-2.7.0-spark_2.4.jar,$SPARK_WORKDIR/snowflake-jdbc-3.12.2.jar \
      --master spark://master:7077 --deploy-mode client \
      $SPARK_WORKDIR/ClusterTest.py remote

$SPARK_HOME/bin/spark-submit \
      --jars $SPARK_WORKDIR/spark-snowflake_2.11-2.7.0-spark_2.4.jar,$SPARK_WORKDIR/snowflake-jdbc-3.12.2.jar \
      --master spark://master:7077 --deploy-mode client \
      --class net.snowflake.spark.snowflake.ClusterTest \
      $SPARK_WORKDIR/clustertest_2.11-1.0.jar remote "net.snowflake.spark.snowflake.testsuite.BasicReadWriteSuite;"

