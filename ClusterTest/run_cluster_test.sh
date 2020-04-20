#!/bin/bash

echo "This script run inside of the docker container."

echo "Below variables must be set by workflow and docker-conpose file"
echo "GITHUB_RUN_ID=$GITHUB_RUN_ID"
echo "GITHUB_SHA=$GITHUB_SHA"
echo "TEST_SPARK_CONNECTOR_VERSION=$TEST_SPARK_CONNECTOR_VERSION"
echo "TEST_SCALA_VERSION=$TEST_SCALA_VERSION"
echo "TEST_COMPILE_SCALA_VERSION=$TEST_COMPILE_SCALA_VERSION"
echo "TEST_JDBC_VERSION=$TEST_JDBC_VERSION"
echo "TEST_SPARK_VERSION=$TEST_SPARK_VERSION"

export SPARK_HOME=/users/spark
export SPARK_WORKDIR=/users/spark/work

# Decrept profile json
cd $SPARK_WORKDIR
bash decrypt_secret.sh $SNOWFLAKE_TEST_CONFIG $ENCRYPTED_SNOWFLAKE_TEST_CONFIG 

export SPARK_CONNECTOR_JAR_NAME=spark-snowflake_${TEST_SCALA_VERSION}-${TEST_SPARK_CONNECTOR_VERSION}-spark_${TEST_SPARK_VERSION}.jar
export JDBC_JAR_NAME=snowflake-jdbc-${TEST_JDBC_VERSION}.jar

echo "Important: if new test cases are added, script .github/docker/check_result.sh MUST be updated"
# Run pyspark test
# python3 has been installed in the container
$SPARK_HOME/bin/spark-submit \
      --conf "spark.pyspark.python=python3" --conf "spark.pyspark.driver.python=python3" \
      --jars $SPARK_WORKDIR/${SPARK_CONNECTOR_JAR_NAME},$SPARK_WORKDIR/${JDBC_JAR_NAME} \
      --master spark://master:7077 --deploy-mode client \
      $SPARK_WORKDIR/ClusterTest.py remote

$SPARK_HOME/bin/spark-submit \
      --jars $SPARK_WORKDIR/${SPARK_CONNECTOR_JAR_NAME},$SPARK_WORKDIR/${JDBC_JAR_NAME} \
      --master spark://master:7077 --deploy-mode client \
      --class net.snowflake.spark.snowflake.ClusterTest \
      $SPARK_WORKDIR/clustertest_${TEST_SCALA_VERSION}-1.0.jar remote "net.snowflake.spark.snowflake.testsuite.BasicReadWriteSuite;"

