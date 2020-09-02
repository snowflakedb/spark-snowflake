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
echo "SNOWFLAKE_TEST_CONFIG=$SNOWFLAKE_TEST_CONFIG"

export SPARK_HOME=/users/spark
export SPARK_WORKDIR=/users/spark/work

export SPARK_CONNECTOR_JAR_NAME=spark-snowflake_${TEST_SCALA_VERSION}-${TEST_SPARK_CONNECTOR_VERSION}-spark_${TEST_SPARK_VERSION}.jar
export JDBC_JAR_NAME=snowflake-jdbc-${TEST_JDBC_VERSION}.jar

# Check test file exists
ls -al $SNOWFLAKE_TEST_CONFIG \
       $SPARK_WORKDIR/${SPARK_CONNECTOR_JAR_NAME} \
       $SPARK_WORKDIR/${JDBC_JAR_NAME} \
       $SPARK_WORKDIR/clustertest_${TEST_SCALA_VERSION}-1.0.jar \
       $SPARK_WORKDIR/ClusterTest.py

echo "Important: if new test cases are added, script .github/docker/check_result.sh MUST be updated"
# Run pyspark test
# python3 has been installed in the container
$SPARK_HOME/bin/spark-submit \
      --jars $SPARK_WORKDIR/${SPARK_CONNECTOR_JAR_NAME},$SPARK_WORKDIR/${JDBC_JAR_NAME} \
      --conf "spark.pyspark.python=python3" --conf "spark.pyspark.driver.python=python3" \
      --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=$SPARK_WORKDIR  -Dnet.snowflake.jdbc.loggerImpl=net.snowflake.client.log.SLF4JLogger -Dlog4j.configuration=file://${SPARK_HOME}/conf/log4j_executor.properties" \
      --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=$SPARK_WORKDIR -Dnet.snowflake.jdbc.loggerImpl=net.snowflake.client.log.SLF4JLogger -Dlog4j.configuration=file://${SPARK_HOME}/conf/log4j_driver.properties" \
      --master spark://master:7077 --deploy-mode client \
      $SPARK_WORKDIR/ClusterTest.py remote

$SPARK_HOME/bin/spark-submit \
      --jars $SPARK_WORKDIR/${SPARK_CONNECTOR_JAR_NAME},$SPARK_WORKDIR/${JDBC_JAR_NAME} \
      --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=$SPARK_WORKDIR  -Dnet.snowflake.jdbc.loggerImpl=net.snowflake.client.log.SLF4JLogger -Dlog4j.configuration=file://${SPARK_HOME}/conf/log4j_executor.properties" \
      --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=$SPARK_WORKDIR -Dnet.snowflake.jdbc.loggerImpl=net.snowflake.client.log.SLF4JLogger -Dlog4j.configuration=file://${SPARK_HOME}/conf/log4j_driver.properties" \
      --master spark://master:7077 --deploy-mode client \
      --class net.snowflake.spark.snowflake.ClusterTest \
      $SPARK_WORKDIR/clustertest_${TEST_SCALA_VERSION}-1.0.jar remote "net.snowflake.spark.snowflake.testsuite.BasicReadWriteSuite;"

# Low memory partition upload test. Setting executor memory to 900MB. The actual usable heap size can be calculated as (900 - 300) * 0.6 = 360MB.
# Heap size is shared by RDD storage and execution memory together. The partition is 309 MB un compressed and 220 MB compressed.
# In this test we verify that with multipart upload, there is no OOM. Otherwise there is.
$SPARK_HOME/bin/spark-submit \
      --jars $SPARK_WORKDIR/${SPARK_CONNECTOR_JAR_NAME},$SPARK_WORKDIR/${JDBC_JAR_NAME} \
      --conf "spark.executor.extraJavaOptions=-Djava.io.tmpdir=$SPARK_WORKDIR  -Dnet.snowflake.jdbc.loggerImpl=net.snowflake.client.log.SLF4JLogger -Dlog4j.configuration=file://${SPARK_HOME}/conf/log4j_executor.properties" \
      --conf "spark.driver.extraJavaOptions=-Djava.io.tmpdir=$SPARK_WORKDIR -Dnet.snowflake.jdbc.loggerImpl=net.snowflake.client.log.SLF4JLogger -Dlog4j.configuration=file://${SPARK_HOME}/conf/log4j_driver.properties" \
      --master spark://master:7077 --deploy-mode client \
      --class net.snowflake.spark.snowflake.ClusterTest \
      --driver-memory 900m \
      --executor-memory 900m \
      $SPARK_WORKDIR/clustertest_${TEST_SCALA_VERSION}-1.0.jar remote "net.snowflake.spark.snowflake.testsuite.LowMemoryStressSuite;"
