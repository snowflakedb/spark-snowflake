#!/bin/bash

export TEST_COMPILE_SCALA_VERSION=2.11
export TEST_SPARK_CONNECTOR_VERSION=2.8.2
export TEST_SPARK_VERSION=3.0
export TEST_CLUSTERTEST_VERSION=1.0

# Test Revision ID; bump up this value whenever the test data in 
# ClusterTest/test_sources.json is updated, or when significant changes
# are made to StressReadWriteSuite.
export TEST_REVISION_ID=1

export SPARK_CONNECTOR_JAR_PATH=${SF_CONNECTOR_DIR}/target/scala-${TEST_COMPILE_SCALA_VERSION}/spark-snowflake-assembly-${TEST_SPARK_CONNECTOR_VERSION}-spark_${TEST_SPARK_VERSION}.jar
export CLUSTERTEST_JAR_PATH=${SF_CONNECTOR_DIR}/ClusterTest/target/scala-${TEST_COMPILE_SCALA_VERSION}/ClusterTest-assembly-${TEST_CLUSTERTEST_VERSION}.jar

cd $SF_CONNECTOR_DIR
build/sbt 'set test in assembly := {}' clean assembly
build/sbt package -DskipTests

cd $SF_CONNECTOR_DIR/ClusterTest
../build/sbt 'set test in assembly := {}' clean assembly

# The email jar env variables are to specify the paths to javax.mail and javax.activation JARs, needed
# for sending the mail alerts
spark-submit \
      --jars $SPARK_CONNECTOR_JAR_PATH,$STRESS_EMAIL_MAIL_JAR,$STRESS_EMAIL_ACTIVATION_JAR \
      --master yarn \
      --class net.snowflake.spark.snowflake.ClusterTest \
      $CLUSTERTEST_JAR_PATH remote "net.snowflake.spark.snowflake.testsuite.StressReadWriteSuite;" stress $TEST_REVISION_ID $STRESS_EMAIL_ALERT_ADDRESS
