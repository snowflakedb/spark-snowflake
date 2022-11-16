#!/bin/bash

echo "This script should be run in repository root directory."

echo "Below variables must be set by workflow and docker-conpose file"
echo "GITHUB_RUN_ID=$GITHUB_RUN_ID"
echo "GITHUB_SHA=$GITHUB_SHA"
echo "TEST_SPARK_CONNECTOR_VERSION=$TEST_SPARK_CONNECTOR_VERSION"
echo "TEST_SCALA_VERSION=$TEST_SCALA_VERSION"
echo "TEST_COMPILE_SCALA_VERSION=$TEST_COMPILE_SCALA_VERSION"
echo "TEST_JDBC_VERSION=$TEST_JDBC_VERSION"
echo "TEST_SPARK_VERSION=$TEST_SPARK_VERSION"

export SPARK_CONNECTOR_JAR_NAME=spark-snowflake_${TEST_SCALA_VERSION}-${TEST_SPARK_CONNECTOR_VERSION}-spark_${TEST_SPARK_VERSION}.jar
export JDBC_JAR_NAME=snowflake-jdbc-${TEST_JDBC_VERSION}.jar

echo $SPARK_CONNECTOR_JAR_NAME
echo $JDBC_JAR_NAME

# Build spark connector
sbt ++$TEST_COMPILE_SCALA_VERSION package

# Build cluster test binaries
cd ClusterTest
sbt ++$TEST_COMPILE_SCALA_VERSION  package
rm -fr work
mkdir work
cp target/scala-${TEST_SCALA_VERSION}/clustertest_${TEST_SCALA_VERSION}-1.0.jar work/
cp src/main/python/*.py work
cd work
tar -czf testcase.tar.gz *
cd ../..

# Build docker image
docker build \
--build-arg SPARK_URL=https://archive.apache.org/dist/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz \
--build-arg SPARK_BINARY_NAME=spark-3.1.1-bin-hadoop2.7.tgz \
--build-arg JDBC_URL=https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/${TEST_JDBC_VERSION}/$JDBC_JAR_NAME \
--build-arg JDBC_BINARY_NAME=$JDBC_JAR_NAME \
--build-arg SPARK_CONNECTOR_LOCATION=target/scala-${TEST_SCALA_VERSION}/$SPARK_CONNECTOR_JAR_NAME \
--build-arg SPARK_CONNECTOR_BINARY_NAME=$SPARK_CONNECTOR_JAR_NAME \
--build-arg TEST_CASE_LOCATION=ClusterTest/work/testcase.tar.gz \
--build-arg TEST_CASE_BINARY_NAME=testcase.tar.gz \
--build-arg ENCRYPTED_SNOWFLAKE_TEST_CONFIG=snowflake.travis.json.gpg \
--build-arg SNOWFLAKE_TEST_CONFIG=snowflake.travis.json \
--build-arg DECRYPT_SCRIPT=.github/scripts/decrypt_secret.sh \
--build-arg SPARK_ENV_SCRIPT=.github/docker/spark-env.sh \
--build-arg LOG4J_EXECUTOR_PROPERTIES=.github/docker/log4j_executor.properties \
--build-arg LOG4J_DRIVER_PROPERTIES=.github/docker/log4j_driver.properties \
--build-arg RUN_TEST_SCRIPT=ClusterTest/run_cluster_test.sh \
--build-arg ENTRYPOINT_SCRIPT=.github/docker/entrypoint.sh \
--tag $DOCKER_IMAGE_TAG -f .github/docker/Dockerfile .

