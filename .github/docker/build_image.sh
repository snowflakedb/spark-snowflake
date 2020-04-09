#!/bin/bash

echo "This script should be run in repository root directory."

# Build spark connector
sbt ++2.11.12 package

# Build cluster test binaries
cd ClusterTest
sbt ++2.11.12 package
rm -fr work
mkdir work
cp target/scala-2.11/clustertest_2.11-1.0.jar work/
cp src/main/python/*.py work
cd work
tar -czf testcase.tar.gz *
cd ../..

# Build docker image
docker build \
--build-arg SPARK_URL=http://apache.spinellicreations.com/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz \
--build-arg SPARK_BINARY_NAME=spark-2.4.5-bin-hadoop2.7.tgz \
--build-arg JDBC_URL=https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.12.2/snowflake-jdbc-3.12.2.jar \
--build-arg JDBC_BINARY_NAME=snowflake-jdbc-3.12.2.jar \
--build-arg SPARK_CONNECTOR_LOCATION=target/scala-2.11/spark-snowflake_2.11-2.7.0-spark_2.4.jar \
--build-arg SPARK_CONNECTOR_BINARY_NAME=spark-snowflake_2.11-2.7.0-spark_2.4.jar \
--build-arg TEST_CASE_LOCATION=ClusterTest/work/testcase.tar.gz \
--build-arg TEST_CASE_BINARY_NAME=testcase.tar.gz \
--build-arg ENCRYPTED_SNOWFLAKE_TEST_CONFIG=snowflake.travis.json.gpg \
--build-arg SNOWFLAKE_TEST_CONFIG=snowflake.travis.json \
--build-arg DECRYPT_SCRIPT=.github/scripts/decrypt_secret.sh \
--build-arg SPARK_ENV_SCRIPT=.github/docker/spark-env.sh \
--build-arg JDBC_LOGGING_PROPERTIES=.github/docker/logging.properties \
--build-arg RUN_TEST_SCRIPT=ClusterTest/run_cluster_test.sh \
--build-arg ENTRYPOINT_SCRIPT=.github/docker/entrypoint.sh \
--tag $DOCKER_IMAGE_TAG -f .github/docker/Dockerfile .

