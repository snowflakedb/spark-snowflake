#!/bin/bash

echo "This script should be run in repository root directory."

# If new test cases are added in ClusterTest/run_cluster_test.sh
# TOTAL_TEST_CASE_COUNT may be updated
export TOTAL_TEST_CASE_COUNT=2
export TOTAL_TIMEOUT_IN_SECONDS=3600

echo "Below variables must be set by workflow and docker-conpose file"
echo "GITHUB_RUN_ID=$GITHUB_RUN_ID"
echo "GITHUB_SHA=$GITHUB_SHA"
echo "TEST_SPARK_CONNECTOR_VERSION=$TEST_SPARK_CONNECTOR_VERSION"
echo "TEST_SCALA_VERSION=$TEST_SCALA_VERSION"
echo "TEST_COMPILE_SCALA_VERSION=$TEST_COMPILE_SCALA_VERSION"
echo "TEST_JDBC_VERSION=$TEST_JDBC_VERSION"
echo "TEST_SPARK_VERSION=$TEST_SPARK_VERSION"

export JDBC_JAR_NAME=snowflake-jdbc-${TEST_JDBC_VERSION}.jar

echo "Start to downlod dependent libraries"
# Below script is not parameterized yet.
curl -s -o scala-library-${TEST_COMPILE_SCALA_VERSION}.jar https://repo1.maven.org/maven2/org/scala-lang/scala-library/${TEST_COMPILE_SCALA_VERSION}/scala-library-${TEST_COMPILE_SCALA_VERSION}.jar
curl -s -o slf4j-log4j12-1.7.30.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-log4j12/1.7.30/slf4j-log4j12-1.7.30.jar
curl -s -o slf4j-api-1.7.30.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar
curl -s -o snowflake-jdbc-download.jar https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/${TEST_JDBC_VERSION}/${JDBC_JAR_NAME}
curl -s -o log4j-1.2.17.jar https://repo1.maven.org/maven2/log4j/log4j/1.2.17/log4j-1.2.17.jar
curl -s -o commons-codec-1.14.jar https://repo1.maven.org/maven2/commons-codec/commons-codec/1.14/commons-codec-1.14.jar

# Check the result
echo "Start to check result. first paramter is totalTestCount, second is totalTmeout"
$JAVA_HOME/bin/java -classpath ./ClusterTest/target/scala-${TEST_SCALA_VERSION}/clustertest_${TEST_SCALA_VERSION}-1.0.jar:./target/scala-${TEST_SCALA_VERSION}/spark-snowflake_${TEST_SCALA_VERSION}-${TEST_SPARK_CONNECTOR_VERSION}-spark_${TEST_SPARK_VERSION}.jar:./snowflake-jdbc-download.jar:./scala-library-${TEST_COMPILE_SCALA_VERSION}.jar:./slf4j-log4j12-1.7.30.jar:./slf4j-api-1.7.30.jar:./log4j-1.2.17.jar:./commons-codec-1.14.jar \
	net.snowflake.spark.snowflake.ClusterTestCheckResult $TOTAL_TEST_CASE_COUNT $TOTAL_TIMEOUT_IN_SECONDS $@
testStatus=$?

echo "show all test log for diagnostic purpose"
docker logs docker_testdriver_1

# Return test status so that github action can report error correctly if there are failure.
exit $testStatus
