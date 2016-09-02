#!/usr/bin/env bash

set -x -e

# Only run unit tests if INTEGRATION_TESTS is not true
if [ "$INTEGRATION_TESTS" != "true" ] ; then
  sbt -Dhadoop.testVersion=$HADOOP_VERSION -Dspark.testVersion=$SPARK_VERSION ++$TRAVIS_SCALA_VERSION coverage test ;
fi

# Only run it if INTEGRATION_TESTS is true
if [ "$INTEGRATION_TESTS" == "true" ]; then
  export IT_SNOWFLAKE_CONF=snowflake.travis.conf
  if [ -e "$IT_SNOWFLAKE_CONF" ] ; then
    sbt -Dhadoop.version=$HADOOP_VERSION -Dspark.version=$SPARK_VERSION ++$TRAVIS_SCALA_VERSION coverage it:test 2> /dev/null
  else
    echo ""$IT_SNOWFLAKE_CONF does not exist"
    exit 1
  fi
fi

# sbt ++$TRAVIS_SCALA_VERSION scalastyle
# sbt ++$TRAVIS_SCALA_VERSION "test:scalastyle"
# sbt ++$TRAVIS_SCALA_VERSION  "it:scalastyle"
