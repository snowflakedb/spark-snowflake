#!/usr/bin/env bash

set -x -e

if [ "$INTEGRATION_TESTS" != "true" ]; then
  # Run only test
  sbt -Dspark.testVersion=$SPARK_VERSION ++$TRAVIS_SCALA_VERSION clean coverage test
  sbt coverageReport
else
  # Run both test and it
  export IT_SNOWFLAKE_CONF=snowflake.travis.conf
  if [ -e "$IT_SNOWFLAKE_CONF" ] ; then
    sbt -Dspark.version=$SPARK_VERSION ++$TRAVIS_SCALA_VERSION clean coverage test it:test
    sbt coverageReport
  else
    echo "$IT_SNOWFLAKE_CONF does not exist"
    exit 1
  fi
fi

# sbt ++$TRAVIS_SCALA_VERSION scalastyle
# sbt ++$TRAVIS_SCALA_VERSION "test:scalastyle"
# sbt ++$TRAVIS_SCALA_VERSION  "it:scalastyle"
