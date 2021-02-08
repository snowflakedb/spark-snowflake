#!/usr/bin/env bash

set -x -e

if [ "$INTEGRATION_TESTS" != "true" ]; then
  # Run only test
  sbt -Dspark.testVersion=$SPARK_VERSION ++$TRAVIS_SCALA_VERSION clean coverage test
  sbt coverageReport
else
  # Run both test and it
  sbt -Dspark.version=$SPARK_VERSION ++$TRAVIS_SCALA_VERSION clean coverage test it:test
  sbt coverageReport
fi

# sbt ++$TRAVIS_SCALA_VERSION scalastyle
# sbt ++$TRAVIS_SCALA_VERSION "test:scalastyle"
# sbt ++$TRAVIS_SCALA_VERSION  "it:scalastyle"
