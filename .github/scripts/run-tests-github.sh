#!/usr/bin/env bash

set -x -e

if [ "$SNOWFLAKE_TEST_ACCOUNT" == "gcp" -a "$SPARK_CONN_ENV_USE_COPY_UNLOAD" == "true" ]; then
  echo "Skip use_copy_unload=true for gcp"
  exit 0
fi

if [ "$SPARK_CONN_ENV_USE_COPY_UNLOAD" == "true" ]; then
  echo "Skip big data test if use_copy_unload=true"
  export SKIP_BIG_DATA_TEST=true
fi

# Enable extra test for test coverage on one platform
if [ "$SNOWFLAKE_TEST_ACCOUNT" == "aws" -a "$SPARK_CONN_ENV_USE_COPY_UNLOAD" == "false" ]; then
  echo "Enable extra test for test coverage on $SNOWFLAKE_TEST_ACCOUNT $SPARK_CONN_ENV_USE_COPY_UNLOAD"
  export EXTRA_TEST_FOR_COVERAGE=true
fi

if [ "$INTEGRATION_TESTS" != "true" ]; then
  # Run only test
  sbt -Dspark.testVersion=$SPARK_VERSION ++$SPARK_SCALA_VERSION clean coverage test
  sbt coverageReport
else
  # Run both test and it
  sbt -Dspark.version=$SPARK_VERSION ++$SPARK_SCALA_VERSION clean coverage test # it:test # test only
  sbt coverageReport
fi

