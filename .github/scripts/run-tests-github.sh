#!/usr/bin/env bash

set -x -e

# It is used to workaround below spark test issue:
#   java.net.BindException: Cannot assign requested address:
#   Service 'sparkDriver' failed after 16 retries (on a random free port)!
#   Consider explicitly setting the appropriate binding address for the service
#   'sparkDriver' (for example spark.driver.bindAddress for SparkDriver) to
#   the correct binding address.
export SPARK_LOCAL_IP=127.0.0.1

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

# If credentials file is missing (forks, external PRs), run unit tests only
if [ "$INTEGRATION_TESTS" == "true" ] && [ ! -f "snowflake.travis.json" ]; then
  echo ""
  echo "========================================================================"
  echo "WARNING: Snowflake credentials not available (snowflake.travis.json"
  echo "         not found). This is expected for forks and external PRs."
  echo ""
  echo "         Integration tests are SKIPPED — do NOT treat this as a full"
  echo "         green build."
  echo "========================================================================"
  echo ""

  # Signal to the workflow that integration tests were skipped
  if [ -n "$GITHUB_OUTPUT" ]; then
    echo "INTEGRATION_TESTS_SKIPPED=true" >> "$GITHUB_OUTPUT"
  fi

  # Run unit tests only
  sbt -DsparkVersion=$SPARK_VERSION "++$SPARK_SCALA_VERSION!" clean coverage test coverageReport
  exit 0
fi

if [ "$INTEGRATION_TESTS" != "true" ]; then
  # Run only test
  # Use ++! to force Scala version even when crossScalaVersions is empty (Spark 4.x)
  sbt -DsparkVersion=$SPARK_VERSION "++$SPARK_SCALA_VERSION!" clean coverage test coverageReport
else
  # Run both test and it
  sbt -DsparkVersion=$SPARK_VERSION "++$SPARK_SCALA_VERSION!" clean coverage test it:test coverageReport
fi
