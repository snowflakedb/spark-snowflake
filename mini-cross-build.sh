#!/bin/bash
set -ex

spark_versions=(
  3.5.0
  4.0.0
  4.1.0
)

# sbt doesn't read the JAVA_HOME env variable.
SBT_EXTRA=""
if [ ! -z "$JAVA_HOME" ]; then
  SBT_EXTRA="-java-home $JAVA_HOME"
fi

# Avoid m2 race conditions by running update in serial.
for spark_version in "${spark_versions[@]}"
do
  echo "Updating $spark_version"
  sbt $SBT_EXTRA -DsparkVersion=$spark_version +update
done

# Build and publish in parallel
for spark_version in "${spark_versions[@]}"
do
  echo "Building $spark_version"
  build_dir="/tmp/spark-snowflake-$spark_version-build"
  rm -rf "${build_dir}"
  mkdir -p "${build_dir}"
  cp -af ./ "${build_dir}"
  cd ${build_dir}
  sleep 2
  nice -n 10 sbt $SBT_EXTRA -DsparkVersion=$spark_version version clean +publishSigned sonaUpload sonaRelease &
  cd -
done
wait
echo $?
