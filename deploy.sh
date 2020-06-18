#!/bin/bash -e
#
# Push the Spark Connector to the public maven repository
# This script needs to be executed by snowflake jenkins job
#

if [ -z "$GPG_KEY_ID" ]; then
  echo "[ERROR] Key Id not specified!"
  exit 1
fi

if [ -z "$GPG_KEY_PASSPHRASE" ]; then
  echo "[ERROR] GPG passphrase is not specified for $GPG_KEY_ID!"
  exit 1
fi

if [ -z "$GPG_PRIVATE_KEY" ]; then
  echo "[ERROR] GPG private key file is not specified!"
  exit 1
fi

if [ -z "$JENKINS_BINTRAY_USER" ]; then
  echo "[ERROR] Jenkins bintry user is not specified!"
  exit 1
fi

if [ -z "$JENKINS_BINTRAY_KEY" ]; then
  echo "[ERROR] Jenkins bintry key is not specified!"
  exit 1
fi

if [ -z "$PUBLISH_S3_URL" ]; then
  echo "[ERROR] 'PUBLISH_S3_URL' is not specified!"
  exit 1
fi

if [ -z "$PUBLISH" ]; then
  echo "[ERROR] 'PUBLISH' is not specified!"
  exit 1
fi

if [ -z "$GITHUB_TAG_1" ]; then
  echo "[ERROR] 'GITHUB_TAG_1' is not specified!"
  exit 1
fi

if [ -z "$GITHUB_TAG_2" ]; then
  echo "[ERROR] 'GITHUB_TAG_2' is not specified!"
  exit 1
fi

if [ -z "$GITHUB_TAG_3" ]; then
  echo "[ERROR] 'GITHUB_TAG_3 ' is not specified!"
  exit 1
fi

STR=$'realm = Bintray API Realm\n
host = api.bintray.com\n
user = '$JENKINS_BINTRAY_USER$'\n
password = '$JENKINS_BINTRAY_KEY$'\n
organization = snowflakedb'

echo "$STR" > .bintray

# import private key first
if [ ! -z "$GPG_PRIVATE_KEY" ] && [ -f "$GPG_PRIVATE_KEY" ]; then
  # First check if already imported private key
  if ! gpg --list-secret-key | grep "$GPG_KEY_ID"; then
    gpg --allow-secret-key-import --import "$GPG_PRIVATE_KEY"
  fi
fi

# Put sbt in PATH because white source scan needs it.
export PATH=build:$PATH

echo publishing main branch...
git checkout tags/$GITHUB_TAG_1
if [ "$PUBLISH" = true ]; then
  build/sbt package
else
  echo "publish to $PUBLISH_S3_URL"
  rm -rf ~/.ivy2/local/
  build/sbt +publishLocalSigned
  aws s3 cp ~/.ivy2/local ${PUBLISH_S3_URL}/${GITHUB_TAG_1}/ --recursive
fi

echo "Run White Source scan"
whitesource/run_whitesource.sh

echo publishing previous_spark_version branch...
git checkout tags/$GITHUB_TAG_2
if [ "$PUBLISH" = true ]; then
  build/sbt package
else
  echo "publish to $PUBLISH_S3_URL"
  rm -rf ~/.ivy2/local/
  build/sbt +publishLocalSigned
  aws s3 cp ~/.ivy2/local ${PUBLISH_S3_URL}/${GITHUB_TAG_2}/ --recursive
fi

echo "Run White Source scan"
whitesource/run_whitesource.sh

echo publishing previous_spark_version branch...
git checkout tags/$GITHUB_TAG_3
if [ "$PUBLISH" = true ]; then
  build/sbt package
else
  echo "publish to $PUBLISH_S3_URL"
  rm -rf ~/.ivy2/local/
  build/sbt +publishLocalSigned
  aws s3 cp ~/.ivy2/local ${PUBLISH_S3_URL}/${GITHUB_TAG_3}/ --recursive
fi

echo "Run White Source scan"
whitesource/run_whitesource.sh

