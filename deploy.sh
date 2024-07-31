#!/bin/bash
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

if [ -z "$SONATYPE_USER" ]; then
  echo "[ERROR] Jenkins sonatype user is not specified!"
  exit 1
fi

if [ -z "$SONATYPE_PASSWORD" ]; then
  echo "[ERROR] Jenkins sonatype pwd is not specified!"
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

mkdir -p ~/.ivy2

STR=$'realm=Sonatype Nexus Repository Manager
host=oss.sonatype.org
user='$SONATYPE_USER$'
password='$SONATYPE_PASSWORD$''

echo "$STR" > ~/.ivy2/.credentials

# import private key first
if [ ! -z "$GPG_PRIVATE_KEY" ] && [ -f "$GPG_PRIVATE_KEY" ]; then
  # First check if already imported private key
  if ! gpg --list-secret-key | grep "$GPG_KEY_ID"; then
    gpg --allow-secret-key-import --import "$GPG_PRIVATE_KEY"
  fi
fi

which sbt
if [ $? -ne 0 ]
then
   pushd ..
   echo "sbt is not installed, download latest sbt for test and build"
   curl -L -o sbt-1.5.3.zip https://github.com/sbt/sbt/releases/download/v1.5.3/sbt-1.5.3.zip
   unzip sbt-1.5.3.zip
   PATH=$PWD/sbt/bin:$PATH
   popd
else
   echo "use system installed sbt"
fi
which sbt
sbt version

echo publishing main branch...
git checkout tags/$GITHUB_TAG_1
if [ "$PUBLISH" = true ]; then
  sbt +publishSigned
else
  echo "publish to $PUBLISH_S3_URL"
  rm -rf ~/.ivy2/local/
  sbt +publishLocalSigned
  aws s3 cp ~/.ivy2/local ${PUBLISH_S3_URL}/${GITHUB_TAG_1}/ --recursive
fi

if [ -n "$GITHUB_TAG_2" ]; then
  echo publishing previous_spark_version branch...
  git checkout tags/$GITHUB_TAG_2
  if [ "$PUBLISH" = true ]; then
    sbt +publishSigned
  else
    echo "publish to $PUBLISH_S3_URL"
    rm -rf ~/.ivy2/local/
    sbt +publishLocalSigned
    aws s3 cp ~/.ivy2/local ${PUBLISH_S3_URL}/${GITHUB_TAG_2}/ --recursive
  fi
fi

if [ -n "$GITHUB_TAG_3" ]; then
  echo publishing previous_spark_version branch...
  git checkout tags/$GITHUB_TAG_3
  if [ "$PUBLISH" = true ]; then
    sbt +publishSigned
  else
    echo "publish to $PUBLISH_S3_URL"
    rm -rf ~/.ivy2/local/
    sbt +publishLocalSigned
    aws s3 cp ~/.ivy2/local ${PUBLISH_S3_URL}/${GITHUB_TAG_3}/ --recursive
  fi
fi
