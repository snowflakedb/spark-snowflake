#!/usr/bin/env bash
#
# Run whitesource for components which need versioning
# Catch error and exit only if not -2 or 0
#
# If an error occurs, return an error code instead of exit -1
set +e
# Fail if any command in pipe fails.
set -o pipefail 

cd ${WORKSPACE}

# SCAN_DIRECTORIES is a comma-separated list (as a string) of file paths which contain all source code and build artifacts for this project
SCAN_DIRECTORIES=$PWD

# If your PROD_BRANCH is not master, you can define it here based on the need
PROD_BRANCH="master"

# PRODUCT_NAME is your team's name or overarching project name
PRODUCT_NAME="spark-snowflake"

# PROJECT_NAME is your project's name or repo name if your project spans multiple repositories
BRANCH_NAME=$(echo ${GIT_BRANCH} | cut -d'/' -f 2)

export GIT_COMMIT=`git rev-parse HEAD`

PROJECT_NAME=$BRANCH_NAME


if [[ -z "${JOB_BASE_NAME}" ]]; then
   echo "[ERROR] No JOB_BASE_NAME is set. Run this on Jenkins"
   exit 0
fi

# Download the latest whitesource unified agent to do the scanning if there is no existing one
curl -LO https://github.com/whitesource/unified-agent-distribution/releases/latest/download/wss-unified-agent.jar
if [[ ! -f "wss-unified-agent.jar" ]]; then
    echo "failed to download whitesource unified agent"
    # if you want to fail the build when failing to download whitesource scanning agent, please use exit 1 
    # exit 1
fi

# whitesource will scan the folder and detect the corresponding configuration
# configuration file wss-generated-file.config will be generated under ${SCAN_DIRECTORIES}
# java -jar wss-unified-agent.jar -detect -d ${SCAN_DIRECTORIES}
# SCAN_CONFIG="${SCAN_DIRECTORIES}/wss-generated-file.config"

# SCAN_CONFIG is the path to your whitesource configuration file
SCAN_CONFIG="whitesource/wss-sbt-agent.config"

if [ "BRANCH_NAME" != "$PROD_BRANCH" ]; then
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -d ${SCAN_DIRECTORIES} \
        -projectVersion ${GIT_COMMIT}
    ERR=$?
    if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
        echo "failed to run whitesource scanning with feature branch"
        # if you want to fail the build when failing to run whitesource with projectName feature branch, please use exit 1 
        # exit 1
    fi 
else
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -d ${SCAN_DIRECTORIES} \
        -projectVersion ${GIT_COMMIT} \
        -offline true
        ERR=$?
        if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
            echo "failed to run whitesource scanning in offline mode"
            # if you want to fail the build when failing to run whitesource scanning with offline mode, please use exit 1 
            # exit 1
        fi
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -projectVersion baseline \
        -requestFiles whitesource/update-request.txt 
        ERR=$?
        if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
            echo "failed to run whitesource scanning with projectName baseline of master branch"
            # if you want to fail the build when failing to run whitesource with projectName baseline, please use exit 1 
            # exit 1
        fi
    java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
        -c ${SCAN_CONFIG} \
        -project ${PROJECT_NAME} \
        -product ${PRODUCT_NAME} \
        -projectVersion ${GIT_COMMIT} \
        -requestFiles whitesource/update-request.txt
        ERR=$?
        if [[ "$ERR" != "254" && "$ERR" != "0" ]]; then
            echo "failed to run whitesource scanning with projectName GIT_COMMIT of master branch"
            # if you want to fail the build when failing to run whitesource with projectName GIT_COMMIT, please use exit 1
            # exit 1
        fi 
fi

exit 0
