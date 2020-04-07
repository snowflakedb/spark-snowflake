FROM ubuntu:latest

ARG SPARK_URL
ARG SPARK_BINARY_NAME
ARG JDBC_URL
ARG JDBC_BINARY_NAME
ARG SPARK_CONNECTOR_LOCATION
ARG SPARK_CONNECTOR_BINARY_NAME
ARG TEST_CASE_LOCATION
ARG TEST_CASE_BINARY_NAME
ARG ENCRYPTED_SNOWFLAKE_TEST_CONFIG
ARG SNOWFLAKE_TEST_CONFIG
ARG SPARK_ENV_SCRIPT
ARG JDBC_LOGGING_PROPERTIES
ARG RUN_TEST_SCRIPT
ARG DECRYPT_SCRIPT
ARG ENTRYPOINT_SCRIPT

ENV SPARK_URL=$SPARK_URL
ENV SPARK_BINARY_NAME=$SPARK_BINARY_NAME
ENV JDBC_URL=$JDBC_URL
ENV JDBC_BINARY_NAME=$JDBC_BINARY_NAME
ENV SPARK_CONNECTOR_LOCATION=$SPARK_CONNECTOR_LOCATION
ENV SPARK_CONNECTOR_BINARY_NAME=$SPARK_CONNECTOR_BINARY_NAME
ENV TEST_CASE_LOCATION=$TEST_CASE_LOCATION
ENV TEST_CASE_BINARY_NAME=$TEST_CASE_BINARY_NAME
ENV ENCRYPTED_SNOWFLAKE_TEST_CONFIG=$ENCRYPTED_SNOWFLAKE_TEST_CONFIG
ENV SNOWFLAKE_TEST_CONFIG=$SNOWFLAKE_TEST_CONFIG
ENV SPARK_ENV_SCRIPT=$SPARK_ENV_SCRIPT
ENV JDBC_LOGGING_PROPERTIES=$JDBC_LOGGING_PROPERTIES
ENV RUN_TEST_SCRIPT=$RUN_TEST_SCRIPT
ENV DECRYPT_SCRIPT=$DECRYPT_SCRIPT
ENV ENTRYPOINT_SCRIPT=$ENTRYPOINT_SCRIPT

#install the below packages on the ubuntu image
RUN apt-get update -qq && apt-get install -qq -y gnupg2 wget openjdk-8-jdk scala

RUN mkdir /users
WORKDIR /users
ENV SPARK_HOME=/users/spark

# Download the Spark binaries from the repo
# Untar the downloaded binaries, move them the folder name spark and
# add the spark bin on my class path
RUN wget --no-verbose $SPARK_URL 
RUN tar -xzf $SPARK_BINARY_NAME && \
    rm -fr spark*.tgz && \
    mv spark* spark && \
    echo "export PATH=$PATH:$SPARK_HOME/bin" >> ~/.bashrc

# create spark directories
RUN mkdir -p ${SPARK_HOME}/work ${SPARK_HOME}/conf

# All testing data is copied to work directory
WORKDIR ${SPARK_HOME}/work

# Download JDBC binaries
RUN wget --no-verbose $JDBC_URL

# Copy Spark Connector binaries
COPY $SPARK_CONNECTOR_LOCATION .

# Copy test cases from test case directory and un-tar it
COPY $TEST_CASE_LOCATION .
RUN tar -xvf $TEST_CASE_BINARY_NAME

# Copy encrypted test profile and decrypt script
COPY $ENCRYPTED_SNOWFLAKE_TEST_CONFIG .
COPY $DECRYPT_SCRIPT decrypt_secret.sh

# Copy run script
COPY $RUN_TEST_SCRIPT run_test.sh

# Copy entrypoint script
COPY $ENTRYPOINT_SCRIPT /entrypoint.sh

# Copy env script
COPY $SPARK_ENV_SCRIPT ${SPARK_HOME}/conf/
COPY $JDBC_LOGGING_PROPERTIES ${SPARK_HOME}/conf/

# Show all copied files
RUN ls -al /entrypoint.sh ${SPARK_HOME}/conf/ ${SPARK_HOME}/work/

# Start program
ENTRYPOINT [ "/entrypoint.sh" ]

