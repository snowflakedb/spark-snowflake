#!/bin/bash

export SPARK_HOME=/users/spark
export SPARK_WORKDIR=/users/spark/work

# Decrept profile json
cd $SPARK_WORKDIR
bash decrypt_secret.sh $SNOWFLAKE_TEST_CONFIG $ENCRYPTED_SNOWFLAKE_TEST_CONFIG 

$SPARK_HOME/bin/spark-submit \
      --jars $SPARK_WORKDIR/spark-snowflake_2.11-2.7.0-spark_2.4.jar,$SPARK_WORKDIR/snowflake-jdbc-3.12.2.jar \
      --master spark://master:7077 --deploy-mode client \
      --class net.snowflake.spark.snowflake.ArrowPerfTest \
      $SPARK_WORKDIR/spark-snowflake-perf_2.11-2.7.0-spark_2.4.jar
