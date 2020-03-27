#!/bin/bash
set -e

## Defaults
#
: ${SPARK_HOME:?must be set!}

## Load spark-env.sh Spark environment configuration variables
SPARK_CONF_DIR="${SPARK_CONF_DIR:-"${SPARK_HOME}/conf"}"
[ ! -f ${SPARK_CONF_DIR}/spark-env.sh ] || . ${SPARK_CONF_DIR}/spark-env.sh

uname -a

## Use the given domain name or hostname for naming a Spark node
#
if [ -n "${SPARK_DOMAIN}" ]; then
  host_opts="-h $(hostname -s).${SPARK_DOMAIN}"
elif [ -n "${SPARK_HOSTNAME}" ]; then
  host_opts="-h ${SPARK_HOSTNAME}"
fi

## Invocation shortcut commands
#
cmd=$1
case $cmd in
master)
    shift
    # ${SPARK_HOME}/sbin/start-master.sh $@
    # tail -f /dev/null
    set -- ${SPARK_HOME}/bin/spark-class "org.apache.spark.deploy.${cmd}.${cmd^}" $@
    exec $@ $host_opts
  ;;
worker)
    shift
    # ${SPARK_HOME}/sbin/start-slave.sh $@
    # tail -f /dev/null
    set -- ${SPARK_HOME}/bin/spark-class "org.apache.spark.deploy.${cmd}.${cmd^}" $@
    exec $@ $host_opts
  ;;
spark-shell|shell)
    shift
    exec ${SPARK_HOME}/bin/spark-shell $@
  ;;
spark-submit)
    shift
    pwd && ls -al
    # decrept profile json
    bash decrypt_secret.sh $SNOWFLAKE_TEST_CONFIG $ENCRYPTED_SNOWFLAKE_TEST_CONFIG 
    # exec gpg --quiet --batch --yes --decrypt --passphrase="$SNOWFLAKE_TEST_CONFIG_SECRET" \
    #         --output $SNOWFLAKE_TEST_CONFIG $ENCRYPTED_SNOWFLAKE_TEST_CONFIG
    echo "step 2"
    ls -al 
    ls ${SPARK_HOME}/bin/spark-submit 
    echo "step 3"
    sleep 10
    exec ${SPARK_HOME}/bin/spark-submit $@
    tail -f /dev/null
  ;;
*)
    # Run an arbitary command
    [ -n "$*" ] && exec $@ || exec /bin/bash
  ;;
esac
