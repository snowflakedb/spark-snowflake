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
    # start-master.sh will execute Master in background and the container ends
    # So, below is used
    set -- ${SPARK_HOME}/bin/spark-class "org.apache.spark.deploy.master.Master" $@
    exec $@ $host_opts
  ;;
worker)
    shift
    # start-slave.sh will execute Master in background and the container ends
    # So, below is used
    set -- ${SPARK_HOME}/bin/spark-class "org.apache.spark.deploy.worker.Worker" $@
    exec $@ $host_opts
  ;;
runtest)
    shift
    # Run test
    exec ${SPARK_HOME}/work/run_test.sh $@
  ;;
spark-shell|shell)
    shift
    exec ${SPARK_HOME}/bin/spark-shell $@
  ;;
*)
    # Run an arbitary command
    [ -n "$*" ] && exec $@ || exec /bin/bash
  ;;
esac
