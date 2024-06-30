#!/bin/bash

start-master.sh -p 7077
start-worker.sh spark://spark-postgres:7077
start-history-server.sh


# Entrypoint, for example notebook, pyspark or spark-sql
if [[ $# -gt 0 ]] ; then
    eval "$@"
else
    # Keep the container running
    tail -f /dev/null
fi