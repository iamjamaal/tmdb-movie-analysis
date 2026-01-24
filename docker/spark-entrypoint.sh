#!/bin/bash

set -e

# Wait for Spark master if this is a worker
if [ "$SPARK_MODE" = "worker" ]; then
    echo "Waiting for Spark master at $SPARK_MASTER_URL..."
    sleep 10
fi

# Start Spark based on mode (in foreground)
if [ "$SPARK_MODE" = "master" ]; then
    echo "Starting Spark Master..."
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
        --host $SPARK_MASTER_HOST \
        --port $SPARK_MASTER_PORT \
        --webui-port $SPARK_MASTER_WEBUI_PORT
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "Starting Spark Worker..."
    exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        $SPARK_MASTER_URL \
        --memory $SPARK_WORKER_MEMORY \
        --cores $SPARK_WORKER_CORES
else
    echo "Error: SPARK_MODE must be set to 'master' or 'worker'"
    exit 1
fi
