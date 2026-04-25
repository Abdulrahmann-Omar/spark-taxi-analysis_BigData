#!/bin/bash

echo "Configuring Spark Cluster..."

export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g
export SPARK_DRIVER_MEMORY=2g

echo "Cores: $SPARK_WORKER_CORES"
echo "Worker Memory: $SPARK_WORKER_MEMORY"
echo "Driver Memory: $SPARK_DRIVER_MEMORY"

echo "Cluster configuration applied"
