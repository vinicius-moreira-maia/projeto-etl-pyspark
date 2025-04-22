#!/bin/bash

SPARK_HOME="/home/vmm/spark/spark-3.3.2-bin-hadoop3"

echo "parando worker..."
"$SPARK_HOME/sbin/stop-worker.sh"

echo "parando master..."
"$SPARK_HOME/sbin/stop-master.sh"

echo "cluster encerrado"
