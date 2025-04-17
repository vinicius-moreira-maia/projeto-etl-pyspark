#!/bin/bash

SPARK_HOME="/home/vmm/spark/spark-3.3.2-bin-hadoop3"

# com pkill eu garanto que todos os processos dos workers sejam encerrados de uma vez
echo "Parando workers..."
pkill -f 'org.apache.spark.deploy.worker.Worker'

echo "Parando master..."
"$SPARK_HOME/sbin/stop-master.sh"

echo "Cluster encerrado."
