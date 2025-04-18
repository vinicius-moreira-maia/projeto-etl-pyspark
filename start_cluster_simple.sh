#!/bin/bash

SPARK_HOME="/home/vmm/spark/spark-3.3.2-bin-hadoop3"

echo "Iniciando nó mestre..."
"$SPARK_HOME/sbin/start-master.sh"

# Espera o master iniciar
sleep 2

# Pega a URL do master a partir do log
MASTER_LOG=$(ls -t "$SPARK_HOME"/logs/spark-*-org.apache.spark.deploy.master*.out 2>/dev/null | head -1)
MASTER_URL=$(grep -o 'spark://[^ ]*' "$MASTER_LOG" | tail -1)

if [ -z "$MASTER_URL" ]; then
  echo "Erro: não foi possível obter a URL do master."
  exit 1
fi

echo "Master iniciado em $MASTER_URL"

echo "Iniciando worker..."
"$SPARK_HOME/sbin/start-worker.sh" "$MASTER_URL"

echo "Cluster iniciado com 1 master e 1 worker. Acesse http://localhost:8080"