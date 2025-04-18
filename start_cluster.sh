#!/bin/bash

SPARK_HOME="/home/vmm/spark/spark-3.3.2-bin-hadoop3"
export SPARK_WORKER_MEMORY=2g

# é necessário remover os logs antigos para não gerar conflito nas novas inicializações de clusters
echo "Limpando logs antigos..."
rm -rf "$SPARK_HOME"/logs/*  # Remove os logs antigos

# '-z "$1"' verifica se o primeiro argumento está vazio
# $0 é o primeiro comando (nome do script, no caso)
if [ -z "$1" ]; then
  echo "Uso: $0 <numero_de_workers>"
  exit 1
fi

NUM_WORKERS=$1

# nproc retorna o número de núcleos disponíveis no sistema
NUM_CORES=$(nproc)

# limite máximo de workers como 50% dos núcleos, arredondando pra cima
MAX_WORKERS=$(( (NUM_CORES + 1) / 2 ))

# devo ter menos workers do que cores disponíveis
# -gt é 'greather than'
if [ "$NUM_WORKERS" -gt "$MAX_WORKERS" ]; then
  echo "Erro: O número de workers não pode ultrapassar 50% dos núcleos disponíveis ($NUM_CORES), ou seja, no máximo $MAX_WORKERS."
  exit 2
fi

echo "Iniciando nó mestre..."
"$SPARK_HOME/sbin/start-master.sh"

# extraindo a última ocorrência da url do master no arquivo de log
sleep 2
MASTER_LOG=$(ls -t "$SPARK_HOME/logs"/spark-*-org.apache.spark.deploy.master*.out 2>/dev/null | head -1)
MASTER_URL=$(grep -o 'spark://[^ ]*' "$MASTER_LOG" | tail -1)

if [ -z "$MASTER_URL" ]; then
  echo "Erro: não foi possível obter a URL do master."
  exit 2
fi

echo "Master iniciado em $MASTER_URL"

# garante que cada worker use apenas 1 core
export SPARK_WORKER_CORES=1

# usando '&' eu garanto que os workers sejam executados em processos distintos
for i in $(seq 1 $NUM_WORKERS); do
  echo "Iniciando worker $i..."
  "$SPARK_HOME/sbin/start-worker.sh" "$MASTER_URL" &
done

# espera todos os processos dos workers serem iniciados
wait

echo "Cluster iniciado com $NUM_WORKERS workers. Acesse localhost:8080"


