#!/bin/bash

SPARK_HOME="/home/vmm/spark/spark-3.3.2-bin-hadoop3"

echo "iniciando master..."
"$SPARK_HOME/sbin/start-master.sh"

# aguarda o master iniciar antes de iniciar o worker
sleep 2

# 'ls -t' lista os arquivos de log a partir das datas de modificação, da mais recente pra mais antiga
# o pipe '|' passa a saída para o head, que retorna o primeiro nome de arquivo da lista
MASTER_LOG=$(ls -t "$SPARK_HOME"/logs/spark-*-org.apache.spark.deploy.master*.out | head -1)

# '-z' checa se a string é vazia ou nula
if [ -z "$MASTER_LOG" ]; then
    echo "não foram encontrados logs do master em $SPARK_HOME/logs"
    exit 1
fi

# usando 'grep' com regex para pegar o padrão de url do master do arquivo de log mais recente
# o pipe '|' passa a saída e pega a última linha, que é a url mais recente
MASTER_URL=$(grep -o 'spark://[^ ]*' "$MASTER_LOG" | tail -1)

if [ -z "$MASTER_URL" ]; then
  echo "não foi possível obter a url do master"
  exit 1
fi

echo "master iniciado: $MASTER_URL"

echo "iniciando worker..."
"$SPARK_HOME/sbin/start-worker.sh" "$MASTER_URL"

echo "cluster iniciado com 1 master e 1 worker: http://localhost:8080"