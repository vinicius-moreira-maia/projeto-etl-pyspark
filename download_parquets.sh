# indica que deve ser executado no bash
#!/bin/bash

# para executar: ./download_parquets.sh green 2021

# faz com que o script pare em caso de erros (comando com retorno diferente de 0)
set -e

# variáveis que recebem parâmetros de linha de comando
TAXI_TYPE=$1 # "yellow" (parâmetro 1)
YEAR=$2 # 2020 (parâmetro 2)

# prefixo para montar a URL
URL_PREFIX="https://d37ci6vzurychx.cloudfront.net/trip-data/"

# loop de 1 a 12
for MONTH in {1..12}; do

  # formatando o mês com 2 dígitos (usando os valores do loop)
  FMONTH=`printf "%02d" ${MONTH}`
  
  # Completando a URL com os parâmetros
  URL="${URL_PREFIX}${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet"

  # variáveis para o caminho da pasta local e nome do arquivo  
  LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}"
  LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
  LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

  echo "Downloading ${URL} to ${LOCAL_PATH}"
  
  # criando a pasta e fazendo download
  mkdir -p ${LOCAL_PREFIX}
  wget "${URL}" -O "${LOCAL_PATH}"

done
