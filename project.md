# projeto-etl-pyspark

# Dataset
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

# Arquitetura


# encerra o processo de todos os workers
/home/vmm/spark/spark-3.3.2-bin-hadoop3/sbin/stop-worker.sh

# encerra o master
/home/vmm/spark/spark-3.3.2-bin-hadoop3/sbin/stop-master.sh

# jps
Java Process Status é um a ferramenta da JDK que lista todos os processos java que estão rodando no sistema
Mstra os PID's (id's de processos) e os nomes das classes principais de cada processo java ativo.





✅ Como usar:
Torne o script executável:

bash
Copiar
Editar
chmod +x start-cluster.sh
Rode o script com o número de workers desejado:

bash
Copiar
Editar
./start-cluster.sh 4
