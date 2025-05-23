from pyspark.sql import SparkSession
import os
import requests
import argparse

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--taxi_type', required=True)
    parser.add_argument('--year', required=True)
    parser.add_argument('--db_name', required=True)
    parser.add_argument('--db_pwd', required=True)
    parser.add_argument('--db_user', required=True)

    args = parser.parse_args()

    taxi_type = args.taxi_type
    year = args.year
    db_name = args.db_name
    db_pwd = args.db_pwd
    db_user = args.db_user
    db_table = f"public.{taxi_type}"
    
    # o master será fornecido via argumento de CLI
    spark = SparkSession.builder \
    .appName('etl-taxi-data') \
    .getOrCreate()
    
    download_parquets(taxi_type, year)
    
    df = spark.read \
         .parquet(f'data/raw/{taxi_type}/{year}/*')
         
    ingest_on_postgres(df, db_table, db_user, db_pwd, db_name)
    

def download_parquets(taxi_type: str, year: str) -> None:
    '''
    Recebe o tipo do taxi (green ou yellow) e o ano do dataset. 
    Monta as url's de acordo com os inputs recebidos. 
    Requisita os dados de todos os meses em que houve registros e escreve em arquivos .parquet.
    '''
    
    url_prefix = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    for month in range(1, 13):
        fmonth = f"{month:02d}"
        filename = f"{taxi_type}_tripdata_{year}-{fmonth}.parquet"
        url = f"{url_prefix}{filename}"

        local_prefix = f"data/raw/{taxi_type}/{year}"
        local_path = os.path.join(local_prefix, filename)

        os.makedirs(local_prefix, exist_ok=True)

        print(f"Downloading {url} to {local_path}")
        
        try:
            response = requests.get(url)
            response.raise_for_status()  # lança erro se o status não for 200
            with open(local_path, "wb") as f:
                f.write(response.content)
        except requests.exceptions.RequestException as e:
            print(f"Erro ao baixar {url}: {e}")

def ingest_on_postgres(df, table, user, pwd, db):
    '''
    Recebe o dataframe com os dados lidos dos arquivos .parquet e os dados da conexão ao banco de dados,
    para realizar a ingestão. 
    '''
    
    try:
        df.write.mode("append") \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://localhost:5432/{db}") \
        .option("user", user) \
        .option("password", pwd) \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table) \
        .option("batchsize", 10000) \
        .option("numPartitions", 8) \
        .save()
    except Exception as e:
        print("Data load error: " + str(e))
    
    rows_imported = df.count()
    print(f'imported {rows_imported} rows into table {table}')

if __name__ == "__main__":
    main()