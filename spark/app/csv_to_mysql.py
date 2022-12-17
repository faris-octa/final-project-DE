# Import package yang diperlukan
from pyspark.sql import SparkSession, DataFrameWriter
#from pyspark.sql import DataFrameWriter

import os

# Buat sebuah SparkSession
spark = SparkSession.builder \
    .appName("Import_CSV_to_MySQL") \
    .getOrCreate()

# Tentukan nama folder yang berisi file-file csv
folder_name = '/usr/local/spark/resources/data'

# Dapatkan daftar file csv dalam folder
csv_files = [file for file in os.listdir(folder_name) if file.endswith(".csv")]

# Buat dictionary untuk menyimpan dataframe
dataframes = {}

# Loop melalui setiap file csv dan baca ke dalam dataframe
for file in csv_files:
  dataframes[file.split(".")[0]] = spark.read.csv(os.path.join(folder_name, file), 
                                                  inferSchema=True, 
                                                  header=True)


# URL JDBC ke database credit
jdbc_url = "jdbc:mysql://mysql-ds9:3306/home-credit-application"
connection_properties = {
    "user": "faris",
    "password": "faris",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# mendapatkan url host MySQL: 
# docker inspect <mysql-container> | grep "\"IPAddress\""

# Iterasi untuk menyimpan DataFrame ke dalam tabel
for df_name, df in dataframes.items():
    # Simpan DataFrame ke dalam tabel yang sama dengan nama kunci dictionary
    df.write.jdbc(url=jdbc_url, 
                  table=df_name, 
                  mode="overwrite", 
                  properties=connection_properties)
    
# menambahkan connector JDBC ke classpath:
# export SPARK_CLASSPATH=/usr/local/spark/jars/mysql-connector-j-8.0.31.jar