# Import package pyspark.sql untuk mengakses fungsi Spark SQL
from pyspark.sql import SparkSession

# Buat sebuah SparkSession
spark = SparkSession.builder \
    .appName("Load MySQL to DWH") \
    .getOrCreate()

# URL JDBC untuk container MySQL dan Postgres
mysql_url = "jdbc:mysql://mysql-ds9:3306/home-credit-application"
postgres_url = "jdbc:postgresql://postgres-ds9:5432/postgres"

# properties untuk container MySQL dan Postgres
mysql_connection_properties = {
    "user": "faris",
    "password": "faris",
    "driver": "com.mysql.cj.jdbc.Driver"
}

postgres_connection_properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

# Definisikan table
tables = ["application_test", "application_train"]

for table in tables:
    try:
        cursor = spark.read.jdbc(url=mysql_url, table=table, properties=mysql_connection_properties)
        cursor.write.jdbc(url=postgres_url, table=table, properties=postgres_connection_properties, mode="overwrite")
    except:
        print(f"gagal melakukan Load table {table}")