from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os


def save_csv(table_name, local_save_path):
    db_url = "jdbc:postgresql://pgwarehouse:5432/london"
    db_properties = {
        "user": "root",
        "password": "root",
        "driver": "org.postgresql.Driver"
    }
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('ReadAndSave') \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
        .getOrCreate()
    
    data = spark.read.jdbc(url=db_url, table=table_name, properties=db_properties)
    
    folder = os.path.dirname(local_save_path)
    if not os.path.exists(folder):
        os.makedirs(folder, mode=0o777)

    data.toPandas().to_csv(local_save_path,index=None)
    
    spark.stop()