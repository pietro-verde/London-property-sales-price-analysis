from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import pickle



def save_csv(table_name, local_save_path):

    index_bounds_path = "/data/index-bound.pkl"
    with open(index_bounds_path, "rb") as f:
        index_bounds = pickle.load(f)

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
    
    # data = spark.read.jdbc(url=db_url, table=table_name, properties=db_properties)

    data = spark.read.jdbc(url=db_url, 
                           table=table_name,
                           properties=db_properties,
                           column="__index__",
                           lowerBound=index_bounds[0],
                           upperBound=index_bounds[1],
                           numPartitions=100
                      )
    
    folder = os.path.dirname(local_save_path)
    if not os.path.exists(folder):
        os.makedirs(folder, mode=0o777)


    # data.toPandas().to_csv(local_save_path,index=None)
    data.write.mode("overwrite").csv(local_save_path, header=True)
    
    
    spark.stop()