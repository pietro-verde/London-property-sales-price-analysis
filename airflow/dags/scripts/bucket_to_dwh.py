from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import types
from scripts.bucket_utils import Bucket
from datetime import datetime
import os
import pickle

def bucket_to_dwh(region,
                  namespace, 
                  bucket_name,
                  ppd_download_bucket_folder,
                  epc_download_bucket_folder,
                  save_as_table_name,
                  write_mode):

    match_rate_path = "/data/match_rate_log.txt"

    index_bounds_path = "/data/index-bound.pkl"
    
    OCI_ACCESS_KEY_ID = os.environ['OCI_ACCESS_KEY_ID']
    OCI_SECRET_ACCESS_KEY = os.environ['OCI_SECRET_ACCESS_KEY']
    OCI_REGION = region
    OCI_NAMESPACE = namespace
    BUCKET_NAME = bucket_name
    DWH_USER = os.environ["DWH_USER"]
    DWH_PASSWORD = os.environ["DWH_PASSWORD"]
    DWH_DB = os.environ["DWH_DB"]
    
    db_url = f"jdbc:postgresql://pgwarehouse:5432/{DWH_DB}"
    db_properties = {
        "user": DWH_USER,
        "password": DWH_PASSWORD,
        "driver": "org.postgresql.Driver"
    }


    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('Transform') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.506') \
        .config('spark.hadoop.fs.s3a.endpoint', f'https://{OCI_NAMESPACE}.compat.objectstorage.{OCI_REGION}.oraclecloud.com') \
        .config('spark.hadoop.fs.s3a.access.key', OCI_ACCESS_KEY_ID) \
        .config('spark.hadoop.fs.s3a.secret.key', OCI_SECRET_ACCESS_KEY) \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'true') \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
        .getOrCreate()


    ppd_path = f's3a://{BUCKET_NAME}/{ppd_download_bucket_folder}/*.parquet'
    ppd = spark.read.parquet(ppd_path, header=True, inferSchema=True)
    
    bucket_session = Bucket(OCI_REGION, OCI_NAMESPACE, bucket_name)
    objects = bucket_session.list_objects()
    
    folders = [x for x in objects if epc_download_bucket_folder in x]
    
    folders = set([x.split("/")[1] for x in folders])
    
    epc_path = f"s3a://{BUCKET_NAME}/"
    
    epc_file_paths = [f"{epc_path}/{epc_download_bucket_folder}/{folder}/*.parquet" for folder in folders]
    
    epc = spark.read.parquet(*epc_file_paths)
    
    epc = epc.withColumn("TOTAL_FLOOR_AREA", F.col("TOTAL_FLOOR_AREA").cast(types.IntegerType()))
    epc = epc.na.fill({"TOTAL_FLOOR_AREA": 0})
    
    def create_key(df, column_list):
        new_cols = []
        for col in column_list:
            new_col = col+"_fmt"
            df = df.withColumn(new_col, F.nvl(col, F.lit("")))
            df = df.withColumn(new_col, F.trim(new_col))
            df = df.withColumn(new_col, F.lower(new_col))
            df = df.withColumn(new_col, F.regexp_replace(new_col," ", ""))
            df = df.withColumn(new_col, F.regexp_replace(new_col,",", ""))
            new_cols.append(new_col)
        df = df.withColumn("KEY", F.concat(*new_cols))
        df = df.na.fill({"KEY": ""})
        df = df.drop(*new_cols)
        return df
    
    ppd_cols_keys = ["SAON", "PAON", "STREET","POSTCODE"]
    ppd = create_key(ppd, ppd_cols_keys)
    
    epc_cols_keys = ['ADDRESS', 'POSTCODE']
    epc = create_key(epc, epc_cols_keys)
    
    # DEDUPE EPC
    # For addresses that have multiple values for "TOTAL_FLOOR_AREA", keep only the highest.
    max_floor_area = epc.groupby('key').agg(F.max("TOTAL_FLOOR_AREA").alias("TOTAL_FLOOR_AREA"))
    epc = epc.join(max_floor_area, on=['key', 'TOTAL_FLOOR_AREA'])[['key', 'TOTAL_FLOOR_AREA']].distinct()
    
    data = ppd.join(epc, how='left', on='key')
    
    match_agg = data.groupby(data['TOTAL_FLOOR_AREA'].isNotNull().alias("is_match")).count()
    cnt_match = match_agg[match_agg["is_match"]==True].collect()[0][1]
    cnt_tot = data.count()
    match_rate = cnt_match/cnt_tot
    
    timestamp = str(datetime.today()).split(".")[0]
    with open(match_rate_path, "a") as f:
        f.write(f"\n{timestamp}|{match_rate:.2f}")
    
    data = data.withColumn("PARTIAL_POSTCODE", F.split_part(F.col('postcode'), F.lit(' '), F.lit(1)))
    
    data = data.withColumn(
        "PROPERTY_TYPE",
        F.when(data["PROPERTY_TYPE"] == "D", "Detached")
         .when(data["PROPERTY_TYPE"] == "S", "Semi-Detached")
         .when(data["PROPERTY_TYPE"] == "T", "Terraced")
         .when(data["PROPERTY_TYPE"] == "F", "Flats/Maisonettes")
         .when(data["PROPERTY_TYPE"] == "O", "Other")
         .otherwise(data["PROPERTY_TYPE"])
    )
    
    data = data.withColumn(
        "OLD_NEW",
        F.when(data["OLD_NEW"] == "Y", "newly built")
         .when(data["OLD_NEW"] == "N", "old")
         .otherwise(data["OLD_NEW"])
    )
    
    data = data.withColumn(
        "DURATION",
        F.when(data["DURATION"] == "F", "Freehold")
         .when(data["DURATION"] == "L", "Leasehold")
         .otherwise(data["DURATION"])
    )
    
    data = data.drop('key')
    
    data = data.na.fill({"TOTAL_FLOOR_AREA": 0})
    
    data = data.withColumn('DATE_OF_TRANSFER',F.col('DATE_OF_TRANSFER').cast(types.DateType()))
    data = data.withColumn('PRICE',F.col('PRICE').cast(types.FloatType()))
    
    data = data.withColumn("__index__", F.monotonically_increasing_id())
    data = data.drop('__index_level_0__')

    index_bounds = data.select(
                F.min("__index__").alias('MIN_INDEX'),
                F.max("__index__").alias('MAX_INDEX')
            ).collect()
    
    bounds_list = [index_bounds[0][0], index_bounds[0][1]]
    with open(index_bounds_path, "wb") as f:
        pickle.dump(bounds_list, f)

    data.write.jdbc(url=db_url, table=save_as_table_name, mode=write_mode, properties=db_properties)
    
    spark.stop()