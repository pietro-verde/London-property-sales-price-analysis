from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os

def bucket_to_dwh(region,
                  namespace, 
                  bucket_name,
                  bucket_folder,
                  save_as_table_name,
                  write_mode):

    OCI_ACCESS_KEY_ID = os.environ['OCI_ACCESS_KEY_ID']
    OCI_SECRET_ACCESS_KEY = os.environ['OCI_SECRET_ACCESS_KEY']
    OCI_REGION = region
    OCI_NAMESPACE = namespace
    BUCKET_NAME = bucket_name
    
    from pyspark.sql import SparkSession
    import pandas as pd

    db_url = "jdbc:postgresql://pgwarehouse:5432/london"
    db_properties = {
        "user": "root",
        "password": "root",
        "driver": "org.postgresql.Driver"
    }

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('PySparkOCIConnection') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.506') \
        .config('spark.hadoop.fs.s3a.endpoint', f'https://{OCI_NAMESPACE}.compat.objectstorage.{OCI_REGION}.oraclecloud.com') \
        .config('spark.hadoop.fs.s3a.access.key', OCI_ACCESS_KEY_ID) \
        .config('spark.hadoop.fs.s3a.secret.key', OCI_SECRET_ACCESS_KEY) \
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
        .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'true') \
        .config("spark.jars", "/opt/spark/jars/postgresql-42.7.4.jar") \
        .getOrCreate()

    file_path = f's3a://{BUCKET_NAME}/{bucket_folder}/*.parquet'
    df = spark.read.parquet(file_path, header=True, inferSchema=True)

    df = df.withColumn('DATE_OF_TRANSFER', F.col("DATE_OF_TRANSFER").cast('date'))

    df_london = df[
        (df['COUNTY'] == 'GREATER LONDON')
        & (df['DATE_OF_TRANSFER'] >= '2010-01-01')
        & (df["POSTCODE"].isNotNull())
    ]

    df_london.write.jdbc(url=db_url, table=save_as_table_name, mode=write_mode, properties=db_properties)

    spark.stop()