from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import types
from bucket_utils import Bucket
from datetime import datetime
import os

region ='uk-london-1'
namespace = 'lrqgbz9z6zlj'
bucket_name = 'london-property-sales-price'

ppd_download_bucket_folder = 'ppd-download-chunks/'
epc_download_bucket_folder = 'epc-download-chunks/'
save_as_table_name = "london"
write_mode = "overwrite"

match_rate_path = "/data/match_rate_log.txt"

OCI_ACCESS_KEY_ID = os.environ['OCI_ACCESS_KEY_ID']
OCI_SECRET_ACCESS_KEY = os.environ['OCI_SECRET_ACCESS_KEY']
OCI_REGION = region
OCI_NAMESPACE = namespace
BUCKET_NAME = bucket_name
    
db_url = "jdbc:postgresql://pgwarehouse:5432/london"
db_properties = {
    "user": "root",
    "password": "root",
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


spark.stop()