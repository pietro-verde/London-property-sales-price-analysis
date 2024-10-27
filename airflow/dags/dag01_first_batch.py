from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import PythonOperator
from scripts.local_utils import clean_folder_list
from scripts.bucket_utils import clear_buckets
from scripts.download_epc import download_epc_file_list
from scripts.unzip_epc import extract_file_list
from scripts.stream_web_to_bucket import stream_web_to_bucket
from scripts.bucket_to_dwh import bucket_to_dwh
from dag_configs import dag_configs
from datetime import datetime
import pickle



region = 'uk-london-1'
namespace = 'lrqgbz9z6zlj'
bucket_name = 'london-property-sales-price'
ppd_download_local_folder = '/data/ppd_cache/'
epc_download_local_folder = '/data/epc_cache/'
ppd_download_bucket_folder = 'ppd-download-chunks/'
epc_download_bucket_folder = 'epc-download-chunks/'
csv_url = dag_configs["csv_url"]
chunk_size = 100_000
save_as_table_name = "london"
write_mode = dag_configs["write_mode"]
epc_file_path = dag_configs["epc_file_path"]
with open(epc_file_path, "rb") as f:
    epc_files = pickle.load(f)


dag = DAG(
    dag_id='dag01_total_fill',
    start_date=datetime(2024, 1, 1),
    schedule=None
    )

bucket_folder_list = [ppd_download_bucket_folder, epc_download_bucket_folder]
t00_clear_bucket = PythonOperator(
    task_id='s00_clear_bucket',
    python_callable=clear_buckets,
    op_kwargs={
            "region":region,
            "namespace":namespace,
            "bucket_name":bucket_name,
            "bucket_folder":bucket_folder_list
        },
    dag = dag
    )

local_folder_list = [ppd_download_local_folder, epc_download_local_folder]
t00_clear_bucket = PythonOperator(
    task_id='s01_clear_local',
    python_callable=clean_folder_list,
    op_kwargs={
            "folder_list":local_folder_list
        },
    dag = dag
    )


t01_stream_web_to_bucket = PythonOperator(
    task_id='s01_stream_web_to_bucket',
    python_callable=stream_web_to_bucket,
    op_kwargs={
            "namespace":namespace,
            "bucket_name":bucket_name,
            "local_folder":local_folder,
            "bucket_folder":bucket_folder,
            "csv_url":csv_url,
            "chunk_size":chunk_size
        },
    dag = dag
    )

t02_bucket_to_dwh = PythonOperator(
    task_id='s02_bucket_to_dwh',
    python_callable=bucket_to_dwh,
    op_kwargs={
            "region":region,
            "namespace":namespace,
            "bucket_name":bucket_name,
            "bucket_folder":bucket_folder,
            "save_as_table_name":save_as_table_name,
            "write_mode":write_mode 
        },
    dag = dag
    )


t00_clear_bucket >> t01_stream_web_to_bucket >> t02_bucket_to_dwh
