from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from scripts.bucket_utils import clear_buckets
from scripts.download_epc import download_epc_file_list
from scripts.unzip_epc import extract_file_list
from scripts.stream_epc_to_bucket import stream_epc_files_to_bucket
from scripts.stream_ppd_to_bucket import stream_ppd_to_bucket
from scripts.bucket_to_dwh import bucket_to_dwh
from scripts.save_csv import save_csv
from dag_configs import dag_configs
from datetime import datetime
import pickle

region = 'uk-london-1'
namespace = 'lrqgbz9z6zlj'
bucket_name = 'london-property-sales-price'
ppd_download_bucket_folder = 'ppd-download-chunks/'
epc_download_bucket_folder = 'epc-download-chunks/'
ppd_download_local_folder = '/data/ppd_cache/'
epc_download_local_folder = '/data/epc_cache/'
epc_file_path = dag_configs["epc_file_path"]#["epc_file_test"]
with open(epc_file_path, "rb") as f:
    epc_files = pickle.load(f)
ppd_csv_url = dag_configs["ppd_csv_url_complete"]#["ppd_csv_url_month"]
chunk_size = 100_000
save_as_table_name = "london"
write_mode = dag_configs["write_mode"]
local_save_path = '/data/output/london-sales-price.csv'



dag = DAG(
    dag_id='complete_fill',
    start_date=datetime(2024, 1, 1),
    schedule=None
    )

bucket_folder_list = [ppd_download_bucket_folder, epc_download_bucket_folder]
t_clear_bucket = PythonOperator(
    task_id='clear_bucket',
    python_callable=clear_buckets,
    op_kwargs={
            "region":region,
            "namespace":namespace,
            "bucket_name":bucket_name,
            "bucket_folder_list":bucket_folder_list
        },
    dag = dag
    )                  


local_folder_list = [ppd_download_local_folder, epc_download_local_folder]
t_clear_local_folder = BashOperator(
    task_id='clear_local',
    bash_command = f"rm -rf {ppd_download_local_folder} {epc_download_local_folder}",
    dag = dag
    )

t_download_epc = PythonOperator(
    task_id='download_epc',
    python_callable=download_epc_file_list,
    op_kwargs={
            "local_folder":epc_download_local_folder, 
            "file_list":epc_files, 
            "chunk_size":chunk_size
        },
    dag = dag
    )

t_unzip_epc = PythonOperator(
    task_id='unzip_epc',
    python_callable=extract_file_list,
    op_kwargs={
            "local_folder":epc_download_local_folder, 
            "file_list":epc_files
        },
    dag = dag
    )

t_stream_epc_to_bucket = PythonOperator(
    task_id='stream_epc_to_bucket',
    python_callable=stream_epc_files_to_bucket,
    op_kwargs={
            "file_list": epc_files,
            "region": region, 
            "namespace": namespace, 
            "bucket_name": bucket_name,
            "epc_local_folder": epc_download_local_folder,
            "epc_bucket_folder": epc_download_bucket_folder,
            "chunk_size": chunk_size
        },
    dag = dag
    )

t_stream_ppd_to_bucket = PythonOperator(
    task_id='stream_ppd_to_bucket',
    python_callable=stream_ppd_to_bucket,
    op_kwargs={
            "namespace":namespace,
            "bucket_name":bucket_name,
            "local_folder":ppd_download_local_folder ,
            "bucket_folder":ppd_download_bucket_folder,
            "csv_url":ppd_csv_url,
            "chunk_size":chunk_size
        },
    dag = dag
    )


t_bucket_to_dwh = PythonOperator(
    task_id='bucket_to_dwh',
    python_callable=bucket_to_dwh,
    op_kwargs={
            "region":region,
            "namespace":namespace,
            "bucket_name":bucket_name,
            "ppd_download_bucket_folder": ppd_download_bucket_folder,
            "epc_download_bucket_folder": epc_download_bucket_folder,
            "save_as_table_name": save_as_table_name,
            "write_mode": write_mode
        },
    dag = dag
    )

t_save_csv = PythonOperator(
    task_id='save_csv',
    python_callable=save_csv,
    op_kwargs={
            "table_name": save_as_table_name,
            "local_save_path": local_save_path
        },
    dag = dag
    )

t_clear_bucket_end = PythonOperator(
    task_id='clear_bucket_end',
    python_callable=clear_buckets,
    op_kwargs={
            "region": region,
            "namespace": namespace,
            "bucket_name": bucket_name,
            "bucket_folder_list": bucket_folder_list
        },
    dag = dag
    )

t_clear_local_folder_end = BashOperator(
    task_id='clear_local_end',
    bash_command = f"rm -rf {ppd_download_local_folder} {epc_download_local_folder}",
    dag = dag
    )

t_clear_bucket >> t_clear_local_folder >> t_download_epc >> t_unzip_epc >> t_stream_epc_to_bucket >> t_stream_ppd_to_bucket >> t_bucket_to_dwh >> t_save_csv >> t_clear_bucket_end >> t_clear_local_folder_end