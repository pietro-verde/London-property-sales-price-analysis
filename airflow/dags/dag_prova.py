from airflow import DAG
from airflow.operators.python import PythonOperator
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
    dag_id='prova',
    start_date=datetime(2024, 1, 1),
    schedule=None
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


t_save_csv