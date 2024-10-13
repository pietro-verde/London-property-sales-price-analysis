from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from stream_web_to_bucket import stream_web_to_bucket
from datetime import datetime

namespace = 'lrqgbz9z6zlj'
bucket_name = 'london-property-sales-price'
local_folder = 'chunks_cache/'
bucket_folder = 'chunks/'
csv_url = 'http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-monthly-update-new-version.csv'
chunk_size = 10_000

with DAG(
    dag_id='dag_total_fill',
    start_date=datetime(2022, 5, 28),
    schedule_interval=None
) as dag:

    stream_web_to_bucket = PythonOperator(
    task_id='stream_web_to_bucket',
    python_callable=stream_web_to_bucket,
    op_kwargs={
            "namespace":namespace,
            "bucket_name":bucket_name,
            "local_folder":local_folder,
            "bucket_folder":bucket_folder,
            "csv_url":csv_url,
            "chunk_size":chunk_size
        }
    )

stream_web_to_bucket