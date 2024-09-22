from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from testoci import upload_to_oracle_storage

from datetime import datetime


with DAG(
    dag_id='first_sample_dag',
    start_date=datetime(2022, 5, 28),
    schedule_interval=None
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )
    
    create_file = BashOperator(
        task_id='create_file',
        bash_command='echo "ciaone" > /opt/airflow/test.csv'
    )

    upload_data_task = PythonOperator(
    task_id='upload_to_data_lake',
    python_callable=upload_to_oracle_storage
    )


    print_saved = BashOperator(
        task_id='print_saved',
        bash_command='echo "file in oci"'
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> create_file >> upload_data_task >> print_saved >> end_task