from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from jobs.csv_to_raw import task_callable


with DAG(
    dag_id="csv_to_raw",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["raw", "s3", "csv"],
) as dag:

    upload_raw_csv = PythonOperator(
        task_id="csv_to_raw",
        python_callable=task_callable,
    )

    upload_raw_csv