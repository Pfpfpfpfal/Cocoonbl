from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="spark_hello_test",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "smoke-test"],
) as dag:

    spark_hello = SparkSubmitOperator(
        task_id="spark_hello",
        conn_id="spark",
        application="/opt/airflow/dags/jobs/spark_hello.py",
        verbose=True,
    )

    spark_hello