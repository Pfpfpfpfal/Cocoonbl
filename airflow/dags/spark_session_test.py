from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
import socket

default_args = {
    "owner": "vlad",
    "depends_on_past": False,
    "retries": 0,
}


def create_spark_session():
    """Минимальный тест: pyspark внутри Airflow без Iceberg."""
    hostname = socket.gethostname()
    print(f"Container hostname: {hostname}")

    spark = (
        SparkSession.builder
        .appName("airflow_spark_session_test")
        .master("local[*]")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", hostname)
        .getOrCreate()
    )

    print("Spark version:", spark.version)
    print("Spark master:", spark.sparkContext.master)

    df = spark.range(10)
    print("Count from test dataframe:", df.count())

    spark.stop()
    print("Spark session stopped")


with DAG(
        dag_id="spark_session_test",
        description="Простой тест: создание SparkSession внутри Airflow (без Iceberg)",
        default_args=default_args,
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["spark", "test"],
) as dag:
    test_spark = PythonOperator(
        task_id="create_spark",
        python_callable=create_spark_session,
    )
