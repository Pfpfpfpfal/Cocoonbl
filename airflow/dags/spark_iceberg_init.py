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


def get_spark():
    """SparkSession с Iceberg + Hive Metastore + MinIO."""
    hostname = socket.gethostname()
    print(f"Container hostname: {hostname}")

    spark = (
        SparkSession.builder
        .appName("airflow_iceberg_init")
        .master("local[*]")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", hostname)

        # Локальные JAR'ы внутри образа
        .config(
            "spark.jars",
            ",".join([
                "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
                "/opt/spark/jars/hadoop-aws-3.3.4.jar",
                "/opt/spark/jars/aws-java-sdk-bundle-1.12.767.jar",
            ])
        )

        # Iceberg + Hive Metastore
        .config("spark.sql.catalog.hive_cat", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hive_cat.type", "hive")
        .config("spark.sql.catalog.hive_cat.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.hive_cat.warehouse", "s3a://warehouse/")

        # MinIO (S3A)
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
    )

    print("Spark version:", spark.version)
    print("Spark master:", spark.sparkContext.master)
    return spark


def init_iceberg_namespaces():
    spark = get_spark()

    # создаём namespace raw
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hive_cat.raw")
    spark.sql("create NAMESPACE if not exists hive_cat.cleaned")
    spark.sql("create NAMESPACE if not exists hive_cat.features")
    spark.sql("create NAMESPACE if not exists hive_cat.marts")

    print("==== NAMESPACES IN hive_cat ====")
    spark.sql("SHOW NAMESPACES IN hive_cat").show(truncate=False)

    spark.stop()
    print("Spark session stopped")


with DAG(
        dag_id="spark_iceberg_init",
        description="Инициализация Iceberg-каталога в MinIO из Airflow (namespace raw)",
        default_args=default_args,
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["spark", "iceberg", "minio", "hive"],
) as dag:
    init_iceberg = PythonOperator(
        task_id="init_iceberg_namespaces",
        python_callable=init_iceberg_namespaces,
    )
