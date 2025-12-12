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

# 4 CSV-файла внутри контейнера Airflow
FILES = {
    "train_transaction": "/data/train_transaction.csv",
    "train_identity": "/data/train_identity.csv",
    "test_transaction": "/data/test_transaction.csv",
    "test_identity": "/data/test_identity.csv",
}

CATALOG = "hive_cat"
NAMESPACE = "raw"


def get_spark():
    hostname = socket.gethostname()
    print(f"Container hostname: {hostname}")

    spark = (
        SparkSession.builder
        .appName("csv_to_iceberg_all")
        .master("local[*]")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", hostname)
        .config(
            "spark.jars",
            ",".join([
                "/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar",
                "/opt/spark/jars/hadoop-aws-3.3.4.jar",
                "/opt/spark/jars/aws-java-sdk-bundle-1.12.767.jar",
            ])
        )
        .config("spark.sql.catalog.hive_cat", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hive_cat.type", "hive")
        .config("spark.sql.catalog.hive_cat.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.hive_cat.warehouse", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    print("Spark version:", spark.version)
    print("Spark master:", spark.sparkContext.master)
    return spark


def csv_to_iceberg_all():
    spark = get_spark()

    # namespace raw, если вдруг ещё не создан
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

    for table_name, csv_path in FILES.items():
        full_table_name = f"{CATALOG}.{NAMESPACE}.{table_name}"
        print(f"\n=== Loading {csv_path} → {full_table_name} ===")

        df = (
            spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(csv_path)
        )

        print("Schema:")
        df.printSchema()
        print("Row count:", df.count())

        (
            df.writeTo(full_table_name)
            .using("iceberg")
            .createOrReplace()
        )

        print(f"Written Iceberg table: {full_table_name}")

    print(f"\nShow tables in {CATALOG}.{NAMESPACE}:")
    spark.sql(f"SHOW TABLES IN {CATALOG}.{NAMESPACE}").show(truncate=False)

    spark.stop()
    print("Spark session stopped")


with DAG(
        dag_id="csv_to_iceberg",
        description="Чтение 4 CSV из локальной FS Airflow и запись в Iceberg (MinIO)",
        default_args=default_args,
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["spark", "csv", "iceberg", "minio"],
) as dag:
    load_csv = PythonOperator(
        task_id="csv_to_iceberg_task",
        python_callable=csv_to_iceberg_all,
    )
