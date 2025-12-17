from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# То же, что в твоём демо DAG :contentReference[oaicite:2]{index=2}
PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0",
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
])

CONF = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

    "spark.sql.catalog.hive_cat": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.hive_cat.type": "hive",
    "spark.sql.catalog.hive_cat.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.hive_cat.warehouse": "s3a://warehouse/iceberg",

    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "password",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}

with DAG(
        dag_id="csv_to_raw_iceberg",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        tags=["spark", "iceberg", "raw", "csv"],
) as dag:

    load_csv_to_raw = SparkSubmitOperator(
        task_id="load_csv_to_raw",
        conn_id="spark",  # spark://spark:7077
        application="/workspace/jobs/csv_to_raw_iceberg.py",
        name="csv-to-raw-iceberg",
        deploy_mode="client",
        verbose=True,
        conf=CONF,
        packages=PACKAGES,
    )

    load_csv_to_raw
