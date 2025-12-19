from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="raw_to_cleaned",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "cleaned", "raw"],
) as dag:

    raw_to_cleaned = SparkSubmitOperator(
        task_id="raw_to_cleaned",
        conn_id="spark",
        application="/opt/airflow/dags/jobs/raw_to_cleaned.py",
        verbose=True,
        conf={
            "spark.executor.cores": "1",
            "spark.cores.max": "2",
            "spark.executor.memory": "1536m",
            "spark.driver.memory": "1536m",

            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

            "spark.sql.catalog.hive_cat": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.hive_cat.type": "hive",
            "spark.sql.catalog.hive_cat.uri": "thrift://hive-metastore:9083",
            "spark.sql.catalog.hive_cat.warehouse": "s3a://warehouse/",
            "spark.sql.catalog.hive_cat.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",

            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "password",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        packages=(
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
            "org.apache.iceberg:iceberg-aws-bundle:1.10.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.367"
        ),
    )

    raw_to_cleaned