from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
    dag_id="init_namespaces",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "init"],
) as dag:

    init_namespaces = SparkSubmitOperator(
    task_id="init_namespaces",
    conn_id="spark",
    application="/opt/airflow/dags/jobs/init_namespaces.py",
    conf={
        "spark.executor.cores": "1",
        "spark.cores.max": "1",
        "spark.executor.memory": "512m",
        "spark.driver.memory": "512m",

        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",

        "spark.sql.catalog.hive_cat": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.hive_cat.type": "hive",
        "spark.sql.catalog.hive_cat.uri": "thrift://hive-metastore:9083",
        "spark.sql.catalog.hive_cat.warehouse": "s3a://warehouse/",
        "spark.sql.catalog.hive_cat.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",

        "spark.sql.catalog.hive_cat.s3.endpoint": "http://minio:9000",
        "spark.sql.catalog.hive_cat.s3.path-style-access": "true",
        "spark.sql.catalog.hive_cat.s3.access-key-id": "admin",
        "spark.sql.catalog.hive_cat.s3.secret-access-key": "password",
        "spark.sql.catalog.hive_cat.s3.region": "us-east-1",

        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.access.key": "admin",
        "spark.hadoop.fs.s3a.secret.key": "password",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

        "spark.jars.packages": ",".join([
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0",
            "org.apache.iceberg:iceberg-aws-bundle:1.10.0",
            "org.postgresql:postgresql:42.7.3",
        ]),
    },
    verbose=True,
)

    init_namespaces