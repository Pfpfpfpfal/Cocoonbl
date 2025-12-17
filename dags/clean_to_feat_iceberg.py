from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark"

# Тот же набор, что ты уже используешь для Iceberg+MinIO
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
        dag_id="clean_to_feat_iceberg",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        tags=["spark", "iceberg", "clean", "features"],
) as dag:

    clean_to_feat = SparkSubmitOperator(
        task_id="build_transaction_features",
        conn_id=SPARK_CONN_ID,
        application="/workspace/jobs/clean_to_feat_iceberg.py",
        name="clean-to-feat-iceberg",
        deploy_mode="client",
        verbose=True,
        conf=CONF,
        packages=PACKAGES,
    )

    clean_to_feat
