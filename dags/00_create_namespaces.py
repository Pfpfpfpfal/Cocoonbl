from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN_ID = "spark"
SPARK_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0,org.apache.hadoop:hadoop-aws:3.3.4"
SPARK_CONF = {"spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
              "spark.sql.catalog.hive_cat": "org.apache.iceberg.spark.SparkCatalog",
              "spark.sql.catalog.hive_cat.type": "hive",
              "spark.sql.catalog.hive_cat.uri": "thrift://hive-metastore:9083",
              "spark.sql.catalog.hive_cat.warehouse": "s3a://warehouse/iceberg",
              "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
              "spark.hadoop.fs.s3a.path.style.access": "true",
              "spark.hadoop.fs.s3a.access.key": "admin",
              "spark.hadoop.fs.s3a.secret.key": "password",
              "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",}

with DAG(
    dag_id="00_create_namespaces",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['spark', 'iceberg', 'setup'],
) as dag:

    SparkSubmitOperator(
        task_id="create_namespaces",
        conn_id=SPARK_CONN_ID,
        application="/opt/airflow/dags/jobs/create_namespaces.py",
        name="create-namespaces",
        deploy_mode="client",
        verbose=True,
        conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
    )
