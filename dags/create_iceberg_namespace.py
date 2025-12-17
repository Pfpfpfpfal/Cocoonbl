from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
        dag_id="create_iceberg_namespace",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        tags=["spark", "iceberg"],
) as dag:

    create_namespace = SparkSubmitOperator(
        task_id="create_namespace",
        conn_id="spark",  # у тебя Spark://7077
        application="/opt/airflow/dags/jobs/create_namespace.py",
        name="create-namespace",
        verbose=True,
        deploy_mode="client",
        # на всякий случай дублируем ключевые конфиги каталога прямо тут,
        # чтобы job точно использовал hive_cat даже если defaults не доехали
        conf={
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.hive_cat": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.hive_cat.type": "hive",
            "spark.sql.catalog.hive_cat.uri": "thrift://hive-metastore:9083",
            "spark.sql.catalog.hive_cat.warehouse": "s3a://warehouse/iceberg",
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "password",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        },
        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0",
    )
