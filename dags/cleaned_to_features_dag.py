from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

AWS_REGION = "us-east-1"

with DAG(
        dag_id="cleaned_to_features",
        start_date=datetime(2025, 1, 1),
        schedule=None,
        catchup=False,
        tags=["spark", "features", "cleaned", "iceberg"],
) as dag:

    cleaned_to_features = SparkSubmitOperator(
        task_id="cleaned_to_features",
        conn_id="spark",
        application="/opt/airflow/dags/jobs/cleaned_to_features.py",
        verbose=True,
        conf={
            # ресурсы
            "spark.executor.memory": "2g",
            "spark.driver.memory": "1g",
            "spark.executor.cores": "2",
            "spark.cores.max": "2",


            # Iceberg + catalog
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.catalog.hive_cat": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.hive_cat.type": "hive",
            "spark.sql.catalog.hive_cat.uri": "thrift://hive-metastore:9083",
            "spark.sql.catalog.hive_cat.warehouse": "s3a://warehouse/",

            # S3FileIO (AWS SDK v2)
            "spark.sql.catalog.hive_cat.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",

            # MinIO для Iceberg S3FileIO
            "spark.sql.catalog.hive_cat.s3.endpoint": "http://minio:9000",
            "spark.sql.catalog.hive_cat.s3.path-style-access": "true",
            "spark.sql.catalog.hive_cat.s3.access-key-id": "admin",
            "spark.sql.catalog.hive_cat.s3.secret-access-key": "password",
            "spark.sql.catalog.hive_cat.s3.region": AWS_REGION,

            # Region (env + java props) — чтобы executors не падали на commit
            "spark.driverEnv.AWS_REGION": AWS_REGION,
            "spark.executorEnv.AWS_REGION": AWS_REGION,
            "spark.hadoop.aws.region": AWS_REGION,
            "spark.driver.extraJavaOptions": f"-Daws.region={AWS_REGION}",
            "spark.executor.extraJavaOptions": f"-Daws.region={AWS_REGION}",

            # Hadoop S3A (на всякий случай)
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.access.key": "admin",
            "spark.hadoop.fs.s3a.secret.key": "password",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

            # warehouse dirs
            "spark.sql.warehouse.dir": "s3a://warehouse/",
            "spark.hadoop.hive.metastore.warehouse.dir": "s3a://warehouse/",

            # (опционально) на окнах бывает тяжело — уменьши шфл
            "spark.sql.shuffle.partitions": "200",

        },
        packages=(
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
            "org.apache.iceberg:iceberg-aws-bundle:1.10.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.367"
        ),
    )

    cleaned_to_features
