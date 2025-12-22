import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG(
        dag_id="alerts_from_s3_to_telegram",
        start_date=datetime(2025, 1, 1),
        schedule="*/10 * * * *",
        catchup=False,
        tags=["spark", "alerts", "marts", "telegram"],
) as dag:

    send_fraud_alerts = SparkSubmitOperator(
        task_id="send_fraud_alerts",
        conn_id="spark",
        application="/opt/airflow/dags/jobs/alerts_from_s3_to_telegram.py",
        env_vars={
            "TG_BOT_TOKEN": os.getenv("TG_BOT_TOKEN", ""),
            "TG_CHAT_ID": os.getenv("TG_CHAT_ID", ""),

            "SCORED_S3_PATH": os.getenv("SCORED_S3_PATH", "s3a://warehouse/marts.db/scored_transactions/data"),
            "ALERT_THRESHOLD": os.getenv("ALERT_THRESHOLD", "0.90"),
            "MAX_ALERTS": os.getenv("MAX_ALERTS", "20"),

            "STATE_S3_PATH": os.getenv("STATE_S3_PATH", "s3a://warehouse/_state/fraud_alerts_state.json"),

            "PROCESS_MODE": os.getenv("PROCESS_MODE", "incremental"),

            "SEND_START_PING": os.getenv("SEND_START_PING", "0"),

            "STATE_KEEP_DAYS": os.getenv("STATE_KEEP_DAYS", "14"),
            "STATE_KEEP_IDS_PER_DAY": os.getenv("STATE_KEEP_IDS_PER_DAY", "5000"),

        },
        conf={
            "spark.executor.cores": "1",
            "spark.cores.max": "1",
            "spark.executor.memory": "768m",
            "spark.executor.memoryOverhead": "256m",
            "spark.driver.memory": "512m",
            "spark.driver.memoryOverhead": "256m",
            "spark.sql.shuffle.partitions": "16",

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
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.367",
            ]),
        },
        verbose=True,
    )

    send_fraud_alerts