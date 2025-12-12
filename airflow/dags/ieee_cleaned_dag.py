from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from pyspark.sql import SparkSession, functions as F
import datetime as dt
import socket

default_args = {
    "owner": "vlad",
    "depends_on_past": False,
    "retries": 0,
}

CATALOG = "hive_cat"
RAW_NS = "raw"
CLEANED_NS = "cleaned"


def get_spark():
    """SparkSession с Iceberg + Hive Metastore + MinIO."""
    hostname = socket.gethostname()
    print(f"Container hostname: {hostname}")

    spark = (
        SparkSession.builder
        .appName("ieee_raw_to_cleaned")
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
        # Iceberg + Hive
        .config("spark.sql.catalog.hive_cat", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hive_cat.type", "hive")
        .config("spark.sql.catalog.hive_cat.uri", "thrift://hive-metastore:9083")
        .config("spark.sql.catalog.hive_cat.warehouse", "s3a://warehouse/")
        # MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    # твои настройки спарка
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    spark.conf.set("spark.sql.files.maxRecordsPerFile", "500000")
    # spark.conf.set("parquet.enable.dictionary", "false")

    print("Spark version:", spark.version)
    print("Spark master:", spark.sparkContext.master)
    return spark


def load_raw_to_cleaned_internal(spark: SparkSession) -> None:
    """Твой исходный код трансформации raw → cleaned."""

    train_txn = spark.table(f"{CATALOG}.{RAW_NS}.train_transaction")
    train_id = spark.table(f"{CATALOG}.{RAW_NS}.train_identity")
    test_txn = spark.table(f"{CATALOG}.{RAW_NS}.test_transaction")
    test_id = spark.table(f"{CATALOG}.{RAW_NS}.test_identity")

    base_epoch = int(dt.datetime(2017, 1, 1, 0, 0, 0).timestamp())

    def build_view(df_txn, df_id, dataset_label: str, has_target: bool):
        df = (
            df_txn.alias("t")
            .join(df_id.alias("i"), on="TransactionID", how="left")
            .withColumn(
                "timestamp",
                F.to_timestamp(F.col("TransactionDT").cast("long") + F.lit(base_epoch))
            )
            .withColumnRenamed("TransactionID", "transaction_id")
            .withColumnRenamed("TransactionAmt", "amount")
        )

        if has_target and "isFraud" in df.columns:
            df = df.withColumnRenamed("isFraud", "is_fraud")
        else:
            df = df.withColumn("is_fraud", F.lit(None).cast("int"))

        df = (
            df
            .withColumn("customer_id", F.col("card1").cast("string"))
            .withColumn(
                "card_id",
                F.concat_ws(
                    "-",
                    F.col("card1").cast("string"),
                    F.col("card2").cast("string"),
                    F.col("card3").cast("string"),
                )
            )
            .withColumn("country", F.col("addr2").cast("string"))
            .withColumn("region", F.col("addr1").cast("string"))
            .withColumn("device_id", F.coalesce(F.col("DeviceInfo"), F.lit("unknown")))
            .withColumn(
                "channel",
                F.when(F.col("DeviceType") == "mobile", "MOBILE")
                .when(F.col("DeviceType") == "desktop", "WEB")
                .otherwise("UNKNOWN")
            )
            .withColumn("email", F.col("P_emaildomain"))
            .withColumn("dataset", F.lit(dataset_label))
            .withColumn("event_date", F.to_date(F.col("timestamp")))
        )

        important_cols = [
            "transaction_id",
            "timestamp",
            "event_date",
            "amount",
            "customer_id",
            "card_id",
            "country",
            "region",
            "device_id",
            "channel",
            "email",
            "is_fraud",
            "dataset",
        ]

        cols_to_keep = [c for c in important_cols if c in df.columns]
        df = df.select(*cols_to_keep)

        return df

    train_clean = build_view(train_txn, train_id, dataset_label="train", has_target=True)
    test_clean = build_view(test_txn, test_id, dataset_label="test", has_target=False)

    # namespace cleaned
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{CLEANED_NS}")

    (
        train_clean
        .repartition("event_date")
        .writeTo(f"{CATALOG}.{CLEANED_NS}.transactions")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("format-version", "2")
        .partitionedBy("event_date")
        .createOrReplace()
    )

    # В оригинале у тебя .append(), но тогда таблица должна существовать заранее.
    # Для простоты делаю также createOrReplace — можно вернуть .append(), если сам создашь таблицу.
    (
        test_clean
        .repartition("event_date")
        .writeTo(f"{CATALOG}.{CLEANED_NS}.ieee_cis_transactions")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("format-version", "2")
        .partitionedBy("event_date")
        .createOrReplace()
    )


def load_raw_to_cleaned_task():
    """Обёртка для Airflow: создаёт/гасит SparkSession."""
    spark = get_spark()
    try:
        load_raw_to_cleaned_internal(spark)
    finally:
        spark.stop()
        print("Spark session stopped")


with DAG(
        dag_id="ieee_cleaned_dag",
        description="Преобразование raw-таблиц IEEE в cleaned-слой Iceberg",
        default_args=default_args,
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["ieee", "cleaned", "spark", "iceberg"],
) as dag:

    raw_to_cleaned = PythonOperator(
        task_id="raw_to_cleaned",
        python_callable=load_raw_to_cleaned_task,
    )
