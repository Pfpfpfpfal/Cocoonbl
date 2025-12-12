from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

default_args = {
    "owner": "vlad",
    "depends_on_past": False,
    "retries": 0,
}

CATALOG = "hive_cat"
CLEANED_NS = "cleaned"
FEATURES_NS = "features"


def get_spark():
    """SparkSession с Iceberg + Hive Metastore + MinIO."""
    spark = (
        SparkSession.builder
        .appName("cleaned_to_features")
        .master("local[*]")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.host", "localhost")
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


def build_features(spark: SparkSession) -> None:
    """Преобразуем данные из cleaned в features слой."""
    # Загружаем таблицу cleaned.transactions
    df = spark.table(f"{CATALOG}.{CLEANED_NS}.transactions")

    # Убираем записи с отсутствующим customer_id
    df = df.filter(F.col("customer_id").isNotNull())

    ts_long = F.col("timestamp").cast("long")

    # Окна для 1 дня и 7 дней
    w_cust_1d = (
        Window.partitionBy("customer_id")
        .orderBy(ts_long)
        .rangeBetween(-86400, 0)
    )

    w_cust_7d = (
        Window.partitionBy("customer_id")
        .orderBy(ts_long)
        .rangeBetween(-86400 * 7, 0)
    )

    w_cust_order = (
        Window.partitionBy("customer_id")
        .orderBy("timestamp")
    )

    df_feat = (
        df
        .withColumn("hour", F.hour("timestamp"))
        .withColumn(
            "is_night",
            F.when((F.col("hour") < 6) | (F.col("hour") >= 23), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn("log_amount", F.log1p(F.col("amount")))
        .withColumn("cust_txn_cnt_1d", F.count("*").over(w_cust_1d))
        .withColumn("cust_amt_sum_1d", F.sum("amount").over(w_cust_1d))
        .withColumn("cust_txn_cnt_7d", F.count("*").over(w_cust_7d))
        .withColumn("cust_amt_sum_7d", F.sum("amount").over(w_cust_7d))
        .withColumn("prev_timestamp", F.lag("timestamp").over(w_cust_order))
        .withColumn(
            "secs_since_prev_txn",
            (F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long"))
        )
    )

    cols = [
        "transaction_id",
        "timestamp",
        "event_date",
        "dataset",
        "customer_id",
        "card_id",
        "country",
        "region",
        "device_id",
        "channel",
        "email",
        "is_fraud",
        "amount",
        "log_amount",
        "hour",
        "is_night",
        "cust_txn_cnt_1d",
        "cust_amt_sum_1d",
        "cust_txn_cnt_7d",
        "cust_amt_sum_7d",
        "secs_since_prev_txn",
    ]

    df_feat = df_feat.select(*cols)

    # Записываем в features-слой в Iceberg
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{FEATURES_NS}")

    (
        df_feat
        .repartition("event_date")
        .writeTo(f"{CATALOG}.{FEATURES_NS}.transaction_features")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("format-version", "2")
        .partitionedBy("event_date")
        .createOrReplace()
    )


def load_cleaned_to_features_task():
    """Обёртка для Airflow: создаёт/гасит SparkSession."""
    spark = get_spark()
    try:
        build_features(spark)
    finally:
        spark.stop()
        print("Spark session stopped")


with DAG(
        dag_id="ieee_features_dag",
        description="Преобразование cleaned-данных в features-слой Iceberg",
        default_args=default_args,
        schedule_interval=None,  # Запуск вручную
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["ieee", "features", "spark", "iceberg"],
) as dag:

    raw_to_features = PythonOperator(
        task_id="cleaned_to_features_task",
        python_callable=load_cleaned_to_features_task,
    )
