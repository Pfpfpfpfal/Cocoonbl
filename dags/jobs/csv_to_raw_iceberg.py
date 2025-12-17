# /opt/airflow/dags/jobs/csv_to_raw_iceberg.py  (или /workspace/jobs/csv_to_raw_iceberg.py)

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit

CATALOG = "hive_cat"
NAMESPACE = "raw"

FILES = {
    "train_transaction": "/data/train_transaction.csv",
    "train_identity": "/data/train_identity.csv",
    "test_transaction": "/data/test_transaction.csv",
    "test_identity": "/data/test_identity.csv",
}

def load_one(spark: SparkSession, table_name: str, path: str):
    full_table = f"{CATALOG}.{NAMESPACE}.{table_name}"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_source_file", lit(path))
    )

    # Пишем в Iceberg. Если таблицы нет — создаём.
    # Для Spark 3.5+ работает writeTo(...).createOrReplace()/append()
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    except Exception:
        pass

    # Если таблица уже есть — append, иначе create
    if spark.catalog.tableExists(full_table):
        df.writeTo(full_table).append()
    else:
        df.writeTo(full_table).using("iceberg").create()

    # Немного лога для контроля
    cnt = spark.table(full_table).count()
    print(f"✅ Loaded {table_name} from {path}. Total rows in table now: {cnt}")

def main():
    spark = SparkSession.builder.getOrCreate()

    for table_name, path in FILES.items():
        load_one(spark, table_name, path)

    spark.stop()

if __name__ == "__main__":
    main()
