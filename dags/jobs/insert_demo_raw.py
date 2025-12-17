from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, expr

CATALOG = "hive_cat"
NAMESPACE = "raw"
TABLE = "demo_events"

def main():
    spark = SparkSession.builder.getOrCreate()

    full_table = f"{CATALOG}.{NAMESPACE}.{TABLE}"

    # 1) создаём таблицу (Iceberg) если её нет
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            event_id STRING,
            source   STRING,
            payload  STRING,
            created_at TIMESTAMP
        )
        USING iceberg
    """)

    # 2) вставляем одну запись (генерим id и timestamp)
    spark.sql(f"""
        INSERT INTO {full_table}
        SELECT
            uuid()                           AS event_id,
            'airflow'                        AS source,
            concat('hello_', cast(rand() as string)) AS payload,
            current_timestamp()              AS created_at
    """)

    # 3) покажем последние 10 строк (удобно в логах)
    spark.sql(f"""
        SELECT * FROM {full_table}
        ORDER BY created_at DESC
        LIMIT 10
    """).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
