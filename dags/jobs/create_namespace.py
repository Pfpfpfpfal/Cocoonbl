from pyspark.sql import SparkSession

CATALOG = "hive_cat"
NAMESPACE = "raw"

def main():
    spark = SparkSession.builder.getOrCreate()

    # важно: обращаемся к v2 namespace именно в каталоге hive_cat
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
    spark.sql(f"SHOW NAMESPACES IN {CATALOG}").show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
