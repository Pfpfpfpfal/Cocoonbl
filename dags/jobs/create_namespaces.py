from pyspark.sql import SparkSession

CATALOG = "hive_cat"

def get_spark(app_name: str):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


NAMESPACES = ["raw", "cleaned", "features", "marts"]

def main():
    spark = get_spark("create_namespaces")
    for ns in NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{ns}")
        print(f"✅ ensured namespace: {CATALOG}.{ns}")
    spark.stop()

if __name__ == "__main__":
    main()
