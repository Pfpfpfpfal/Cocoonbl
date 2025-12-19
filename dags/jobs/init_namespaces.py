from pyspark.sql import SparkSession

namespaces = [
    "hive_cat.raw",
    "hive_cat.cleaned",
    "hive_cat.features",
    "hive_cat.graph",
    "hive_cat.marts",
]

def main():
    spark = (SparkSession.builder.appName("init_namespaces").getOrCreate())
    for ns in namespaces:
        spark.sql(f"create namespace if not exists {ns}")
        print(f"ok {ns}")
    
    spark.stop()

if __name__ == "__main__":
    main()