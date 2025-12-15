from pyspark.sql import SparkSession
import time

spark = (
    SparkSession.builder
    .appName("spark_hello_from_airflow")
    .getOrCreate()
)

print("=" * 60)
print("🔥 SparkSession STARTED 🔥")
print("App name:", spark.sparkContext.appName)
print("Master:", spark.sparkContext.master)
print("Spark version:", spark.version)
print("Executor memory:", spark.sparkContext.getConf().get("spark.executor.memory"))
print("=" * 60)

time.sleep(10)

spark.stop()
print("✅ SparkSession STOPPED")