from pyspark.sql import SparkSession, functions as F

S3_BASE = "s3a://warehouse/features.db/lgbm_data"

def export_test_dataset(spark: SparkSession) -> None:
    df = spark.table("hive_cat.features.trans_gnn")
    df_test = df.filter(F.col("dataset") == "test")

    output_path = f"{S3_BASE}/test"

    (
        df_test
        #.repartition(1)
        .write
        .mode("overwrite")
        .parquet(output_path)
    )

    print(f"Exported test dataset to: {output_path}")

def main():
    spark = SparkSession.builder.appName("export_test_dataset_to_s3").getOrCreate()
    try:
        export_test_dataset(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()