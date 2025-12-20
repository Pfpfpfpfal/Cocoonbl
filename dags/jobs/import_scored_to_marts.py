from pyspark.sql import SparkSession, functions as F

def import_scored_to_marts(
    spark: SparkSession,
    scored_path: str = "s3a://warehouse/features.db/lgbm_data/scored/test_scored.parquet",
) -> None:
    df_scored = spark.read.parquet(scored_path)

    df_scored = (
        df_scored
        .withColumn("event_date", F.to_date(F.col("event_date")))
        .withColumn("fraud_score", F.col("fraud_score").cast("double"))
        .withColumn("fraud_label", F.col("fraud_label").cast("int"))
    )

    (
        df_scored
        .repartition("event_date")
        .writeTo("hive_cat.marts.scored_transactions")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("format-version", "2")
        .partitionedBy("event_date")
        .createOrReplace()
    )

def main():
    spark = SparkSession.builder.appName("import_scored_to_marts").getOrCreate()
    try:
        import_scored_to_marts(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()