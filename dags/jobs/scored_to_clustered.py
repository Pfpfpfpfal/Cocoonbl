from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import BisectingKMeans


def run(spark: SparkSession, k: int = 6) -> None:
    df = (
        spark.table("hive_cat.marts.scored_transactions")
        .select(
            "transaction_id",
            "log_amount",
            F.log10(F.col("secs_since_prev_txn") + F.lit(1)).alias("log_secs_prev"),
            F.log10(F.col("cust_txn_cnt_7d") + F.lit(1)).alias("log_txn_cnt_7d"),
            "hour",
            "is_night",
            "fraud_score",
            "fraud_label",
        )
        .na.fill(0)
    )

    assembler = VectorAssembler(
        inputCols=["log_amount", "log_secs_prev", "log_txn_cnt_7d", "hour", "is_night"],
        outputCol="features_raw",
    )
    df2 = assembler.transform(df)

    scaler = StandardScaler(inputCol="features_raw", outputCol="features")
    model_scaler = scaler.fit(df2)
    df3 = model_scaler.transform(df2)

    kmeans = BisectingKMeans(k=k, featuresCol="features", predictionCol="cluster")
    model_kmeans = kmeans.fit(df3)

    df_clusters = model_kmeans.transform(df3)

    (
        df_clusters.select(
            "transaction_id",
            "cluster",
            "log_amount",
            "log_secs_prev",
            "log_txn_cnt_7d",
            "hour",
            "is_night",
            "fraud_score",
            "fraud_label",
        )
        .writeTo("hive_cat.marts.scored_transactions_clustered")
        .using("iceberg")
        .createOrReplace()
    )


def main():
    spark = SparkSession.builder.appName("scored_to_clustered").getOrCreate()
    try:
        run(spark, k=6)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()