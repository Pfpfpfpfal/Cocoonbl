from pyspark.sql import SparkSession, functions as F
import argparse


def import_gnn_to_features(
    spark: SparkSession,
    gnn_path: str,
) -> None:
    df_gnn = spark.read.parquet(gnn_path)

    df_gnn = (
        df_gnn
        .withColumn("transaction_id_str", F.col("transaction_id").cast("string"))
        .drop("transaction_id")
    )

    df_feat = (
        spark.table("hive_cat.features.transaction_features")
        .withColumn("transaction_id_str", F.col("transaction_id").cast("string"))
    )

    # matched = df_feat.join(df_gnn, on="transaction_id_str", how="inner").count()
    # print("Matched rows by transaction_id_str:", matched)

    df_join = (
        df_feat.join(df_gnn, on="transaction_id_str", how="left")
              .drop("transaction_id_str")
    )

    fill_map = {"gnn_score": 0.0}
    for i in range(1, 17):
        col = f"gnn_emb_{i}"
        if col in df_join.columns:
            fill_map[col] = 0.0

    df_join = df_join.na.fill(fill_map)

    (
        df_join
        .repartition("event_date")
        .writeTo("hive_cat.features.trans_gnn")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("format-version", "2")
        .partitionedBy("event_date")
        .createOrReplace()
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--gnn-path",
        default="s3a://warehouse/features.db/gnn_features/txn_gnn_features.parquet",
        help="Path to GNN parquet on S3 (use s3a://...)",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("import_gnn_to_features").getOrCreate()
    try:
        import_gnn_to_features(spark, gnn_path=args.gnn_path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()