from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


def build_features(spark: SparkSession) -> None:
    # ВАЖНО: у нас namespace clean (а не cleaned)
    df = spark.table("hive_cat.clean.transactions")

    # только валидные customer_id
    df = df.filter(F.col("customer_id").isNotNull())

    ts_long = F.col("timestamp").cast("long")

    w_cust_1d = (
        Window.partitionBy("customer_id")
        .orderBy(ts_long)
        .rangeBetween(-86400, 0)
    )

    w_cust_7d = (
        Window.partitionBy("customer_id")
        .orderBy(ts_long)
        .rangeBetween(-86400 * 7, 0)
    )

    w_cust_order = (
        Window.partitionBy("customer_id")
        .orderBy("timestamp")
    )

    df_feat = (
        df
        .withColumn("hour", F.hour("timestamp"))
        .withColumn(
            "is_night",
            F.when((F.col("hour") < 6) | (F.col("hour") >= 23), F.lit(1)).otherwise(F.lit(0))
        )
        .withColumn("log_amount", F.log1p(F.col("amount")))
        .withColumn("cust_txn_cnt_1d", F.count("*").over(w_cust_1d))
        .withColumn("cust_amt_sum_1d", F.sum("amount").over(w_cust_1d))
        .withColumn("cust_txn_cnt_7d", F.count("*").over(w_cust_7d))
        .withColumn("cust_amt_sum_7d", F.sum("amount").over(w_cust_7d))
        .withColumn("prev_timestamp", F.lag("timestamp").over(w_cust_order))
        .withColumn(
            "secs_since_prev_txn",
            (F.col("timestamp").cast("long") - F.col("prev_timestamp").cast("long"))
        )
    )

    cols = [
        "transaction_id",
        "timestamp",
        "event_date",
        "dataset",
        "customer_id",
        "card_id",
        "country",
        "region",
        "device_id",
        "channel",
        "email",
        "is_fraud",
        "amount",
        "log_amount",
        "hour",
        "is_night",
        "cust_txn_cnt_1d",
        "cust_amt_sum_1d",
        "cust_txn_cnt_7d",
        "cust_amt_sum_7d",
        "secs_since_prev_txn",
    ]

    df_feat = df_feat.select(*cols)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS hive_cat.features")

    (
        df_feat
        .repartition("event_date")
        .writeTo("hive_cat.features.transaction_features")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("format-version", "2")
        .partitionedBy("event_date")
        .createOrReplace()
    )


def main():
    spark = (
        SparkSession.builder
        .appName("clean-to-feat-iceberg")
        .getOrCreate()
    )
    try:
        build_features(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
