from pyspark.sql import SparkSession, functions as F
import datetime as dt


def build_clean(df_txn, df_id, dataset_label: str, has_target: bool):
    base_epoch = int(dt.datetime(2017, 1, 1, 0, 0, 0).timestamp())

    df = (
        df_txn.alias("t")
        .join(df_id.alias("i"), on="TransactionID", how="left")
        .withColumn(
            "timestamp",
            F.to_timestamp(F.col("TransactionDT").cast("long") + F.lit(base_epoch)),
        )
        .withColumnRenamed("TransactionID", "transaction_id")
        .withColumnRenamed("TransactionAmt", "amount")
    )

    if has_target and "isFraud" in df.columns:
        df = df.withColumnRenamed("isFraud", "is_fraud")
    else:
        df = df.withColumn("is_fraud", F.lit(None).cast("int"))

    df = (
        df.withColumn("customer_id", F.col("card1").cast("string"))
        .withColumn(
            "card_id",
            F.concat_ws(
                "-",
                F.col("card1").cast("string"),
                F.col("card2").cast("string"),
                F.col("card3").cast("string"),
            ),
        )
        .withColumn("country", F.col("addr2").cast("string"))
        .withColumn("region", F.col("addr1").cast("string"))
        .withColumn("device_id", F.coalesce(F.col("DeviceInfo"), F.lit("unknown")))
        .withColumn(
            "channel",
            F.when(F.col("DeviceType") == "mobile", "MOBILE")
             .when(F.col("DeviceType") == "desktop", "WEB")
             .otherwise("UNKNOWN"),
        )
        .withColumn("email", F.col("P_emaildomain"))
        .withColumn("dataset", F.lit(dataset_label))
        .withColumn("event_date", F.to_date(F.col("timestamp")))
    )

    important_cols = [
        "transaction_id",
        "timestamp",
        "event_date",
        "amount",
        "customer_id",
        "card_id",
        "country",
        "region",
        "device_id",
        "channel",
        "email",
        "is_fraud",
        "dataset",
    ]

    return df.select(*important_cols)

def main():
    spark = SparkSession.builder.appName("raw_to_cleaned").getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", "64")
    spark.conf.set("spark.sql.files.maxRecordsPerFile", "500000")

    RAW_BASE = "s3a://warehouse/raw.db"

    train_txn = spark.read.parquet(f"{RAW_BASE}/train_transaction/")
    train_id = spark.read.parquet(f"{RAW_BASE}/train_identity/")
    test_txn = spark.read.parquet(f"{RAW_BASE}/test_transaction/")
    test_id = spark.read.parquet(f"{RAW_BASE}/test_identity/")

    train_clean = build_clean(train_txn, train_id, "train", has_target=True)
    test_clean = build_clean(test_txn,  test_id,  "test",  has_target=False)

    (
        train_clean
        .writeTo("hive_cat.cleaned.transactions")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    (
        test_clean
        .writeTo("hive_cat.cleaned.transactions")
        .append()
    )

    spark.stop()

if __name__ == "__main__":
    main()