from pyspark.sql import SparkSession

CATALOG = "hive_cat"

def get_spark(app_name: str):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


spark = get_spark("raw_to_clean")
from pyspark.sql import SparkSession, functions as F
import datetime as dt


def load_raw_to_cleaned(spark: SparkSession) -> None:

    train_txn = spark.table("hive_cat.raw.train_transaction")
    train_id = spark.table("hive_cat.raw.train_identity")
    test_txn = spark.table("hive_cat.raw.test_transaction")
    test_id = spark.table("hive_cat.raw.test_identity")

    base_epoch = int(dt.datetime(2017, 1, 1, 0, 0, 0).timestamp())

    def build_view(df_txn, df_id, dataset_label: str, has_target: bool):
        df = (df_txn.alias("t")
            .join(df_id.alias("i"), on="TransactionID", how="left")
            .withColumn(
                "timestamp",
                F.to_timestamp(F.col("TransactionDT").cast("long") + F.lit(base_epoch))
            )
            .withColumnRenamed("TransactionID", "transaction_id")
            .withColumnRenamed("TransactionAmt", "amount")
        )

        if has_target and "isFraud" in df.columns:
            df = df.withColumnRenamed("isFraud", "is_fraud")
        else:
            df = df.withColumn("is_fraud", F.lit(None).cast("int"))

        df = (df
            .withColumn("customer_id", F.col("card1").cast("string"))
            .withColumn(
                "card_id",
                F.concat_ws(
                    "-",
                    F.col("card1").cast("string"),
                    F.col("card2").cast("string"),
                    F.col("card3").cast("string"),
                )
            )
            .withColumn("country", F.col("addr2").cast("string"))
            .withColumn("region", F.col("addr1").cast("string"))
            .withColumn("device_id", F.coalesce(F.col("DeviceInfo"), F.lit("unknown")))
            .withColumn(
                "channel",
                F.when(F.col("DeviceType") == "mobile", "MOBILE")
                 .when(F.col("DeviceType") == "desktop", "WEB")
                 .otherwise("UNKNOWN")
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

        cols_to_keep = [c for c in important_cols if c in df.columns]
        df = df.select(*cols_to_keep)

        return df

    train_clean = build_view(train_txn, train_id, dataset_label="train", has_target=True)
    test_clean  = build_view(test_txn,  test_id,  dataset_label="test",  has_target=False)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS hive_cat.cleaned")

    (
        train_clean
        .repartition("event_date")  # repartition(64, "event_date")
        .writeTo("hive_cat.cleaned.transactions")
        .using("iceberg")
        .tableProperty("write.format.default", "parquet")
        .tableProperty("format-version", "2")
        .partitionedBy("event_date")
        .createOrReplace()
    )
    
    (
        test_clean
        .repartition("event_date")
        .writeTo("hive_cat.cleaned.ieee_cis_transactions")
        .append()
    )

if __name__ == "__main__":
    load_raw_to_cleaned(spark)

spark.stop()
