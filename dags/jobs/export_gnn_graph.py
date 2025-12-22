from pyspark.sql import SparkSession, functions as F

def export_gnn_graph(spark: SparkSession,src_table: str = "hive_cat.features.transaction_features",ns: str = "hive_cat.graph") -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")

    df = spark.table(src_table)

    df_all = (
        df
        .withColumn("transaction_id_str", F.col("transaction_id").cast("string"))
        .withColumn("customer_id_str", F.col("customer_id").cast("string"))
        .withColumn("card_id_str", F.col("card_id").cast("string"))
        .withColumn("device_id_str", F.col("device_id").cast("string"))
        .withColumn("email_str", F.col("email").cast("string"))
    )

    df_train = (
        df_all
        .filter(F.col("dataset") == "train")
        .withColumn("label", F.col("is_fraud").cast("int"))
    )

    def write_iceberg(df_out, table_name: str, mode: str = "createOrReplace"):
        full = f"{ns}.{table_name}"
        w = df_out.writeTo(full).using("iceberg") \
            .tableProperty("write.format.default", "parquet") \
            .tableProperty("format-version", "2")
        if mode == "append":
            w.append()
        else:
            w.createOrReplace()

    # ---------- nodes ----------
    customers = (
        df_all.select(F.col("customer_id_str").alias("customer_id"))
        .where(F.col("customer_id_str").isNotNull())
        .distinct()
    )
    write_iceberg(customers, "nodes_customers")

    cards = (
        df_all.select(F.col("card_id_str").alias("card_id"))
        .where(F.col("card_id_str").isNotNull())
        .distinct()
    )
    write_iceberg(cards, "nodes_cards")

    devices = (
        df_all.select(F.col("device_id_str").alias("device_id"))
        .where(F.col("device_id_str").isNotNull())
        .distinct()
    )
    write_iceberg(devices, "nodes_devices")

    emails = (
        df_all.select(F.col("email_str").alias("email"))
        .where(F.col("email_str").isNotNull())
        .distinct()
    )
    write_iceberg(emails, "nodes_emails")

    txns = (
        df_all.select(F.col("transaction_id_str").alias("transaction_id"))
        .where(F.col("transaction_id_str").isNotNull())
        .distinct()
    )
    write_iceberg(txns, "nodes_transactions")

    # ---------- labales (only train) ----------
    txn_labels = (
        df_train
        .select(
            F.col("transaction_id_str").alias("transaction_id"),
            F.col("label"),
        )
        .where(F.col("transaction_id_str").isNotNull())
    )
    write_iceberg(txn_labels, "labels_transactions")

    # ---------- edges ----------
    edges_cust_card = (
        df_all.select(
            F.col("customer_id_str").alias("customer_id"),
            F.col("card_id_str").alias("card_id"),
        )
        .where(F.col("customer_id_str").isNotNull() & F.col("card_id_str").isNotNull())
        .distinct()
    )
    write_iceberg(edges_cust_card, "edges_customer_card")
    edges_card_device = (
        df_all.select(
            F.col("card_id_str").alias("card_id"),
            F.col("device_id_str").alias("device_id"),
        )
        .where(F.col("card_id_str").isNotNull() & F.col("device_id_str").isNotNull())
        .distinct()
    )
    write_iceberg(edges_card_device, "edges_card_device")

    edges_card_email = (
        df_all.select(
            F.col("card_id_str").alias("card_id"),
            F.col("email_str").alias("email"),
        )
        .where(F.col("card_id_str").isNotNull() & F.col("email_str").isNotNull())
        .distinct()
    )
    write_iceberg(edges_card_email, "edges_card_email")

    edges_cust_txn = (
        df_all.select(
            F.col("customer_id_str").alias("customer_id"),
            F.col("transaction_id_str").alias("transaction_id"),
        )
        .where(F.col("customer_id_str").isNotNull() & F.col("transaction_id_str").isNotNull())
        .distinct()
    )
    write_iceberg(edges_cust_txn, "edges_customer_txn")

    edges_txn_device = (
        df_all.select(
            F.col("transaction_id_str").alias("transaction_id"),
            F.col("device_id_str").alias("device_id"),
        )
        .where(F.col("transaction_id_str").isNotNull() & F.col("device_id_str").isNotNull())
        .distinct()
    )
    write_iceberg(edges_txn_device, "edges_txn_device")

def main():
    spark = (SparkSession.builder.appName("export_gnn_graph").getOrCreate())
    try:
        export_gnn_graph(spark, src_table="hive_cat.features.transaction_features", ns="hive_cat.graph")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()