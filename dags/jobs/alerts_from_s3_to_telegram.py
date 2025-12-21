import os
import re
import time
import requests
from typing import Optional, List, Tuple

from pyspark.sql import SparkSession, functions as F


def _tg_send(text: str) -> None:
    token = os.getenv("TG_BOT_TOKEN")
    chat_id = os.getenv("TG_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("Set TG_BOT_TOKEN and TG_CHAT_ID env vars")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
    }

    last_err = None
    for _ in range(3):
        try:
            r = requests.post(url, json=payload, timeout=15)
            print("TG response:", r.status_code, r.text)
            r.raise_for_status()
            return
        except Exception as e:
            last_err = e
            time.sleep(2)

    raise last_err


def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    s = set(cols)
    for c in candidates:
        if c in s:
            return c
    return None


def _latest_event_date_partition(spark: SparkSession, base_path: str) -> Optional[Tuple[str, str]]:
    """
    base_path: s3a://.../data
    returns: (latest_date, latest_partition_path) like:
      ("2018-01-31", "s3a://.../data/event_date=2018-01-31")
    """
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()

    uri = jvm.java.net.URI(base_path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hconf)

    p = jvm.org.apache.hadoop.fs.Path(base_path)
    if not fs.exists(p):
        return None

    statuses = fs.listStatus(p)
    rx = re.compile(r".*event_date=(\d{4}-\d{2}-\d{2})/?$")

    best_date = None
    best_path = None

    for st in statuses:
        if not st.isDirectory():
            continue
        sp = st.getPath().toString()
        m = rx.match(sp)
        if not m:
            continue
        d = m.group(1)
        if (best_date is None) or (d > best_date):
            best_date = d
            best_path = sp

    if best_date is None or best_path is None:
        return None
    return best_date, best_path


def main() -> None:
    spark = SparkSession.builder.appName("alerts_from_scored_to_telegram").getOrCreate()

    try:
        _tg_send("✅ DAG ping: job started")

        scored_base_path = os.getenv(
            "SCORED_S3_PATH",
            "s3a://warehouse/marts.db/scored_transactions/data"
        ).rstrip("/")

        threshold = float(os.getenv("ALERT_THRESHOLD", "0.90"))
        max_alerts = int(os.getenv("MAX_ALERTS", "20"))
        event_date_env = os.getenv("EVENT_DATE")  # optional YYYY-MM-DD

        # 1) выбираем партицию, чтобы не сканить все 183 папки
        if event_date_env:
            latest_date = event_date_env
            part_path = f"{scored_base_path}/event_date={latest_date}"
        else:
            latest = _latest_event_date_partition(spark, scored_base_path)
            if latest is None:
                _tg_send(f"⚠️ No partitions found under:\n{scored_base_path}")
                return
            latest_date, part_path = latest

        print("Reading partition:", part_path)

        # 2) читаем только одну партицию
        df = spark.read.parquet(part_path)

        # НИКАКИХ df.rdd / RDD actions -> не поднимется python-worker на executor’ах
        if df.limit(1).count() == 0:
            _tg_send(f"ℹ️ scored_transactions empty for {latest_date}")
            return

        # 3) маппинг колонок (на случай разных неймингов)
        col_txn = _pick_col(df.columns, ["transaction_id", "TransactionID", "txn_id", "tx_id"])
        col_score = _pick_col(df.columns, ["fraud_score", "score", "prediction", "probability", "proba"])
        col_label = _pick_col(df.columns, ["fraud_label", "label", "is_fraud", "isFraud", "target"])

        missing = [k for k, v in {
            "transaction_id": col_txn,
            "fraud_score": col_score,
            "fraud_label": col_label,
        }.items() if v is None]

        if missing:
            _tg_send(
                "⚠️ Fraud alerts job error\n"
                f"Missing columns: {', '.join(missing)}\n"
                f"Path: {part_path}\n"
                f"Columns: {df.columns}"
            )
            return

        df2 = (
            df
            .withColumnRenamed(col_txn, "transaction_id")
            .withColumnRenamed(col_score, "fraud_score")
            .withColumnRenamed(col_label, "fraud_label")
            .withColumn("fraud_score", F.col("fraud_score").cast("double"))
            .withColumn("fraud_label", F.col("fraud_label").cast("int"))
        )

        alerts_df = (
            df2
            .filter((F.col("fraud_label") == 1) | (F.col("fraud_score") >= F.lit(threshold)))
            .select("transaction_id", "fraud_score", "fraud_label")
            .dropDuplicates(["transaction_id"])
            .orderBy(F.col("fraud_score").desc_nulls_last())
            .limit(max_alerts)
        )

        alerts = alerts_df.collect()

        if not alerts:
            _tg_send(
                "✅ Fraud check: nothing suspicious\n"
                f"date: {latest_date}\n"
                f"threshold: {threshold}\n"
                f"path: {part_path}"
            )
            return

        _tg_send(
            "🚨 FRAUD ALERTS\n"
            f"count: {len(alerts)}\n"
            f"date: {latest_date}\n"
            f"threshold: {threshold}\n"
            f"path: {part_path}"
        )

        for r in alerts:
            _tg_send(
                "🚨 FRAUD ALERT\n"
                f"tx: {r.transaction_id}\n"
                f"score: {r.fraud_score}\n"
                f"label: {r.fraud_label}"
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
