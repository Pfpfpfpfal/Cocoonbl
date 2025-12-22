import os
import re
import time
import json
import requests
from typing import Optional, List, Tuple, Dict, Any
from pyspark.sql import SparkSession, functions as F

# ---------------------------
# Telegram
# ---------------------------
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

# ---------------------------
# Utils
# ---------------------------
def _pick_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    s = set(cols)
    for c in candidates:
        if c in s:
            return c
    return None

def _hadoop_fs(spark: SparkSession, any_path: str):
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    uri = jvm.java.net.URI(any_path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hconf)
    return jvm, fs

def _read_text(spark: SparkSession, path: str) -> Optional[str]:
    jvm, fs = _hadoop_fs(spark, path)
    p = jvm.org.apache.hadoop.fs.Path(path)
    if not fs.exists(p):
        return None
    stream = fs.open(p)
    try:
        baos = jvm.java.io.ByteArrayOutputStream()
        buf = jvm.java.lang.reflect.Array.newInstance(jvm.java.lang.Byte.TYPE, 4096)
        while True:
            n = stream.read(buf)
            if n <= 0:
                break
            baos.write(buf, 0, n)
        return baos.toString("UTF-8")
    finally:
        stream.close()

def _write_text_atomic(spark: SparkSession, path: str, text: str) -> None:
    jvm, fs = _hadoop_fs(spark, path)
    p = jvm.org.apache.hadoop.fs.Path(path)

    parent = p.getParent()
    if parent is not None and not fs.exists(parent):
        fs.mkdirs(parent)

    tmp = jvm.org.apache.hadoop.fs.Path(path + ".tmp")
    if fs.exists(tmp):
        fs.delete(tmp, True)

    out = fs.create(tmp, True)
    try:
        out.write(bytearray(text.encode("utf-8")))
    finally:
        out.close()

    if fs.exists(p):
        fs.delete(p, True)
    fs.rename(tmp, p)

def _list_event_date_partitions(spark: SparkSession, base_path: str) -> List[Tuple[str, str]]:
    jvm, fs = _hadoop_fs(spark, base_path)
    p = jvm.org.apache.hadoop.fs.Path(base_path)
    if not fs.exists(p):
        return []

    rx = re.compile(r".*event_date=(\d{4}-\d{2}-\d{2})/?$")
    out: List[Tuple[str, str]] = []

    for st in fs.listStatus(p):
        if not st.isDirectory():
            continue
        sp = st.getPath().toString()
        m = rx.match(sp)
        if not m:
            continue
        out.append((m.group(1), sp))

    out.sort(key=lambda x: x[0])
    return out

def _load_state(spark: SparkSession, state_path: str) -> Dict[str, Any]:
    raw = _read_text(spark, state_path)
    if not raw:
        return {"last_event_date": None, "sent_tx_ids": {}}
    try:
        st = json.loads(raw)
        if not isinstance(st, dict):
            return {"last_event_date": None, "sent_tx_ids": {}}
        st.setdefault("last_event_date", None)
        st.setdefault("sent_tx_ids", {})
        if not isinstance(st["sent_tx_ids"], dict):
            st["sent_tx_ids"] = {}
        return st
    except Exception:
        return {"last_event_date": None, "sent_tx_ids": {}}

def _trim_state(state: Dict[str, Any], keep_days: int = 14, keep_ids_per_day: int = 5000) -> Dict[str, Any]:
    sent = state.get("sent_tx_ids", {})
    if not isinstance(sent, dict):
        state["sent_tx_ids"] = {}
        return state

    dates = sorted(sent.keys())
    if len(dates) > keep_days:
        for d in dates[: len(dates) - keep_days]:
            sent.pop(d, None)

    for d, ids in list(sent.items()):
        if isinstance(ids, list) and len(ids) > keep_ids_per_day:
            sent[d] = ids[-keep_ids_per_day:]
        elif not isinstance(ids, list):
            sent[d] = []

    state["sent_tx_ids"] = sent
    return state

def main() -> None:
    spark = SparkSession.builder.appName("alerts_from_scored_to_telegram").getOrCreate()

    try:
        scored_base_path = os.getenv(
            "SCORED_S3_PATH",
            "s3a://warehouse/marts.db/scored_transactions/data"
        ).rstrip("/")

        threshold = float(os.getenv("ALERT_THRESHOLD", "0.90"))
        max_alerts = int(os.getenv("MAX_ALERTS", "20"))

        event_date_env = os.getenv("EVENT_DATE")

        process_mode = os.getenv("PROCESS_MODE", "incremental").strip().lower()

        state_path = os.getenv(
            "STATE_S3_PATH",
            "s3a://warehouse/_state/fraud_alerts_state.json"
        ).strip()

        send_start_ping = os.getenv("SEND_START_PING", "0") in ("1", "true", "True", "yes", "Y")

        if send_start_ping:
            _tg_send("Fraud alerts job started")

        state = _load_state(spark, state_path)
        last_event_date = state.get("last_event_date")
        sent_tx_ids: Dict[str, List[str]] = state.get("sent_tx_ids", {})
        if not isinstance(sent_tx_ids, dict):
            sent_tx_ids = {}
            state["sent_tx_ids"] = sent_tx_ids

        if event_date_env:
            partitions = [(event_date_env, f"{scored_base_path}/event_date={event_date_env}")]
        else:
            partitions = _list_event_date_partitions(spark, scored_base_path)
            if not partitions:
                _tg_send(f"No partitions found under:\n{scored_base_path}")
                return

            if process_mode != "all" and last_event_date:
                partitions = [(d, p) for (d, p) in partitions if d > last_event_date]

        if not partitions:
            print("No new partitions to process. last_event_date=", last_event_date)
            return

        any_new_alerts = False
        newest_processed_date = last_event_date

        for part_date, part_path in partitions:
            print("Reading partition:", part_path)

            df = spark.read.parquet(part_path)

            if df.limit(1).count() == 0:
                print("Empty partition:", part_date)
                newest_processed_date = max(newest_processed_date, part_date) if newest_processed_date else part_date
                continue

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
                    "Fraud alerts job error\n"
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
                newest_processed_date = max(newest_processed_date, part_date) if newest_processed_date else part_date
                continue

            already = set(sent_tx_ids.get(part_date, []))
            new_rows = [r for r in alerts if str(r.transaction_id) not in already]

            if not new_rows:
                print("All alerts already sent for", part_date)
                newest_processed_date = max(newest_processed_date, part_date) if newest_processed_date else part_date
                continue

            any_new_alerts = True

            _tg_send(
                "FRAUD ALERTS (new)\n"
                f"new_count: {len(new_rows)}\n"
                f"date: {part_date}\n"
                f"threshold: {threshold}\n"
                f"path: {part_path}"
            )

            for r in new_rows:
                _tg_send(
                    "FRAUD ALERT\n"
                    f"tx: {r.transaction_id}\n"
                    f"score: {r.fraud_score}\n"
                    f"label: {r.fraud_label}\n"
                    f"date: {part_date}"
                )
                already.add(str(r.transaction_id))

            sent_tx_ids[part_date] = list(already)
            newest_processed_date = max(newest_processed_date, part_date) if newest_processed_date else part_date

        if newest_processed_date:
            state["last_event_date"] = newest_processed_date
        state["sent_tx_ids"] = sent_tx_ids
        state = _trim_state(state, keep_days=int(os.getenv("STATE_KEEP_DAYS", "14")), keep_ids_per_day=int(os.getenv("STATE_KEEP_IDS_PER_DAY", "5000")))
        _write_text_atomic(spark, state_path, json.dumps(state, ensure_ascii=False))

        if any_new_alerts:
            print("Sent new alerts. State saved to:", state_path)
        else:
            print("No new alerts. State saved to:", state_path)

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
