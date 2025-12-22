import os
import re
import tempfile
import requests
import pandas as pd
import boto3

HTTP_BASE = "http://host.docker.internal:8000"
FILES = [
    "train_transaction.csv",
    "train_identity.csv",
    "test_transaction.csv",
    "test_identity.csv",
]

S3_ENDPOINT = "http://minio:9000"
BUCKET = "warehouse"
PREFIX = "raw.db/"

ACCESS_KEY = "admin"
SECRET_KEY = "password"

CHUNK_ROWS = 50_000

def _safe_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", name)


def _download_stream_to_file(url: str, out_path: str, timeout=300):
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

def csv_to_parquet_chunks_and_upload():
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name="us-east-1",
    )

    s3.head_bucket(Bucket=BUCKET)

    for fname in FILES:
        url = f"{HTTP_BASE}/{fname}"
        base = _safe_name(fname.replace(".csv", ""))

        print(f"Downloading {url}")

        with tempfile.TemporaryDirectory() as td:
            csv_path = os.path.join(td, f"{base}.csv")
            _download_stream_to_file(url, csv_path)

            print(f"Converting CSV -> Parquet chunks: {csv_path}")

            reader = pd.read_csv(csv_path, chunksize=CHUNK_ROWS)

            part = 0
            total_rows = 0
            for chunk_df in reader:
                part += 1
                rows = len(chunk_df)
                total_rows += rows

                parquet_file = os.path.join(td, f"{base}.part-{part:05d}.parquet")

                chunk_df.to_parquet(parquet_file, index=False, engine="pyarrow", compression="snappy")

                s3_key = f"{PREFIX}{base}/part-{part:05d}.snappy.parquet"
                with open(parquet_file, "rb") as f:
                    s3.upload_fileobj(f, BUCKET, s3_key)

                print(f"Uploaded: s3://{BUCKET}/{s3_key} (rows={rows})")

            print(f"Done {fname}: total_rows={total_rows}, parts={part}")

def task_callable(**_context):
    csv_to_parquet_chunks_and_upload()