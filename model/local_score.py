import os
import pandas as pd
import joblib
import s3fs

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "admin"
S3_SECRET_KEY = "password"

TEST_PATH = os.getenv(
    "LGBM_TEST_PATH",
    "s3://warehouse/features.db/lgbm_data/test/",
)

SCORED_PATH = os.getenv(
    "LGBM_SCORED_PATH",
    "s3://warehouse/features.db/lgbm_data/scored/",
)

MODEL_PATH = os.getenv("LGBM_MODEL_PATH", "./ml/lgbm_fraud_gnn.pkl")

def make_fs():
    return s3fs.S3FileSystem(
        key=S3_ACCESS_KEY,
        secret=S3_SECRET_KEY,
        client_kwargs={"endpoint_url": S3_ENDPOINT},
    )

def s3_to_bucket_prefix(s3_url: str):
    if not s3_url.startswith("s3://"):
        raise ValueError(f"Expected s3://... got: {s3_url}")
    p = s3_url[len("s3://"):]
    bucket, _, rest = p.partition("/")
    return bucket, rest.rstrip("/")

def read_parquet_s3(s3_prefix_url: str) -> pd.DataFrame:
    fs = make_fs()
    bucket, prefix = s3_to_bucket_prefix(s3_prefix_url)
    path = f"{bucket}/{prefix}"

    items = fs.ls(path)
    print(f"S3 ls OK: s3://{path} -> {len(items)} objects")

    return pd.read_parquet(path, engine="pyarrow", filesystem=fs)

def write_parquet_s3(df: pd.DataFrame, s3_file_url: str) -> None:
    fs = make_fs()
    bucket, key = s3_to_bucket_prefix(s3_file_url)
    path = f"{bucket}/{key}"

    with fs.open(path, "wb") as f:
        df.to_parquet(f, index=False, engine="pyarrow")
    print(f"Uploaded parquet to: s3://{path}")

def main():
    print(f"Loading test data from: {TEST_PATH}")
    df_test = read_parquet_s3(TEST_PATH)

    print(f"Loading model from: {MODEL_PATH}")
    bundle = joblib.load(MODEL_PATH)
    model = bundle["model"]
    feature_cols = bundle["features"]

    print("Features expected by model:", len(feature_cols))

    X = df_test[feature_cols]
    fraud_proba = model.predict_proba(X)[:, 1]

    df_test["fraud_score"] = fraud_proba
    df_test["fraud_label"] = (df_test["fraud_score"] > 0.5).astype("int32")

    if "timestamp" in df_test.columns:
        df_test = df_test.drop(columns=["timestamp"])

    out_file = SCORED_PATH.rstrip("/") + "/test_scored.parquet"
    write_parquet_s3(df_test, out_file)

    print(f"Scored test saved to: {out_file}")

if __name__ == "__main__":
    main()