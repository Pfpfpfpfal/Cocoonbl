import os
from pathlib import Path
from typing import Tuple, Dict, List
import pandas as pd
import numpy as np
import torch
from torch import nn
from torch_geometric.data import HeteroData
from torch_geometric.nn import HeteroConv, SAGEConv
import trino
import boto3
from botocore.client import Config

# ---------------------------
# config
# ---------------------------
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "warehouse")
S3_KEY = os.getenv("S3_KEY", "features.db/gnn_features/txn_gnn_features.parquet")

S3_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
S3_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
REGION = "us-east-1"

TRINO_HOST = "localhost"
TRINO_PORT = 8082
TRINO_USER = "trino"
TRINO_CATALOG = "iceberg"
TRINO_SCHEMA = "graph"

MODEL_PATH = Path("../model/outputs/gnn_train/fraud_gnn.pt")

OUTPUT_DIR = Path("./outputs/gnn_infer")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_PARQUET = OUTPUT_DIR / "txn_gnn_features.parquet"

EMB_KEEP = 16

# ---------------------------
# Trino helpers
# ---------------------------
def trino_df(sql: str) -> pd.DataFrame:
    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )
    return pd.read_sql(sql, conn)

def tbl(name: str) -> str:
    return f"{name}"

def upload_to_s3(local_path: str, bucket: str, key: str) -> None:
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=REGION,
        config=Config(signature_version="s3v4"),
    )

    s3.upload_file(local_path, bucket, key)
    print(f"Uploaded to s3://{bucket}/{key}")
# ---------------------------
# Graph building
# ---------------------------
def build_id_index_map(series: pd.Series) -> Tuple[Dict[str, int], List[str]]:
    uniq = series.dropna().astype(str).unique()
    uniq_sorted = sorted(uniq.tolist())
    id2idx = {v: i for i, v in enumerate(uniq_sorted)}
    return id2idx, uniq_sorted

def ones_features(n: int, dim: int = 8) -> torch.Tensor:
    return torch.ones((n, dim), dtype=torch.float32)

def make_edge_index(
    df_edges: pd.DataFrame,
    src_col: str,
    dst_col: str,
    src_id2idx: Dict[str, int],
    dst_id2idx: Dict[str, int],
) -> torch.Tensor:
    src_raw = df_edges[src_col].astype(str)
    dst_raw = df_edges[dst_col].astype(str)

    src_idx = src_raw.map(src_id2idx)
    dst_idx = dst_raw.map(dst_id2idx)

    m = src_idx.notna() & dst_idx.notna()
    src = src_idx[m].astype(np.int64).to_numpy()
    dst = dst_idx[m].astype(np.int64).to_numpy()

    edge_index = torch.from_numpy(np.vstack([src, dst])).long()
    return edge_index

def build_hetero_data_from_trino_for_infer() -> Tuple[HeteroData, Dict[str, int]]:
    print("Reading nodes from Trino...")
    customers_df = trino_df(f"SELECT customer_id FROM {tbl('nodes_customers')}")
    cards_df     = trino_df(f"SELECT card_id FROM {tbl('nodes_cards')}")
    devices_df   = trino_df(f"SELECT device_id FROM {tbl('nodes_devices')}")
    emails_df    = trino_df(f"SELECT email FROM {tbl('nodes_emails')}")
    txns_df      = trino_df(f"SELECT transaction_id FROM {tbl('nodes_transactions')}")

    print("Building id maps...")
    cust_id2idx, cust_ids = build_id_index_map(customers_df["customer_id"])
    card_id2idx, card_ids = build_id_index_map(cards_df["card_id"])
    dev_id2idx, dev_ids   = build_id_index_map(devices_df["device_id"])
    email_id2idx, email_ids = build_id_index_map(emails_df["email"])
    txn_id2idx, txn_ids   = build_id_index_map(txns_df["transaction_id"])

    data = HeteroData()
    data["customer"].x    = ones_features(len(cust_ids))
    data["card"].x        = ones_features(len(card_ids))
    data["device"].x      = ones_features(len(dev_ids))
    data["email"].x       = ones_features(len(email_ids))
    data["transaction"].x = ones_features(len(txn_ids))
    print("Reading edges from Trino...")
    edges_cust_card_df = trino_df(f"SELECT customer_id, card_id FROM {tbl('edges_customer_card')}")
    edges_card_dev_df  = trino_df(f"SELECT card_id, device_id FROM {tbl('edges_card_device')}")
    edges_card_email_df= trino_df(f"SELECT card_id, email FROM {tbl('edges_card_email')}")
    edges_cust_txn_df  = trino_df(f"SELECT customer_id, transaction_id FROM {tbl('edges_customer_txn')}")
    edges_txn_dev_df   = trino_df(f"SELECT transaction_id, device_id FROM {tbl('edges_txn_device')}")

    print("Building edge_index tensors...")
    data["customer", "uses_card", "card"].edge_index = make_edge_index(
        edges_cust_card_df, "customer_id", "card_id", cust_id2idx, card_id2idx
    )
    data["card", "uses_device", "device"].edge_index = make_edge_index(
        edges_card_dev_df, "card_id", "device_id", card_id2idx, dev_id2idx
    )
    data["card", "uses_email", "email"].edge_index = make_edge_index(
        edges_card_email_df, "card_id", "email", card_id2idx, email_id2idx
    )
    data["customer", "made_txn", "transaction"].edge_index = make_edge_index(
        edges_cust_txn_df, "customer_id", "transaction_id", cust_id2idx, txn_id2idx
    )
    data["transaction", "on_device", "device"].edge_index = make_edge_index(
        edges_txn_dev_df, "transaction_id", "device_id", txn_id2idx, dev_id2idx
    )

    print(f"Graph ready: txn_nodes={len(txn_ids)}")
    return data, txn_id2idx

# ---------------------------
# Model
# ---------------------------
class FraudGNN(nn.Module):
    def __init__(self, metadata, hidden_dim=64):
        super().__init__()
        self.conv = HeteroConv(
            {edge_type: SAGEConv((-1, -1), hidden_dim) for edge_type in metadata[1]},
            aggr="mean",
        )
        self.lin_out = nn.Linear(hidden_dim, 1)

    def forward(self, data: HeteroData):
        x_dict = self.conv(data.x_dict, data.edge_index_dict)
        x_dict = {k: x.relu() for k, x in x_dict.items()}
        tx_x = x_dict["transaction"]
        logits = self.lin_out(tx_x).squeeze(-1)
        return logits, tx_x

def run_infer():
    device = torch.device("cpu")

    if not MODEL_PATH.exists():
        raise FileNotFoundError(f"Model not found: {MODEL_PATH.resolve()}")

    print("Loading model bundle:", MODEL_PATH)
    bundle = torch.load(MODEL_PATH, map_location="cpu")
    state_dict = bundle["state_dict"]
    metadata = bundle["metadata"]

    print("Building hetero graph (from Trino)...")
    data, txn_id2idx = build_hetero_data_from_trino_for_infer()
    data = data.to(device)

    model = FraudGNN(metadata=metadata, hidden_dim=64).to(device)
    model.load_state_dict(state_dict)
    model.eval()

    print("Running inference...")
    with torch.no_grad():
        logits, tx_emb = model(data)
        probs = torch.sigmoid(logits).cpu().numpy().astype(np.float32)
        tx_emb = tx_emb.cpu().numpy()

    idx2txn_id = {idx: tid for tid, idx in txn_id2idx.items()}
    txn_ids = [idx2txn_id[i] for i in range(len(idx2txn_id))]

    out_df = pd.DataFrame({"transaction_id": txn_ids, "gnn_score": probs})

    emb_dim = tx_emb.shape[1]
    k = min(EMB_KEEP, emb_dim)
    for i in range(k):
        out_df[f"gnn_emb_{i+1}"] = tx_emb[:, i].astype(np.float32)

    out_dir = Path("outputs") / "gnn_infer"
    out_dir.mkdir(parents=True, exist_ok=True)
    local_out = str(out_dir / "txn_gnn_features.parquet")

    out_df.to_parquet(local_out, index=False)

    upload_to_s3(local_out, S3_BUCKET, S3_KEY)

    os.remove(local_out)


if __name__ == "__main__":
    run_infer()