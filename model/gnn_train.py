import os
from pathlib import Path
import pandas as pd
import torch
from torch import nn
from torch_geometric.data import HeteroData
from torch_geometric.nn import HeteroConv, SAGEConv
from sklearn.model_selection import train_test_split

BASE_DIR = Path("../notebooks/ml_dataset/gnn_edge")
OUTPUT_DIR = Path("../model/ml/gnn_train/outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def read_parquet_dir(path: Path) -> pd.DataFrame:
    files = list(path.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files in {path}")
    dfs = [pd.read_parquet(f) for f in files]
    return pd.concat(dfs, ignore_index=True)

def build_id_index_map(series: pd.Series):
    uniq = series.dropna().unique()
    uniq_sorted = sorted(uniq)
    id2idx = {v: i for i, v in enumerate(uniq_sorted)}
    return id2idx, uniq_sorted

def build_hetero_data() -> tuple[HeteroData, pd.DataFrame]:
    customers_df = read_parquet_dir(BASE_DIR / "nodes" / "customers")
    cards_df = read_parquet_dir(BASE_DIR / "nodes" / "cards")
    devices_df = read_parquet_dir(BASE_DIR / "nodes" / "devices")
    emails_df = read_parquet_dir(BASE_DIR / "nodes" / "emails")
    txns_df = read_parquet_dir(BASE_DIR / "nodes" / "transactions")

    txn_labels_df = read_parquet_dir(BASE_DIR / "labels" / "transactions")

    cust_id2idx, cust_ids = build_id_index_map(customers_df["customer_id"])
    card_id2idx, card_ids = build_id_index_map(cards_df["card_id"])
    dev_id2idx, dev_ids = build_id_index_map(devices_df["device_id"])
    email_id2idx, email_ids = build_id_index_map(emails_df["email"])
    txn_id2idx, txn_ids = build_id_index_map(txns_df["transaction_id"])

    data = HeteroData()

    data["customer"].num_nodes    = len(cust_ids)
    data["card"].num_nodes        = len(card_ids)
    data["device"].num_nodes      = len(dev_ids)
    data["email"].num_nodes       = len(email_ids)
    data["transaction"].num_nodes = len(txn_ids)

    def ones_features(n):
        return torch.ones((n, 8), dtype=torch.float32)

    data["customer"].x = ones_features(len(cust_ids))
    data["card"].x = ones_features(len(card_ids))
    data["device"].x = ones_features(len(dev_ids))
    data["email"].x = ones_features(len(email_ids))
    data["transaction"].x = ones_features(len(txn_ids))

    edges_cust_card_df = read_parquet_dir(BASE_DIR / "edges" / "customer_card")
    src = edges_cust_card_df["customer_id"].map(cust_id2idx).dropna().astype(int)
    dst = edges_cust_card_df["card_id"].map(card_id2idx).dropna().astype(int)
    edge_index = torch.tensor([src.values, dst.values], dtype=torch.long)
    data["customer", "uses_card", "card"].edge_index = edge_index

    edges_card_dev_df = read_parquet_dir(BASE_DIR / "edges" / "card_device")
    src = edges_card_dev_df["card_id"].map(card_id2idx).dropna().astype(int)
    dst = edges_card_dev_df["device_id"].map(dev_id2idx).dropna().astype(int)
    edge_index = torch.tensor([src.values, dst.values], dtype=torch.long)
    data["card", "uses_device", "device"].edge_index = edge_index

    edges_card_email_df = read_parquet_dir(BASE_DIR / "edges" / "card_email")
    src = edges_card_email_df["card_id"].map(card_id2idx).dropna().astype(int)
    dst = edges_card_email_df["email"].map(email_id2idx).dropna().astype(int)
    edge_index = torch.tensor([src.values, dst.values], dtype=torch.long)
    data["card", "uses_email", "email"].edge_index = edge_index

    edges_cust_txn_df = read_parquet_dir(BASE_DIR / "edges" / "customer_txn")
    src = edges_cust_txn_df["customer_id"].map(cust_id2idx).dropna().astype(int)
    dst = edges_cust_txn_df["transaction_id"].map(txn_id2idx).dropna().astype(int)
    edge_index = torch.tensor([src.values, dst.values], dtype=torch.long)
    data["customer", "made_txn", "transaction"].edge_index = edge_index

    edges_txn_dev_df = read_parquet_dir(BASE_DIR / "edges" / "txn_device")
    src = edges_txn_dev_df["transaction_id"].map(txn_id2idx).dropna().astype(int)
    dst = edges_txn_dev_df["device_id"].map(dev_id2idx).dropna().astype(int)
    edge_index = torch.tensor([src.values, dst.values], dtype=torch.long)
    data["transaction", "on_device", "device"].edge_index = edge_index

    valid_labels = txn_labels_df[txn_labels_df["transaction_id"].isin(txn_id2idx.keys())].copy()

    txn_idx = valid_labels["transaction_id"].map(txn_id2idx).astype(int)
    labels = valid_labels["label"].astype(int).values

    num_txn = len(txn_ids)
    y = torch.zeros(num_txn, dtype=torch.float32)
    train_mask = torch.zeros(num_txn, dtype=torch.bool)

    y[txn_idx.values] = torch.from_numpy(labels).float()
    train_mask[txn_idx.values] = True

    data["transaction"].y = y
    data["transaction"].train_mask = train_mask

    return data, txn_labels_df, txn_id2idx

class FraudGNN(nn.Module):
    def __init__(self, metadata, hidden_dim=64):
        super().__init__()
        self.conv = HeteroConv(
            {
                edge_type: SAGEConv((-1, -1), hidden_dim)
                for edge_type in metadata[1]
            },
            aggr="mean",
        )
        self.lin_out = nn.Linear(hidden_dim, 1)

    def forward(self, data: HeteroData):
        x_dict = data.x_dict
        edge_index_dict = data.edge_index_dict

        x_dict = self.conv(x_dict, edge_index_dict)
        x_dict = {k: x.relu() for k, x in x_dict.items()}

        tx_x = x_dict["transaction"]
        logits = self.lin_out(tx_x).squeeze(-1)
        return logits, tx_x
    
def train_gnn():
    device = torch.device("cpu")

    print("Building hetero graph...")
    data, txn_labels_df, txn_id2idx = build_hetero_data()
    data = data.to(device)

    metadata = data.metadata()
    model = FraudGNN(metadata, hidden_dim=64).to(device)

    optimizer = torch.optim.Adam(model.parameters(), lr=1e-3, weight_decay=1e-5)
    loss_fn = nn.BCEWithLogitsLoss()

    y = data["transaction"].y
    mask = data["transaction"].train_mask

    labeled_idx = torch.where(mask)[0].cpu().numpy()
    train_idx, val_idx = train_test_split(
        labeled_idx, test_size=0.2, stratify=y[labeled_idx].cpu().numpy(), random_state=42
    )
    train_idx = torch.from_numpy(train_idx).to(device)
    val_idx   = torch.from_numpy(val_idx).to(device)

    def evaluate():
        model.eval()
        with torch.no_grad():
            logits, _ = model(data)
            probs = torch.sigmoid(logits)
            val_probs = probs[val_idx]
            val_true = y[val_idx]
            val_loss = loss_fn(logits[val_idx], val_true)
        return val_loss.item()

    print("Start training...")
    for epoch in range(1, 51):
        model.train()
        optimizer.zero_grad()
        logits, _ = model(data)

        loss = loss_fn(logits[train_idx], y[train_idx])
        loss.backward()
        optimizer.step()

        if epoch % 5 == 0:
            val_loss = evaluate()
            print(f"Epoch {epoch:03d} | train_loss={loss.item():.4f} | val_loss={val_loss:.4f}")

    print("Training finished.")

    model.eval()
    with torch.no_grad():
        logits, tx_emb = model(data)
        probs = torch.sigmoid(logits).cpu().numpy()
        tx_emb = tx_emb.cpu().numpy()

    idx2txn_id = {idx: tid for tid, idx in txn_id2idx.items()}
    txn_ids = [idx2txn_id[i] for i in range(len(idx2txn_id))]

    out_df = pd.DataFrame({
        "transaction_id": txn_ids,
        "gnn_score": probs,
    })

    emb_dim = tx_emb.shape[1]
    k = min(16, emb_dim)
    for i in range(k):
        out_df[f"gnn_emb_{i+1}"] = tx_emb[:, i]

    out_path = OUTPUT_DIR / "txn_gnn_features.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out_path, index=False)
    print(f"GNN features saved to {out_path}")

    model_path = OUTPUT_DIR / "fraud_gnn.pt"
    torch.save(
        {
            "state_dict": model.state_dict(),
            "metadata": metadata,
        },
        model_path,
    )
    print(f"Model saved to {model_path}")

if __name__ == "__main__":
    train_gnn()