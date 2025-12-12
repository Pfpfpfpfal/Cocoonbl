import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from lightgbm import LGBMClassifier
import joblib
import numpy as np

train_path = "../notebooks/ml_dataset/dataset_train"

print(f"Loading train data from: {train_path}")
df = pd.read_parquet(train_path, engine="pyarrow")
print("Shape:", df.shape)
print("Columns:", df.columns.tolist())
print(df.dtypes)

target_col = "is_fraud"

exclude_cols = {
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
    target_col,
}

feature_cols = [c for c in df.columns if c not in exclude_cols]
print("Num features:", len(feature_cols))
print("Feature cols:", feature_cols)

X = df[feature_cols]
y = df[target_col].astype(int)

X_train, X_valid, y_train, y_valid = train_test_split(
    X,
    y,
    test_size=0.2,
    stratify=y,
    random_state=42,
)

model = LGBMClassifier(
    n_estimators=500,
    learning_rate=0.05,
    max_depth=-1,
    subsample=0.8,
    colsample_bytree=0.8,
    class_weight="balanced",
    n_jobs=-1,
)

print("Training LightGBM...")
model.fit(X_train, y_train)

y_pred_prob = model.predict_proba(X_valid)[:, 1]
auc = roc_auc_score(y_valid, y_pred_prob)
print(f"AUC on validation: {auc:.4f}")

def eval_topk_and_lift(y_true, y_score, top_k_list=(0.001, 0.005, 0.01, 0.02, 0.05), n_bins=10):
    y_true = np.asarray(y_true).astype(int)
    y_score = np.asarray(y_score).astype(float)

    df = pd.DataFrame({"y": y_true, "score": y_score}).sort_values("score", ascending=False).reset_index(drop=True)

    base_rate = df["y"].mean()
    total_pos = df["y"].sum()

    rows = []
    for frac in top_k_list:
        k = max(1, int(round(frac * len(df))))
        top = df.iloc[:k]
        tp = int(top["y"].sum())
        recall = tp / total_pos if total_pos > 0 else 0.0
        precision = tp / k
        rows.append({
            "top_frac": frac,
            "k": k,
            "tp": tp,
            "precision": precision,
            "recall": recall,
            "lift": (precision / base_rate) if base_rate > 0 else np.nan,
        })

    topk_table = pd.DataFrame(rows)

    df["bin"] = pd.qcut(df.index + 1, q=n_bins, labels=False)
    bin_stats = (
        df.groupby("bin")
          .agg(cnt=("y", "size"), pos=("y", "sum"), avg_score=("score", "mean"))
          .reset_index()
    )
    bin_stats["pos_rate"] = bin_stats["pos"] / bin_stats["cnt"]
    bin_stats["lift"] = bin_stats["pos_rate"] / base_rate if base_rate > 0 else np.nan

    bin_stats["neg"] = bin_stats["cnt"] - bin_stats["pos"]
    total_neg = bin_stats["neg"].sum()

    bin_stats = bin_stats.sort_values("bin").reset_index(drop=True)
    bin_stats["cum_pos"] = bin_stats["pos"].cumsum()
    bin_stats["cum_neg"] = bin_stats["neg"].cumsum()
    bin_stats["cdf_pos"] = bin_stats["cum_pos"] / total_pos if total_pos > 0 else 0.0
    bin_stats["cdf_neg"] = bin_stats["cum_neg"] / total_neg if total_neg > 0 else 0.0
    bin_stats["ks"] = (bin_stats["cdf_pos"] - bin_stats["cdf_neg"]).abs()

    ks_value = float(bin_stats["ks"].max()) if len(bin_stats) else 0.0

    return topk_table, bin_stats, ks_value, base_rate

topk_table, lift_table, ks_value, base_rate = eval_topk_and_lift(y_valid, y_pred_prob)

print(f"Base fraud rate in valid: {base_rate:.6f}")
print(f"KS: {ks_value:.4f}")

print("\nTop-K metrics:")
print(topk_table.to_string(index=False, float_format=lambda x: f"{x:.4f}"))

print("\nLift/KS by decile (bin=0 is top scores):")
print(lift_table[["bin","cnt","pos","pos_rate","lift","avg_score","ks"]].to_string(index=False, float_format=lambda x: f"{x:.4f}"))

os.makedirs("./ml", exist_ok=True)

bundle = {"model": model, "features": feature_cols}
joblib.dump(bundle, "./ml/lgbm_fraud_gnn.pkl")
print("Model saved to ./ml/lgbm_fraud_gnn.pkl")