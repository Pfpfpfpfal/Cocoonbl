import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score
from lightgbm import LGBMClassifier
import joblib
import pyarrow
import os

path = "../notebooks/hive_cat.features.ml_dataset"

df = pd.read_parquet(path, engine="pyarrow")

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

X = df[feature_cols]
y = df[target_col].astype(int)

X_train, X_valid, y_train, y_valid = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

model = LGBMClassifier(
    n_estimators=500,
    learning_rate=0.05,
    max_depth=-1,
    subsample=0.8,
    colsample_bytree=0.8,
    class_weight="balanced",
    n_jobs=-1,
)
model.fit(X_train, y_train)

y_pred_prob = model.predict_proba(X_valid)[:, 1]
auc = roc_auc_score(y_valid, y_pred_prob)
print(f"auc : {auc:.4f}")

os.makedirs("./ml", exist_ok=True)

bundle = {"model":model, "features":feature_cols}
joblib.dump(bundle, "./ml/lgbm_fraud.pkl")
print("model saved to lgbm_fraud.pkl")