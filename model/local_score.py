import os
import pandas as pd
import joblib

test_path = "../notebooks/ml_dataset/dataset_test"
print(f"Loading test data from: {test_path}")
df_test = pd.read_parquet(test_path)

bundle = joblib.load("./ml/lgbm_fraud_gnn.pkl")
model = bundle["model"]
feature_cols = bundle["features"]

print("Features expected by model:", len(feature_cols))

X = df_test[feature_cols]

fraud_proba = model.predict_proba(X)[:, 1]

df_test["fraud_score"] = fraud_proba
df_test["fraud_label"] = (df_test["fraud_score"] > 0.5).astype("int32")

if "timestamp" in df_test.columns:
    df_test = df_test.drop(columns=["timestamp"])

scored_dir = "./ml/test_scored_gnn"
os.makedirs(scored_dir, exist_ok=True)

scored_path = os.path.join(scored_dir, "test_scored.parquet")
df_test.to_parquet(scored_path, index=False)
print(f"Scored test saved to {scored_path}")