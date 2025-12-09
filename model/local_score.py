import pandas as pd
import joblib
import os

test_path = "./notebooks/ml_dataset/dataset_test"
df_test = pd.read_parquet(test_path)

bundle = joblib.load("./model/ml/lgbm_fraud.pkl")
model = bundle["model"]
feature_cols = bundle["features"]

X = df_test[feature_cols]
fraud_proba = model.predict_proba(X)[:, 1]

df_test["fraud_score"] = fraud_proba
df_test["fraud_label"] = (df_test["fraud_score"] > 0.5).astype("int32")

scored_path = "./model/ml/test_scored/"
os.makedirs(scored_path, exist_ok=True)

scored_file_path = os.path.join(scored_path, "test_scored.parquet")
df_test.to_parquet(scored_file_path, index=False)
print(f"Scored test saved to {scored_path}")