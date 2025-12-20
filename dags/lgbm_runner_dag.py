from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    dag_id="lgbm_infer_http_trigger",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lgbm", "http"],
) as dag:

    run_lgbm = SimpleHttpOperator(
        task_id="run_lgbm_infer",
        http_conn_id="lgbm_service",
        endpoint="/run-lgbm-infer",
        method="POST",
        log_response=True,
        headers={"Content-Type": "application/json"},
    )

    run_lgbm