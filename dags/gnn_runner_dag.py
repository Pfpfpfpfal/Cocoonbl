from datetime import datetime
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

with DAG(
    dag_id="gnn_infer_http_trigger",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gnn", "http"],
) as dag:

    run_gnn = SimpleHttpOperator(
        task_id="run_gnn_infer",
        http_conn_id="gnn_service",
        endpoint="/run-gnn-infer",
        method="POST",
        log_response=True,
        headers={"Content-Type": "application/json"},
    )

    run_gnn