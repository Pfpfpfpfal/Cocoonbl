set -euo pipefail

echo "[airflow-init] running migrations"
airflow db migrate

echo "[airflow-init] creating default connections"
airflow connections create-default-connections || true

echo "[airflow-init] creating admin user"
airflow users create \
  --username admin \
  --password admin \
  --firstname Air \
  --lastname Flow \
  --role Admin \
  --email admin@example.com || true

if [ -f /opt/airflow/airflow_connections.json ]; then
  echo "[airflow-init] importing connections from /opt/airflow/airflow_connections.json ..."
  airflow connections import /opt/airflow/airflow_connections.json || true
else
  echo "[airflow-init] no /opt/airflow/airflow_connections.json found, skipping"
fi

if [ -f /opt/airflow/airflow_variables.json ]; then
  echo "[airflow-init] importing variables from /opt/airflow/airflow_variables.json ..."
  airflow variables import /opt/airflow/airflow_variables.json || true
else
  echo "[airflow-init] no /opt/airflow/airflow_variables.json found, skipping"
fi

echo "[airflow-init] done."