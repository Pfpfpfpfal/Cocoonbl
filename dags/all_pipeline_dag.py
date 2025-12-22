from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.http.operators.http import SimpleHttpOperator

AWS_REGION = "us-east-1"

SPARK_CONF = {
    "spark.executor.memory": "2g",
    "spark.driver.memory": "1g",
    "spark.executor.cores": "2",
    "spark.cores.max": "2",

    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.hive_cat": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.hive_cat.type": "hive",
    "spark.sql.catalog.hive_cat.uri": "thrift://hive-metastore:9083",
    "spark.sql.catalog.hive_cat.warehouse": "s3a://warehouse/",
    "spark.sql.catalog.hive_cat.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",

    "spark.sql.catalog.hive_cat.s3.endpoint": "http://minio:9000",
    "spark.sql.catalog.hive_cat.s3.path-style-access": "true",
    "spark.sql.catalog.hive_cat.s3.access-key-id": "admin",
    "spark.sql.catalog.hive_cat.s3.secret-access-key": "password",
    "spark.sql.catalog.hive_cat.s3.region": AWS_REGION,

    "spark.driverEnv.AWS_REGION": AWS_REGION,
    "spark.executorEnv.AWS_REGION": AWS_REGION,
    "spark.hadoop.aws.region": AWS_REGION,
    "spark.driver.extraJavaOptions": f"-Daws.region={AWS_REGION}",
    "spark.executor.extraJavaOptions": f"-Daws.region={AWS_REGION}",

    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "password",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",

    "spark.sql.warehouse.dir": "s3a://warehouse/",
    "spark.hadoop.hive.metastore.warehouse.dir": "s3a://warehouse/",

    "spark.sql.shuffle.partitions": "200",
}

SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,"
    "org.apache.iceberg:iceberg-aws-bundle:1.10.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.367"
)

with DAG(
    dag_id="full_fraud_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["pipeline", "spark", "gnn", "lgbm", "iceberg"],
) as dag:

    csv_to_raw = SparkSubmitOperator(
        task_id="csv_to_raw",
        conn_id="spark",
        application="/opt/airflow/dags/jobs/csv_to_raw.py",
        verbose=True,
        conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
    )

    raw_to_cleaned = SparkSubmitOperator(
        task_id="raw_to_cleaned",
        conn_id="spark",
        application="/opt/airflow/dags/jobs/raw_to_cleaned.py",
        verbose=True,
        conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
    )

    cleaned_to_features = SparkSubmitOperator(
        task_id="cleaned_to_features",
        conn_id="spark",
        application="/opt/airflow/dags/jobs/cleaned_to_features.py",
        verbose=True,
        conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
    )

    export_gnn_graph = SparkSubmitOperator(
        task_id="export_gnn_graph",
        conn_id="spark",
        application="/opt/airflow/dags/jobs/export_gnn_graph.py",
        verbose=True,
        conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
    )

    run_gnn = SimpleHttpOperator(
        task_id="run_gnn_infer",
        http_conn_id="gnn_service",
        endpoint="/run-gnn-infer",
        method="POST",
        log_response=True,
        headers={"Content-Type": "application/json"},
    )

    import_join = SparkSubmitOperator(
        task_id="import_join",
        conn_id="spark",
        application="/opt/airflow/dags/jobs/import_join.py",
        verbose=True,
        conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
    )

    export_test_dataset = SparkSubmitOperator(
        task_id="export_test_dataset",
        conn_id="spark",
        application="/opt/airflow/dags/jobs/export_test_dataset.py",
        verbose=True,
        conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
    )

    run_lgbm = SimpleHttpOperator(
        task_id="run_lgbm_infer",
        http_conn_id="lgbm_service",
        endpoint="/run-lgbm-infer",
        method="POST",
        log_response=True,
        headers={"Content-Type": "application/json"},
    )

    lgbm_scored_to_marts = SparkSubmitOperator(
        task_id="lgbm_scored_to_marts",
        conn_id="spark",
        application="/opt/airflow/dags/jobs/import_scored_to_marts.py",
        verbose=True,
        conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
    )

    scored_to_clustered = SparkSubmitOperator(
        task_id="scored_to_clustered",
        conn_id="spark",
        application="/opt/airflow/dags/jobs/scored_to_clustered.py",
        verbose=True,
        conf=SPARK_CONF,
        packages=SPARK_PACKAGES,
    )

    (
        csv_to_raw
        >> raw_to_cleaned
        >> cleaned_to_features
        >> export_gnn_graph
        >> run_gnn
        >> import_join
        >> export_test_dataset
        >> run_lgbm
        >> lgbm_scored_to_marts
        >> scored_to_clustered
    )
