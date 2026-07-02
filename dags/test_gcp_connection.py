"""
test_gcp_connection.py

Phase 1 verification DAG.
Confirms Airflow can:
  1. Read Airflow Variables
  2. Write a file to GCS Bronze bucket
  3. Create a BigQuery table and insert a row
  4. Read that row back from BigQuery

Once this DAG runs green, the Airflow <-> GCP handshake is confirmed.
"""

from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# --- DAG default args ---
default_args = {
    "owner": "techpulse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# --- Read project-level variables set in docker-compose ---
PROJECT_ID  = Variable.get("project_id")
GCS_BRONZE  = Variable.get("gcs_bronze")


def task_write_gcs(**context):
    """Write a small test JSON file to GCS Bronze."""
    hook = GCSHook(gcp_conn_id="google_cloud_default")
    payload = json.dumps({
        "test": True,
        "message": "Airflow can write to GCS Bronze",
        "ts": context["ts"],
    })
    hook.upload(
        bucket_name=GCS_BRONZE,
        object_name="news/dt=2026-01-01/test_write.json",
        data=payload,
        mime_type="application/json",
    )
    print(f"Written test file to gs://{GCS_BRONZE}/news/dt=2026-01-01/test_write.json")


def task_write_bigquery(**context):
    """Create a test table in BigQuery and insert one row."""
    hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
    client = hook.get_client()

    table_id = f"{PROJECT_ID}.techpulse_bronze.connection_test"

    # Create table if not exists
    schema = [
        {"name": "test_message", "type": "STRING"},
        {"name": "run_ts",       "type": "TIMESTAMP"},
    ]
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_id}` (
            test_message STRING,
            run_ts       TIMESTAMP
        )
    """
    client.query(create_sql).result()

    # Insert a row
    insert_sql = f"""
        INSERT INTO `{table_id}` (test_message, run_ts)
        VALUES ('Airflow can write to BigQuery', CURRENT_TIMESTAMP())
    """
    client.query(insert_sql).result()
    print(f"Inserted test row into {table_id}")


def task_read_bigquery(**context):
    """Read back from the test table to confirm round-trip."""
    hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
    client = hook.get_client()

    table_id = f"{PROJECT_ID}.techpulse_bronze.connection_test"
    rows = list(client.query(
        f"SELECT test_message, run_ts FROM `{table_id}` ORDER BY run_ts DESC LIMIT 1"
    ).result())

    if not rows:
        raise ValueError("No rows found in BigQuery test table — something went wrong")

    print(f"Read back from BigQuery: {rows[0]['test_message']} @ {rows[0]['run_ts']}")
    print("✅ Full Airflow → GCS → BigQuery round-trip confirmed")


# --- DAG definition ---
with DAG(
    dag_id="test_gcp_connection",
    default_args=default_args,
    description="Phase 1 verification: Airflow <-> GCS <-> BigQuery",
    schedule_interval=None,       # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["techpulse", "phase1", "test"],
) as dag:

    write_gcs = PythonOperator(
        task_id="write_test_file_to_gcs",
        python_callable=task_write_gcs,
    )

    write_bq = PythonOperator(
        task_id="write_test_row_to_bigquery",
        python_callable=task_write_bigquery,
    )

    read_bq = PythonOperator(
        task_id="read_back_from_bigquery",
        python_callable=task_read_bigquery,
    )

    write_gcs >> write_bq >> read_bq