from datetime import datetime, timedelta
import os
from typing import Dict

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator


# ============================================================
# Airflow DAG: E-commerce Lakehouse Pipeline
# ============================================================
# Flow:
# raw file check
#   -> local/ADLS upload
#   -> Bronze Databricks job
#   -> Silver Databricks jobs
#   -> Gold Databricks job
#   -> Audit/Validation/SLA jobs
# ============================================================


# ============================================================
# Config
# ============================================================

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")

RAW_SOURCE_PATH = os.getenv(
    "RAW_SOURCE_PATH",
    f"{PROJECT_ROOT}/data/raw"
)

# Databricks Job IDs should be configured in Airflow environment variables.
# Example:
# DBX_JOB_BRONZE_LOADER=123456
# DBX_JOB_SILVER_FLAT=123457
# DBX_JOB_SILVER_CDC=123458

DATABRICKS_JOB_IDS: Dict[str, str] = {
    "bronze_loader": os.getenv("DBX_JOB_BRONZE_LOADER"),
    "silver_flat_tables": os.getenv("DBX_JOB_SILVER_FLAT"),
    "silver_cdc_tables": os.getenv("DBX_JOB_SILVER_CDC"),
    "silver_log_tables": os.getenv("DBX_JOB_SILVER_LOGS"),
    "gold_layer": os.getenv("DBX_JOB_GOLD_LAYER"),
    "pipeline_audit": os.getenv("DBX_JOB_PIPELINE_AUDIT"),
    "record_validation": os.getenv("DBX_JOB_RECORD_VALIDATION"),
    "sla_monitoring": os.getenv("DBX_JOB_SLA_MONITORING"),
}


# ============================================================
# Failure callback
# ============================================================

def failure_callback(context):
    """
    Runs when any Airflow task fails.
    In production, this can be extended to send Slack/Teams/Email alerts.
    """

    task_instance = context.get("task_instance")
    dag = context.get("dag")
    exception = context.get("exception")
    execution_date = context.get("execution_date")

    print("====================================================")
    print("AIRFLOW TASK FAILURE ALERT")
    print(f"DAG ID         : {dag.dag_id if dag else None}")
    print(f"Task ID        : {task_instance.task_id if task_instance else None}")
    print(f"Execution Date : {execution_date}")
    print(f"Error          : {exception}")
    print("====================================================")


# ============================================================
# Python task functions
# ============================================================

def check_raw_files_available():
    """
    Sensor-style check using PythonOperator.

    This checks whether local raw files exist before upload.
    In production, this can be replaced by an ADLS/Azure Blob sensor.
    """

    if not os.path.exists(RAW_SOURCE_PATH):
        raise FileNotFoundError(f"Raw source path does not exist: {RAW_SOURCE_PATH}")

    file_count = 0

    for root, _, files in os.walk(RAW_SOURCE_PATH):
        for file in files:
            if not file.startswith("."):
                file_count += 1

    if file_count == 0:
        raise FileNotFoundError(f"No raw files found under: {RAW_SOURCE_PATH}")

    print(f"Raw file check passed. Files found: {file_count}")


def upload_raw_to_adls():
    """
    Upload local raw files/config files to ADLS.

    In our project, upload logic is already implemented in ingestion scripts.
    This function can call those scripts directly after refactoring them into functions.
    """

    print("Starting raw upload to ADLS...")

    # Option 1: if your script has a function, import and call it:
    # from ingestion.upload_raw_to_adls import upload_all_raw_files
    # upload_all_raw_files()

    # Option 2: if config upload is separate:
    # from ingestion.upload_config_to_adls import upload_config
    # upload_config()

    # For now this is a placeholder to show orchestration.
    print("Raw upload to ADLS completed.")


def validate_databricks_job_ids():
    """
    Validate that required Databricks job IDs are configured.
    """

    missing_jobs = [
        job_key
        for job_key, job_id in DATABRICKS_JOB_IDS.items()
        if job_id is None or str(job_id).strip() == ""
    ]

    if missing_jobs:
        raise ValueError(
            f"Missing Databricks job IDs for: {missing_jobs}. "
            "Set them as Airflow environment variables."
        )

    print("All Databricks job IDs are configured.")


# ============================================================
# Default DAG args
# ============================================================

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": failure_callback,
}


# ============================================================
# DAG definition
# ============================================================

with DAG(
    dag_id="ecommerce_lakehouse_pipeline",
    default_args=default_args,
    description="End-to-end orchestration for multi-source e-commerce lakehouse",
    start_date=datetime(2026, 5, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=[
        "ecommerce",
        "lakehouse",
        "azure",
        "adls",
        "databricks",
        "bronze",
        "silver",
        "gold",
    ],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    check_raw_files = PythonOperator(
        task_id="check_raw_files_available",
        python_callable=check_raw_files_available,
    )

    upload_raw = PythonOperator(
        task_id="upload_raw_to_adls",
        python_callable=upload_raw_to_adls,
    )

    validate_jobs = PythonOperator(
        task_id="validate_databricks_job_ids",
        python_callable=validate_databricks_job_ids,
    )

    run_bronze_loader = DatabricksRunNowOperator(
        task_id="run_bronze_loader",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_IDS["bronze_loader"],
    )

    run_silver_flat_tables = DatabricksRunNowOperator(
        task_id="run_silver_flat_tables",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_IDS["silver_flat_tables"],
    )

    run_silver_cdc_tables = DatabricksRunNowOperator(
        task_id="run_silver_cdc_tables",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_IDS["silver_cdc_tables"],
    )

    run_silver_log_tables = DatabricksRunNowOperator(
        task_id="run_silver_log_tables",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_IDS["silver_log_tables"],
    )

    run_gold_layer = DatabricksRunNowOperator(
        task_id="run_gold_layer",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_IDS["gold_layer"],
    )

    run_pipeline_audit = DatabricksRunNowOperator(
        task_id="run_pipeline_audit",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_IDS["pipeline_audit"],
    )

    run_record_validation = DatabricksRunNowOperator(
        task_id="run_record_validation",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_IDS["record_validation"],
    )

    run_sla_monitoring = DatabricksRunNowOperator(
        task_id="run_sla_monitoring",
        databricks_conn_id="databricks_default",
        job_id=DATABRICKS_JOB_IDS["sla_monitoring"],
    )

    end = EmptyOperator(
        task_id="end"
    )

    # ========================================================
    # Dependencies
    # ========================================================

    (
        start
        >> check_raw_files
        >> upload_raw
        >> validate_jobs
        >> run_bronze_loader
        >> run_silver_flat_tables
        >> run_silver_cdc_tables
        >> run_silver_log_tables
        >> run_gold_layer
        >> run_pipeline_audit
        >> run_record_validation
        >> run_sla_monitoring
        >> end
    )