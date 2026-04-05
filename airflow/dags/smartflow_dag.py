"""
SmartFlow Data Pipeline DAG
============================
This DAG orchestrates the complete SmartFlow data pipeline:

Bronze layer  : CSV ingestion + API ingestion (run in parallel)
Silver layer  : Spark ETL transformation → Data quality validation
ML layer      : Feature engineering → Churn model training
Annotation    : LLM review annotation (runs in parallel with ML)
Gold layer    : dbt star schema build

Schedule: daily at 6:00 AM UTC
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# Add project root to Python path so our src modules can be imported
PROJECT_ROOT = "/home/arkam/smartflow-data-platform"
sys.path.insert(0, PROJECT_ROOT)

# ================================================================
# DEFAULT ARGUMENTS
# These apply to every task in the DAG unless overridden
# ================================================================
default_args = {
    "owner":              "smartflow",
    "depends_on_past":    False,

    # How many times to retry a failed task before giving up
    "retries":            2,

    # How long to wait between retries
    "retry_delay":        timedelta(minutes=3),

    # Email on failure — set to your real email in production
    "email_on_failure":   False,
    "email_on_retry":     False,

    # If the pipeline falls behind, don't run all missed intervals
    "catchup":            False,
}

# ================================================================
# TASK FUNCTIONS
# Each function wraps one of your existing pipeline scripts.
# Airflow calls these functions when it executes each task.
# ================================================================

def task_ingest_csv(**context):
    """Ingest CSV files into Bronze layer."""
    from src.ingestion.csv_ingester import run_csv_ingestion
    logging.info("Starting CSV ingestion...")
    run_csv_ingestion()
    logging.info("CSV ingestion complete")


def task_ingest_api(**context):
    """Ingest exchange rates from API into Bronze layer."""
    from src.ingestion.api_ingester import run_api_ingestion
    logging.info("Starting API ingestion...")
    run_api_ingestion()
    logging.info("API ingestion complete")


def task_spark_etl(**context):
    """Run Spark ETL: Bronze → Silver transformation."""
    import subprocess
    logging.info("Starting Spark ETL...")

    # Run Spark as a subprocess because Spark manages its own JVM
    # Running it directly inside Airflow's process can cause memory issues
    result = subprocess.run(
        ["python", f"{PROJECT_ROOT}/src/etl/transform.py"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT
    )

    if result.returncode != 0:
        logging.error(f"Spark ETL failed:\n{result.stderr}")
        raise RuntimeError(f"Spark ETL failed with return code {result.returncode}")

    logging.info("Spark ETL complete")
    logging.info(result.stdout[-2000:] if result.stdout else "No output")


def task_data_quality(**context):
    """Run data quality validation on Silver tables."""
    from src.etl.data_quality import run_validation
    logging.info("Starting data quality validation...")

    passed = run_validation()

    if not passed:
        raise ValueError(
            "Data quality validation FAILED — "
            "pipeline stopped to prevent bad data reaching Gold layer"
        )

    logging.info("Data quality validation passed")


def task_feature_engineering(**context):
    """Build ML feature store from Silver tables."""
    from src.ml.feature_engineering import run_feature_engineering
    logging.info("Starting feature engineering...")
    run_feature_engineering()
    logging.info("Feature engineering complete")


def task_churn_model(**context):
    """Train churn prediction model and score all customers."""
    from src.ml.churn_model import run_churn_pipeline
    logging.info("Starting churn model pipeline...")
    metrics = run_churn_pipeline()
    logging.info(f"Churn model complete — AUC: {metrics['auc_roc']:.3f}")

    # Push metrics to XCom so downstream tasks or monitoring can read them
    # XCom is Airflow's mechanism for passing small data between tasks
    context["ti"].xcom_push(key="auc_roc",   value=metrics["auc_roc"])
    context["ti"].xcom_push(key="accuracy",  value=metrics["accuracy"])


def task_annotate_reviews(**context):
    """Run LLM annotation on review text."""
    from src.annotation.review_annotator import run_annotation_pipeline
    logging.info("Starting review annotation...")
    run_annotation_pipeline()
    logging.info("Review annotation complete")


def task_log_summary(**context):
    """
    Log a summary of the pipeline run.
    Reads XCom values pushed by upstream tasks.
    """
    ti = context["ti"]

    # Pull metrics pushed by the churn model task
    auc_roc  = ti.xcom_pull(task_ids="churn_model",  key="auc_roc")
    accuracy = ti.xcom_pull(task_ids="churn_model",  key="accuracy")

    logging.info("=" * 60)
    logging.info("SMARTFLOW PIPELINE RUN SUMMARY")
    logging.info("=" * 60)
    logging.info(f"  Run date:          {context['ds']}")
    logging.info(f"  Churn AUC-ROC:     {auc_roc:.3f}" if auc_roc else "  Churn AUC-ROC: N/A")
    logging.info(f"  Churn Accuracy:    {accuracy:.3f}" if accuracy else "  Churn Accuracy: N/A")
    logging.info("  All tasks completed successfully")
    logging.info("=" * 60)


# ================================================================
# DAG DEFINITION
# ================================================================
with DAG(
    dag_id="smartflow_pipeline",

    description="SmartFlow end-to-end data pipeline: "
                "Bronze → Silver → ML → Gold",

    # Run daily at 6:00 AM UTC
    # Cron format: minute hour day month day_of_week
    schedule="0 6 * * *",

    start_date=datetime(2024, 1, 1),

    # Don't run all missed dates if pipeline was offline
    catchup=False,

    default_args=default_args,

    # Tags make it easy to find in the Airflow UI
    tags=["smartflow", "etl", "ml", "data-mart"],

    # Maximum concurrent runs of this DAG
    max_active_runs=1,

) as dag:

    # ----------------------------------------------------------------
    # BRONZE LAYER — run ingestion tasks in parallel
    # ----------------------------------------------------------------
    ingest_csv = PythonOperator(
        task_id="ingest_csv",
        python_callable=task_ingest_csv,
        doc_md="Ingest raw CSV files into bronze PostgreSQL tables"
    )

    ingest_api = PythonOperator(
        task_id="ingest_api",
        python_callable=task_ingest_api,
        doc_md="Fetch live exchange rates from Open Exchange Rates API"
    )

    # ----------------------------------------------------------------
    # SILVER LAYER — ETL then validation
    # ----------------------------------------------------------------
    spark_etl = PythonOperator(
        task_id="spark_etl",
        python_callable=task_spark_etl,

        # Spark needs more time than typical tasks
        execution_timeout=timedelta(minutes=30),
        doc_md="Run PySpark Bronze → Silver transformation"
    )

    data_quality = PythonOperator(
        task_id="data_quality",
        python_callable=task_data_quality,
        doc_md="Validate Silver layer data quality — blocks pipeline if checks fail"
    )

    # ----------------------------------------------------------------
    # ML + ANNOTATION — run in parallel after quality gate
    # ----------------------------------------------------------------
    feature_engineering = PythonOperator(
        task_id="feature_engineering",
        python_callable=task_feature_engineering,
        doc_md="Engineer customer features and save to ml.feature_store"
    )

    churn_model = PythonOperator(
        task_id="churn_model",
        python_callable=task_churn_model,
        doc_md="Train RandomForest churn model and score all customers"
    )

    annotate_reviews = PythonOperator(
        task_id="annotate_reviews",
        python_callable=task_annotate_reviews,
        doc_md="Annotate review text with LLM sentiment labels"
    )

    # ----------------------------------------------------------------
    # GOLD LAYER — dbt runs after ML scoring is complete
    # ----------------------------------------------------------------
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {PROJECT_ROOT}/dbt && "
            f"source {PROJECT_ROOT}/venv/bin/activate && "
            f"dbt run --profiles-dir /home/arkam/.dbt"
        ),
        doc_md="Build Gold layer star schema using dbt"
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {PROJECT_ROOT}/dbt && "
            f"source {PROJECT_ROOT}/venv/bin/activate && "
            f"dbt test --profiles-dir /home/arkam/.dbt"
        ),
        doc_md="Run dbt schema tests on Gold layer"
    )

    # ----------------------------------------------------------------
    # SUMMARY
    # ----------------------------------------------------------------
    log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=task_log_summary,
        doc_md="Log pipeline run summary and metrics"
    )

    # ================================================================
    # TASK DEPENDENCIES — this defines the execution order
    #
    # The >> operator means "must complete before"
    # [a, b] >> c means both a AND b must complete before c starts
    #
    # Visual flow:
    # ingest_csv ──┐
    #              ├──► spark_etl ──► data_quality ──► feature_engineering ──► churn_model ──┐
    # ingest_api ──┘                              └──► annotate_reviews ────────────────────►├──► dbt_run ──► dbt_test ──► log_summary
    # ================================================================

    # Both ingestion tasks must finish before Spark runs
    [ingest_csv, ingest_api] >> spark_etl

    # Spark must finish before quality check
    spark_etl >> data_quality

    # After quality gate passes, ML and annotation run in parallel
    data_quality >> feature_engineering
    data_quality >> annotate_reviews

    # Churn model needs features first
    feature_engineering >> churn_model

    # dbt runs after both churn scoring and annotation are done
    [churn_model, annotate_reviews] >> dbt_run

    # Test after build
    dbt_run >> dbt_test

    # Summary is the final task
    dbt_test >> log_summary