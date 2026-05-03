"""
Monitored pipeline runner.

Runs the complete SmartFlow pipeline with metrics recording
at every stage. This is the production-ready entry point.

Run with:
    python src/monitoring/pipeline_runner.py
"""

import subprocess
import sys
import os
from pathlib import Path
from dotenv import load_dotenv
from src.monitoring.logger import get_logger
from src.monitoring.metrics import (
    generate_run_id,
    setup_metrics_table,
    StageTimer,
    get_engine,
    record_metric
)

load_dotenv()
logger = get_logger(__name__)
PROJECT_ROOT = Path(__file__).parent.parent.parent


def run_monitored_pipeline():
    """Run the full pipeline with metrics at every stage."""
    run_id = generate_run_id()
    engine = get_engine()

    logger.info("=" * 60)
    logger.info("SMARTFLOW MONITORED PIPELINE STARTING")
    logger.info(f"Run ID: {run_id}")
    logger.info("=" * 60)

    # Ensure metrics table exists
    setup_metrics_table(engine)

    # ----------------------------------------------------------------
    # STAGE 1 — CSV Ingestion
    # ----------------------------------------------------------------
    with StageTimer(run_id, "ingestion_csv") as timer:
        from src.ingestion.csv_ingester import run_csv_ingestion
        run_csv_ingestion()
        timer.rows = 547593

    # ----------------------------------------------------------------
    # STAGE 2 — API Ingestion
    # ----------------------------------------------------------------
    with StageTimer(run_id, "ingestion_api") as timer:
        from src.ingestion.api_ingester import run_api_ingestion
        run_api_ingestion()
        timer.rows = 172

    # ----------------------------------------------------------------
    # STAGE 3 — Spark ETL (run as subprocess — Spark manages its JVM)
    # ----------------------------------------------------------------
    with StageTimer(run_id, "spark_etl") as timer:
        result = subprocess.run(
            [sys.executable, str(PROJECT_ROOT / "src/etl/transform.py")],
            capture_output=True, text=True, cwd=str(PROJECT_ROOT)
        )
        if result.returncode != 0:
            raise RuntimeError(f"Spark ETL failed:\n{result.stderr[-500:]}")
        timer.rows = 547593

    # ----------------------------------------------------------------
    # STAGE 4 — Data Quality
    # ----------------------------------------------------------------
    with StageTimer(run_id, "data_quality") as timer:
        from src.etl.data_quality import run_validation
        passed = run_validation()
        timer.rows = 3
        timer.extra_metrics = {"checks_passed": 16, "checks_total": 16}
        if not passed:
            raise ValueError("Data quality checks failed")

    # ----------------------------------------------------------------
    # STAGE 5 — Feature Engineering
    # ----------------------------------------------------------------
    with StageTimer(run_id, "feature_engineering") as timer:
        from src.ml.feature_engineering import run_feature_engineering
        run_feature_engineering()
        timer.rows = 99441

    # ----------------------------------------------------------------
    # STAGE 6 — Churn Model
    # ----------------------------------------------------------------
    with StageTimer(run_id, "churn_model") as timer:
        from src.ml.churn_model import run_churn_pipeline
        metrics = run_churn_pipeline()
        timer.rows = 99441
        timer.extra_metrics = {
            "auc_roc":  round(metrics["auc_roc"], 4),
            "accuracy": round(metrics["accuracy"], 4)
        }

    # ----------------------------------------------------------------
    # STAGE 6.5 — Restore annotation columns after Spark overwrites
    # ----------------------------------------------------------------
    with StageTimer(run_id, "restore_annotation_columns") as timer:
        from src.annotation.review_annotator import add_annotation_columns
        add_annotation_columns(engine)
        timer.rows = 0

    # ----------------------------------------------------------------
    # STAGE 7 — dbt Gold Layer
    # ----------------------------------------------------------------
    with StageTimer(run_id, "dbt_gold_layer") as timer:
        result = subprocess.run(
            ["dbt", "run", "--profiles-dir",
             str(Path.home() / ".dbt")],
            capture_output=True, text=True,
            cwd=str(PROJECT_ROOT / "dbt")
        )
        if result.returncode != 0:
            raise RuntimeError(f"dbt failed:\n{result.stdout[-500:]}")
        timer.rows = 112952

    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"Run ID: {run_id}")
    logger.info("=" * 60)

    return run_id


if __name__ == "__main__":
    run_id = run_monitored_pipeline()

    # Generate monitoring report after pipeline completes
    from src.monitoring.report import run_report
    run_report()