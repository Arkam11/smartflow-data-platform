"""
Pipeline metrics tracker for SmartFlow.

Records key metrics after each pipeline stage to PostgreSQL.
This builds a history of every run so you can track trends,
spot regressions, and prove the pipeline is healthy.

Metrics stored per run per stage:
- run_id          : unique identifier for the full pipeline run
- stage           : which part of the pipeline (ingestion, etl, ml, etc.)
- status          : success or failed
- rows_processed  : how many rows were handled
- duration_seconds: how long it took
- extra_metrics   : JSON blob for stage-specific metrics
- created_at      : when it ran
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from src.monitoring.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


def get_engine():
    db_url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_url)


def setup_metrics_table(engine):
    """
    Create the pipeline_runs metrics table if it doesn't exist.
    Called once at pipeline startup.
    """
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE SCHEMA IF NOT EXISTS monitoring
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
                id              SERIAL PRIMARY KEY,
                run_id          VARCHAR(36) NOT NULL,
                stage           VARCHAR(50) NOT NULL,
                status          VARCHAR(10) NOT NULL,
                rows_processed  INTEGER,
                duration_seconds FLOAT,
                extra_metrics   JSONB,
                error_message   TEXT,
                created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            )
        """))
        conn.commit()
    logger.info("Metrics table ready: monitoring.pipeline_runs")


def record_metric(
    run_id: str,
    stage: str,
    status: str,
    rows_processed: int = 0,
    duration_seconds: float = 0.0,
    extra_metrics: Optional[dict] = None,
    error_message: Optional[str] = None
):
    """
    Record a single pipeline stage metric to the database.

    Args:
        run_id          : UUID for this full pipeline run
        stage           : name of the stage e.g. "ingestion_csv"
        status          : "success" or "failed"
        rows_processed  : number of rows processed in this stage
        duration_seconds: how long the stage took
        extra_metrics   : dict of additional metrics (model accuracy etc.)
        error_message   : error text if status is "failed"
    """
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO monitoring.pipeline_runs
                    (run_id, stage, status, rows_processed,
                     duration_seconds, extra_metrics, error_message)
                VALUES
                    (:run_id, :stage, :status, :rows_processed,
                     :duration_seconds, CAST(:extra_metrics AS jsonb), :error_message)
            """), {
                "run_id":           run_id,
                "stage":            stage,
                "status":           status,
                "rows_processed":   rows_processed,
                "duration_seconds": duration_seconds,
                "extra_metrics":    json.dumps(extra_metrics or {}),
                "error_message":    error_message
            })
            conn.commit()
        logger.debug(f"Metric recorded: {stage} — {status} — {rows_processed:,} rows")
    except Exception as e:
        # Never let metric recording break the main pipeline
        logger.warning(f"Failed to record metric for {stage}: {e}")


class StageTimer:
    """
    Context manager that times a pipeline stage and records the metric.

    Usage:
        with StageTimer(run_id, "ingestion_csv") as timer:
            rows = run_csv_ingestion()
            timer.rows = rows

    If the stage raises an exception, it records status="failed".
    If it completes successfully, it records status="success".
    """

    def __init__(self, run_id: str, stage: str,
                 extra_metrics: Optional[dict] = None):
        self.run_id = run_id
        self.stage = stage
        self.extra_metrics = extra_metrics or {}
        self.rows = 0
        self._start = None

    def __enter__(self):
        self._start = time.time()
        logger.info(f"Stage started: {self.stage}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.time() - self._start

        if exc_type is None:
            record_metric(
                run_id=self.run_id,
                stage=self.stage,
                status="success",
                rows_processed=self.rows,
                duration_seconds=round(duration, 2),
                extra_metrics=self.extra_metrics
            )
            logger.info(
                f"Stage complete: {self.stage} — "
                f"{self.rows:,} rows — {duration:.1f}s"
            )
        else:
            record_metric(
                run_id=self.run_id,
                stage=self.stage,
                status="failed",
                rows_processed=self.rows,
                duration_seconds=round(duration, 2),
                error_message=str(exc_val)
            )
            logger.error(
                f"Stage failed: {self.stage} — {exc_val} — {duration:.1f}s"
            )
        # Return False so exceptions still propagate
        return False


def generate_run_id() -> str:
    """Generate a unique ID for a pipeline run."""
    return str(uuid.uuid4())