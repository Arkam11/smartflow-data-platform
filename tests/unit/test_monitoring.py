import pytest
import json
from unittest.mock import patch, MagicMock
from src.monitoring.logger import get_logger
from src.monitoring.metrics import generate_run_id, StageTimer, record_metric


def test_get_logger_returns_logger():
    logger = get_logger("test.component")
    assert logger is not None
    assert logger.name == "test.component"


def test_get_logger_same_instance():
    logger1 = get_logger("test.same")
    logger2 = get_logger("test.same")
    assert logger1 is logger2


def test_generate_run_id_is_unique():
    id1 = generate_run_id()
    id2 = generate_run_id()
    assert id1 != id2
    assert len(id1) == 36  # UUID format


def test_generate_run_id_is_string():
    run_id = generate_run_id()
    assert isinstance(run_id, str)


def test_stage_timer_records_success():
    recorded = []

    def mock_record(run_id, stage, status, **kwargs):
        recorded.append({"stage": stage, "status": status})

    with patch("src.monitoring.metrics.record_metric", side_effect=mock_record):
        with StageTimer("run-123", "test_stage") as timer:
            timer.rows = 100

    assert len(recorded) == 1
    assert recorded[0]["stage"] == "test_stage"
    assert recorded[0]["status"] == "success"


def test_stage_timer_records_failure():
    recorded = []

    def mock_record(run_id, stage, status, **kwargs):
        recorded.append({"stage": stage, "status": status})

    with patch("src.monitoring.metrics.record_metric", side_effect=mock_record):
        with pytest.raises(ValueError):
            with StageTimer("run-123", "failing_stage"):
                raise ValueError("test error")

    assert recorded[0]["status"] == "failed"


def test_stage_timer_propagates_exception():
    with patch("src.monitoring.metrics.record_metric"):
        with pytest.raises(RuntimeError, match="test error"):
            with StageTimer("run-123", "stage"):
                raise RuntimeError("test error")


def test_record_metric_handles_db_error_gracefully():
    with patch("src.monitoring.metrics.get_engine") as mock_engine:
        mock_engine.side_effect = Exception("DB connection failed")
        # Should not raise — metric failures must never break the pipeline
        record_metric("run-123", "test_stage", "success", rows_processed=100)