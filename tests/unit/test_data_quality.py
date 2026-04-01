import pytest
import pandas as pd
from src.etl.data_quality import (
    check_no_nulls,
    check_values_in_set,
    check_values_between,
    check_row_count,
    check_unique,
    run_checks,
    get_orders_checks,
    get_order_items_checks,
    get_reviews_checks,
)


def test_check_no_nulls_passes_clean_data():
    df = pd.DataFrame({"order_id": ["a", "b", "c"]})
    passed, msg = check_no_nulls(df, "order_id")
    assert passed == True


def test_check_no_nulls_fails_with_nulls():
    df = pd.DataFrame({"order_id": ["a", None, "c"]})
    passed, msg = check_no_nulls(df, "order_id")
    assert passed == False
    assert "1" in msg


def test_check_values_in_set_passes():
    df = pd.DataFrame({"status": ["delivered", "shipped"]})
    passed, msg = check_values_in_set(df, "status", {"delivered", "shipped", "canceled"})
    assert passed is True


def test_check_values_in_set_fails():
    df = pd.DataFrame({"status": ["delivered", "UNKNOWN"]})
    passed, msg = check_values_in_set(df, "status", {"delivered", "shipped"})
    assert passed is False
    assert "UNKNOWN" in msg


def test_check_values_between_passes():
    df = pd.DataFrame({"rating": [1, 3, 5]})
    passed, msg = check_values_between(df, "rating", 1, 5)
    assert passed is True


def test_check_values_between_fails():
    df = pd.DataFrame({"rating": [1, 6, 3]})
    passed, msg = check_values_between(df, "rating", 1, 5)
    assert passed is False


def test_check_row_count_passes():
    df = pd.DataFrame({"id": range(100)})
    passed, msg = check_row_count(df, 50, 200)
    assert passed is True


def test_check_row_count_fails_too_few():
    df = pd.DataFrame({"id": range(10)})
    passed, msg = check_row_count(df, 50, 200)
    assert passed is False


def test_check_unique_passes():
    df = pd.DataFrame({"order_id": ["a", "b", "c"]})
    passed, msg = check_unique(df, "order_id")
    assert passed is True


def test_check_unique_fails_with_duplicates():
    df = pd.DataFrame({"order_id": ["a", "b", "a"]})
    passed, msg = check_unique(df, "order_id")
    assert passed is False


def test_run_checks_returns_correct_counts():
    df = pd.DataFrame({
        "order_id":    ["a", "b", "c"],
        "status_code": ["delivered", "INVALID", "shipped"]
    })
    checks = [
        {
            "description": "order_id no nulls",
            "fn": check_no_nulls,
            "args": ["order_id"]
        },
        {
            "description": "valid status codes",
            "fn": check_values_in_set,
            "args": ["status_code", {"delivered", "shipped"}]
        },
    ]
    result = run_checks(df, "orders", checks)
    assert result["passed"] == 1
    assert result["failed"] == 1
    assert result["total"] == 2
    assert result["success"] is False


def test_expectation_lists_are_not_empty():
    assert len(get_orders_checks()) >= 7
    assert len(get_order_items_checks()) >= 5
    assert len(get_reviews_checks()) >= 4