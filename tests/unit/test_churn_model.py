import pytest
import pandas as pd
import numpy as np
from unittest.mock import patch, MagicMock
from src.ml.churn_model import (
    prepare_training_data,
    score_all_customers,
    FEATURE_COLUMNS,
)
from src.ml.feature_engineering import define_churn_label


def make_sample_features(n=200):
    """Create a minimal feature DataFrame for testing."""
    np.random.seed(42)
    df = pd.DataFrame({
        "customer_key":          [f"cust_{i}" for i in range(n)],
        "churned":               np.random.randint(0, 2, n),
        "total_orders":          np.random.randint(1, 10, n),
        "avg_days_to_deliver":   np.random.uniform(3, 30, n),
        "late_delivery_rate":    np.random.uniform(0, 1, n),
        "total_spend":           np.random.uniform(50, 2000, n),
        "avg_order_value":       np.random.uniform(50, 500, n),
        "avg_rating":            np.random.uniform(1, 5, n),
        "review_count":          np.random.randint(0, 5, n),
        "days_since_last_order": np.random.randint(1, 400, n),
        "customer_lifetime_days": np.random.randint(0, 700, n),
        "payment_type_encoded":  np.random.randint(0, 4, n),
        "state_encoded":         np.random.randint(0, 27, n),
    })
    return df


def test_feature_columns_all_present():
    df = make_sample_features()
    for col in FEATURE_COLUMNS:
        assert col in df.columns, f"Feature column '{col}' missing"


def test_prepare_training_data_returns_correct_shapes():
    df = make_sample_features(200)
    X_train, X_test, y_train, y_test = prepare_training_data(df)

    assert len(X_train) + len(X_test) == 200
    assert len(X_train) == len(y_train)
    assert len(X_test) == len(y_test)
    assert len(X_test) == pytest.approx(40, abs=5)  # ~20% of 200


def test_prepare_training_data_only_uses_feature_columns():
    df = make_sample_features(200)
    X_train, X_test, y_train, y_test = prepare_training_data(df)
    assert list(X_train.columns) == FEATURE_COLUMNS


def test_score_all_customers_returns_scores_for_all():
    from sklearn.ensemble import RandomForestClassifier
    df = make_sample_features(200)

    model = RandomForestClassifier(n_estimators=5, random_state=42)
    model.fit(df[FEATURE_COLUMNS], df["churned"])

    scores = score_all_customers(model, df)

    assert len(scores) == 200
    assert "churn_score" in scores.columns
    assert "churn_risk" in scores.columns
    assert scores["churn_score"].between(0, 1).all()


def test_churn_scores_in_valid_range():
    from sklearn.ensemble import RandomForestClassifier
    df = make_sample_features(200)

    model = RandomForestClassifier(n_estimators=5, random_state=42)
    model.fit(df[FEATURE_COLUMNS], df["churned"])

    scores = score_all_customers(model, df)
    assert scores["churn_score"].min() >= 0.0
    assert scores["churn_score"].max() <= 1.0


def test_define_churn_label_produces_binary_labels():
    orders = pd.DataFrame({
        "order_id":    [f"ord_{i}" for i in range(100)],
        "customer_key": [f"cust_{i % 20}" for i in range(100)],
        "order_date":  pd.date_range("2017-01-01", periods=100, freq="3D", tz="UTC"),
        "days_to_deliver": [7] * 100,
        "delivered_on_time": [True] * 100,
    })
    result = define_churn_label(orders)
    assert set(result["churned"].unique()).issubset({0, 1})
    assert "days_since_last_order" in result.columns
    assert "customer_lifetime_days" in result.columns