import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from src.ingestion.api_ingester import fetch_exchange_rates, transform_rates_to_dataframe


# --- Test 1: API fetch returns correct structure ---
def test_fetch_exchange_rates_returns_rates():
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "timestamp": 1711843200,
        "base": "USD",
        "rates": {"BRL": 4.97, "EUR": 0.92, "GBP": 0.79}
    }
    mock_response.raise_for_status = MagicMock()

    with patch("requests.get", return_value=mock_response):
        result = fetch_exchange_rates("fake_api_key")

    assert "rates" in result
    assert result["base"] == "USD"
    assert "BRL" in result["rates"]


# --- Test 2: Transform produces correct DataFrame shape ---
def test_transform_rates_produces_correct_columns():
    sample_api_data = {
        "timestamp": 1711843200,
        "base": "USD",
        "rates": {"BRL": 4.97, "EUR": 0.92, "GBP": 0.79}
    }

    df = transform_rates_to_dataframe(sample_api_data)

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 3   # 3 currencies in sample data
    assert "target_currency" in df.columns
    assert "exchange_rate" in df.columns
    assert "fetched_at" in df.columns
    assert "base_currency" in df.columns


# --- Test 3: Transform produces correct values ---
def test_transform_rates_values_are_correct():
    sample_api_data = {
        "timestamp": 1711843200,
        "base": "USD",
        "rates": {"BRL": 4.97}
    }

    df = transform_rates_to_dataframe(sample_api_data)
    brl_row = df[df["target_currency"] == "BRL"].iloc[0]

    assert brl_row["exchange_rate"] == 4.97
    assert brl_row["base_currency"] == "USD"


# --- Test 4: Missing API key raises clear error ---
def test_missing_api_key_raises_value_error():
    with patch.dict("os.environ", {}, clear=True):
        with patch("src.ingestion.api_ingester.os.getenv", return_value=None):
            from src.ingestion.api_ingester import run_api_ingestion
            with pytest.raises(ValueError, match="EXCHANGE_RATES_API_KEY"):
                run_api_ingestion()