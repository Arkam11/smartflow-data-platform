import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from src.ingestion.csv_ingester import load_csv_to_bronze


def test_load_csv_reads_correct_row_count(tmp_path):
    # Create a temporary CSV file for testing
    # tmp_path is a pytest built-in that gives you a temporary folder
    test_csv = tmp_path / "test_orders.csv"
    test_csv.write_text("order_id,customer_id,status\nORD1,CUST1,delivered\nORD2,CUST2,shipped\n")

    mock_engine = MagicMock()

    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        result = load_csv_to_bronze(str(test_csv), "raw_orders", mock_engine)

    assert result == 2   # 2 data rows in our test CSV


def test_load_csv_uses_bronze_schema(tmp_path):
    test_csv = tmp_path / "test.csv"
    test_csv.write_text("id,value\n1,100\n")

    mock_engine = MagicMock()

    with patch("pandas.DataFrame.to_sql") as mock_to_sql:
        load_csv_to_bronze(str(test_csv), "raw_orders", mock_engine)
        # Verify to_sql was called with schema='bronze'
        call_kwargs = mock_to_sql.call_args.kwargs
        assert call_kwargs["schema"] == "bronze"