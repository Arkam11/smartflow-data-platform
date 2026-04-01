import pytest
import os
from unittest.mock import patch, MagicMock


def test_jdbc_url_uses_env_variables():
    with patch.dict("os.environ", {
        "DB_HOST": "testhost",
        "DB_PORT": "5433",
        "DB_NAME": "testdb"
    }):
        # Re-import to pick up patched env vars
        import importlib
        import src.etl.spark_session as ss
        importlib.reload(ss)

        assert "testhost" in ss.JDBC_URL
        assert "5433" in ss.JDBC_URL
        assert "testdb" in ss.JDBC_URL


def test_jdbc_properties_contain_required_keys():
    from src.etl.spark_session import JDBC_PROPERTIES
    assert "user" in JDBC_PROPERTIES
    assert "password" in JDBC_PROPERTIES
    assert "driver" in JDBC_PROPERTIES
    assert JDBC_PROPERTIES["driver"] == "org.postgresql.Driver"


def test_missing_jar_raises_file_not_found(tmp_path):
    with patch("src.etl.spark_session.JDBC_JAR_PATH", "/nonexistent/path.jar"):
        from src.etl.spark_session import get_spark
        with pytest.raises(FileNotFoundError, match="JDBC driver not found"):
            get_spark("TestApp")