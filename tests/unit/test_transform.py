import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
import os

# We use a real local SparkSession for transformation tests
# This is the correct approach — transformation logic must be tested with real Spark
@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("SmartFlowTest")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_transform_orders_renames_columns(spark):
    from src.etl.transform import transform_orders

    # Create a minimal fake Bronze orders table
    data = [(
        "order_001", "cust_001", "delivered",
        "2017-10-02 10:56:33", "2017-10-02 11:07:15",
        "2017-10-04 19:55:00", "2017-10-10 21:25:13",
        "2017-10-18 00:00:00"
    )]
    columns = [
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    fake_df = spark.createDataFrame(data, columns)

    with patch("src.etl.transform.read_bronze_table", return_value=fake_df):
        result = transform_orders(spark)

    assert "customer_key" in result.columns   # renamed from customer_id
    assert "status_code" in result.columns    # renamed from order_status
    assert "order_date" in result.columns     # renamed from order_purchase_timestamp
    assert "customer_id" not in result.columns


def test_transform_orders_casts_timestamps(spark):
    from src.etl.transform import transform_orders
    from pyspark.sql.types import StructType, StructField, StringType

    # Explicitly define schema so Spark knows None columns are StringType
    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("order_approved_at", StringType(), True),
        StructField("order_delivered_carrier_date", StringType(), True),
        StructField("order_delivered_customer_date", StringType(), True),
        StructField("order_estimated_delivery_date", StringType(), True),
    ])

    data = [(
        "order_002", "cust_002", "shipped",
        "2018-05-15 09:30:00", None, None, None, "2018-06-01 00:00:00"
    )]

    fake_df = spark.createDataFrame(data, schema)

    with patch("src.etl.transform.read_bronze_table", return_value=fake_df):
        result = transform_orders(spark)

    # Check that order_date is now a TimestampType
    from pyspark.sql.types import TimestampType
    order_date_field = [f for f in result.schema.fields if f.name == "order_date"][0]
    assert isinstance(order_date_field.dataType, TimestampType)


def test_transform_orders_filters_null_order_id(spark):
    from src.etl.transform import transform_orders
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", StringType(), True),
        StructField("order_approved_at", StringType(), True),
        StructField("order_delivered_carrier_date", StringType(), True),
        StructField("order_delivered_customer_date", StringType(), True),
        StructField("order_estimated_delivery_date", StringType(), True),
    ])

    data = [
        ("order_003", "cust_003", "delivered",
         "2018-01-01 10:00:00", None, None, None, "2018-01-15 00:00:00"),
        (None, "cust_004", "delivered",
         "2018-01-02 10:00:00", None, None, None, "2018-01-16 00:00:00"),
    ]

    fake_df = spark.createDataFrame(data, schema)

    with patch("src.etl.transform.read_bronze_table", return_value=fake_df):
        result = transform_orders(spark)

    # Only 1 row should survive — the one with null order_id is dropped
    assert result.count() == 1


def test_transform_customers_title_case_city(spark):
    from src.etl.transform import transform_customers

    data = [
        ("cust_001", "unique_001", "1310", "SAO PAULO", "SP"),
        ("cust_002", "unique_002", "20040", "rio de janeiro", "RJ"),
    ]
    columns = [
        "customer_id", "customer_unique_id",
        "customer_zip_code_prefix", "customer_city", "customer_state"
    ]
    fake_df = spark.createDataFrame(data, columns)

    with patch("src.etl.transform.read_bronze_table", return_value=fake_df):
        result = transform_customers(spark)

    cities = [row["city"] for row in result.collect()]
    assert "Sao Paulo" in cities
    assert "Rio De Janeiro" in cities


def test_transform_order_items_derived_total(spark):
    from src.etl.transform import transform_order_items

    data = [("order_001", 1, "prod_001", "seller_001",
             "2018-01-10 00:00:00", "150.00", "12.50")]
    columns = [
        "order_id", "order_item_id", "product_id", "seller_id",
        "shipping_limit_date", "price", "freight_value"
    ]
    fake_df = spark.createDataFrame(data, columns)

    with patch("src.etl.transform.read_bronze_table", return_value=fake_df):
        result = transform_order_items(spark)

    row = result.collect()[0]
    assert float(row["item_price_brl"]) == 150.00
    assert float(row["freight_cost_brl"]) == 12.50
    assert float(row["total_item_cost_brl"]) == 162.50