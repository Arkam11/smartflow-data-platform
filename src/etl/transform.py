import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DecimalType, IntegerType, StringType
from src.etl.spark_session import get_spark, read_bronze_table, write_silver_table

logging.basicConfig(
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


# ================================================================
# HELPER FUNCTIONS
# ================================================================

def log_dataframe_stats(df: DataFrame, name: str):
    """Log row count and null counts — essential for monitoring data quality."""
    count = df.count()
    logger.info(f"  [{name}] rows: {count:,}")
    return count


def drop_duplicates_with_log(df: DataFrame, key_col: str, name: str) -> DataFrame:
    """
    Remove duplicate rows based on a key column and log how many were removed.
    Duplicates in Bronze are common — sources sometimes send the same record twice.
    """
    before = df.count()
    df_clean = df.dropDuplicates([key_col])
    after = df_clean.count()
    removed = before - after
    if removed > 0:
        logger.warning(f"  [{name}] Removed {removed:,} duplicate rows on '{key_col}'")
    else:
        logger.info(f"  [{name}] No duplicates found on '{key_col}'")
    return df_clean


def filter_nulls_with_log(df: DataFrame, critical_cols: list, name: str) -> DataFrame:
    """
    Remove rows where critical columns are null and log how many were dropped.
    Critical columns = columns that must exist for the row to be meaningful.
    Example: an order with no order_id is useless — we cannot link it to anything.
    """
    before = df.count()
    condition = None
    for col in critical_cols:
        if condition is None:
            condition = F.col(col).isNotNull()
        else:
            condition = condition & F.col(col).isNotNull()
    df_clean = df.filter(condition)
    after = df_clean.count()
    removed = before - after
    if removed > 0:
        logger.warning(f"  [{name}] Dropped {removed:,} rows with nulls in: {critical_cols}")
    else:
        logger.info(f"  [{name}] No null rows found in critical columns: {critical_cols}")
    return df_clean


# ================================================================
# ORDERS TRANSFORMATION
# Bronze: raw_orders → Silver: orders
# ================================================================

def transform_orders(spark: SparkSession) -> DataFrame:
    """
    Transform raw orders into clean silver orders.

    Key transformations:
    - Cast all date strings to proper timestamps
    - Rename columns per mapping document
    - Filter out rows with null order_id or customer_id
    - Remove duplicate orders
    - Add a derived column: days_to_deliver
    """
    logger.info("Transforming orders...")
    df = read_bronze_table(spark, "raw_orders")

    # Remove duplicates first — always do this before other transforms
    df = drop_duplicates_with_log(df, "order_id", "orders")

    # Filter critical nulls — orders without these fields are useless
    df = filter_nulls_with_log(df, ["order_id", "customer_id"], "orders")

    # Apply all transformations in one Spark operation
    # F.to_timestamp() converts string "2017-10-02 10:56:33" to a real timestamp
    # F.lower() + F.trim() standardises text fields
    # F.col() references a column by name
    df_silver = df.select(
        F.col("order_id"),
        F.col("customer_id").alias("customer_key"),
        F.lower(F.trim(F.col("order_status"))).alias("status_code"),
        F.to_timestamp("order_purchase_timestamp").alias("order_date"),
        F.to_timestamp("order_approved_at").alias("approved_date"),
        F.to_timestamp("order_delivered_carrier_date").alias("shipped_date"),
        F.to_timestamp("order_delivered_customer_date").alias("delivered_date"),
        F.to_timestamp("order_estimated_delivery_date").alias("estimated_delivery_date"),
    ).withColumn(
        # Derived column: how many days did delivery actually take?
        # datediff() calculates the difference between two dates in days
        # This will be null if delivered_date is null (undelivered orders)
        "days_to_deliver",
        F.datediff(
            F.col("delivered_date"),
            F.col("order_date")
        )
    ).withColumn(
        # Derived column: was the order delivered on time?
        # True if delivered before or on the estimated date
        "delivered_on_time",
        F.when(
            F.col("delivered_date").isNull(), None
        ).otherwise(
            F.col("delivered_date") <= F.col("estimated_delivery_date")
        )
    )

    count = log_dataframe_stats(df_silver, "silver_orders")
    logger.info(f"Orders transformation complete — {count:,} rows")
    return df_silver


# ================================================================
# CUSTOMERS TRANSFORMATION
# Bronze: raw_customers → Silver: customers
# ================================================================

def transform_customers(spark: SparkSession) -> DataFrame:
    """
    Transform raw customers into clean silver customers.

    Key transformations:
    - Standardise city names to Title Case
    - Uppercase state codes
    - Rename columns per mapping document
    - Zero-pad zip codes to 5 digits
    """
    logger.info("Transforming customers...")
    df = read_bronze_table(spark, "raw_customers")

    df = drop_duplicates_with_log(df, "customer_id", "customers")
    df = filter_nulls_with_log(df, ["customer_id", "customer_unique_id"], "customers")

    df_silver = df.select(
        F.col("customer_id").alias("customer_key"),
        F.col("customer_unique_id").alias("customer_id"),

        # Title Case: "sao paulo" → "Sao Paulo", "SAO PAULO" → "Sao Paulo"
        # initcap() does Title Case in Spark
        F.initcap(F.trim(F.col("customer_city"))).alias("city"),

        # Uppercase state codes: "sp" → "SP"
        F.upper(F.trim(F.col("customer_state"))).alias("state_code"),

        # Zero-pad zip to 5 digits: 1310 → "01310"
        # lpad() = left pad with a character up to a target length
        F.lpad(F.col("customer_zip_code_prefix").cast(StringType()), 5, "0").alias("zip_prefix"),
    )

    count = log_dataframe_stats(df_silver, "silver_customers")
    logger.info(f"Customers transformation complete — {count:,} rows")
    return df_silver


# ================================================================
# ORDER ITEMS TRANSFORMATION
# Bronze: raw_order_items → Silver: order_items
# ================================================================

def transform_order_items(spark: SparkSession) -> DataFrame:
    """
    Transform raw order items into clean silver order items.

    Key transformations:
    - Rename price columns to include currency suffix (BRL)
    - Add derived total_item_cost_brl = price + freight_value
    - Cast prices to proper Decimal type
    - Filter negative prices (data quality issue in source)
    """
    logger.info("Transforming order items...")
    df = read_bronze_table(spark, "raw_order_items")

    df = filter_nulls_with_log(df, ["order_id", "product_id"], "order_items")

    df_silver = df.select(
        F.col("order_id"),
        F.col("order_item_id"),
        F.col("product_id").alias("product_key"),
        F.col("seller_id"),
        F.to_timestamp("shipping_limit_date").alias("shipping_deadline"),

        # Cast to Decimal for precise financial calculations
        # Never use float/double for money — rounding errors accumulate
        F.col("price").cast(DecimalType(10, 2)).alias("item_price_brl"),
        F.col("freight_value").cast(DecimalType(10, 2)).alias("freight_cost_brl"),
    ).withColumn(
        # Derived: total cost = item price + freight
        "total_item_cost_brl",
        (F.col("item_price_brl") + F.col("freight_cost_brl")).cast(DecimalType(10, 2))
    ).filter(
        # Filter out any rows with negative prices — these are data errors
        F.col("item_price_brl") >= 0
    )

    count = log_dataframe_stats(df_silver, "silver_order_items")
    logger.info(f"Order items transformation complete — {count:,} rows")
    return df_silver


# ================================================================
# REVIEWS TRANSFORMATION
# Bronze: raw_reviews → Silver: reviews
# ================================================================

def transform_reviews(spark: SparkSession) -> DataFrame:
    """
    Transform raw reviews into clean silver reviews.

    Key transformations:
    - Rename columns per mapping
    - Cast review_score to Integer
    - Clean up review text (strip whitespace, handle nulls)
    - Add sentiment_label column as null placeholder (filled in Phase 4)
    """
    logger.info("Transforming reviews...")
    df = read_bronze_table(spark, "raw_reviews")

    df = drop_duplicates_with_log(df, "review_id", "reviews")
    df = filter_nulls_with_log(df, ["review_id", "order_id"], "reviews")

    df_silver = df.select(
        F.col("review_id"),
        F.col("order_id"),
        F.col("review_score").cast(IntegerType()).alias("rating"),

        # Clean review text — trim whitespace, keep nulls as null (not empty string)
        F.when(
            F.trim(F.col("review_comment_message")) == "", None
        ).otherwise(
            F.trim(F.col("review_comment_message"))
        ).alias("review_text"),

        F.to_timestamp("review_creation_date").alias("review_date"),
    ).withColumn(
        # Placeholder for Phase 4 LLM annotation
        # F.lit(None) creates a null column
        # cast(StringType()) gives it an explicit type
        "sentiment_label",
        F.lit(None).cast(StringType())
    )

    count = log_dataframe_stats(df_silver, "silver_reviews")
    logger.info(f"Reviews transformation complete — {count:,} rows")
    return df_silver


# ================================================================
# EXCHANGE RATES TRANSFORMATION
# Bronze: raw_exchange_rates → Silver: exchange_rates
# ================================================================

def transform_exchange_rates(spark: SparkSession) -> DataFrame:
    """
    Transform raw exchange rates into clean silver exchange rates.

    Key transformations:
    - Rename columns per mapping
    - Uppercase currency codes
    - Cast rate to high-precision Decimal
    - Keep only the most recent snapshot per currency
    """
    logger.info("Transforming exchange rates...")
    df = read_bronze_table(spark, "raw_exchange_rates")

    df_silver = df.select(
        F.upper(F.col("base_currency")).alias("base_currency"),
        F.upper(F.col("target_currency")).alias("target_currency"),
        F.col("exchange_rate").cast(DecimalType(18, 6)).alias("rate"),
        F.col("rate_timestamp"),
        F.col("fetched_at").alias("captured_at"),
    )

    # Keep only the latest rate per currency — deduplicate by keeping
    # the row with the most recent fetched_at for each target_currency
    from pyspark.sql.window import Window
    window = Window.partitionBy("target_currency").orderBy(F.col("captured_at").desc())

    df_silver = df_silver.withColumn(
        "row_num", F.row_number().over(window)
    ).filter(
        F.col("row_num") == 1
    ).drop("row_num")

    count = log_dataframe_stats(df_silver, "silver_exchange_rates")
    logger.info(f"Exchange rates transformation complete — {count:,} rows")
    return df_silver


# ================================================================
# MAIN RUNNER — orchestrates all transformations
# ================================================================

def run_silver_transformation():
    """
    Run the complete Bronze → Silver ETL pipeline.
    Each table is transformed independently then written to Silver.
    If one table fails, the error is logged but other tables still run.
    """
    logger.info("=" * 60)
    logger.info("Starting Bronze → Silver transformation")
    logger.info("=" * 60)

    spark = get_spark("SmartFlow-BronzeToSilver")

    results = {}

    # Define all transformations to run
    # Each entry: (function_to_call, silver_table_name)
    transformations = [
        (transform_orders,         "orders"),
        (transform_customers,      "customers"),
        (transform_order_items,    "order_items"),
        (transform_reviews,        "reviews"),
        (transform_exchange_rates, "exchange_rates"),
    ]

    for transform_fn, table_name in transformations:
        try:
            logger.info(f"\n--- Processing: {table_name} ---")
            df_silver = transform_fn(spark)
            write_silver_table(df_silver, table_name)
            results[table_name] = "SUCCESS"
        except Exception as e:
            logger.error(f"FAILED: {table_name} — {e}")
            results[table_name] = f"FAILED: {e}"

    spark.stop()

    # Print final summary
    logger.info("\n" + "=" * 60)
    logger.info("TRANSFORMATION SUMMARY")
    logger.info("=" * 60)
    for table, status in results.items():
        logger.info(f"  {table:<20} {status}")

    failed = [t for t, s in results.items() if s != "SUCCESS"]
    if failed:
        raise RuntimeError(f"Some transformations failed: {failed}")
    else:
        logger.info("\nAll transformations completed successfully")


if __name__ == "__main__":
    run_silver_transformation()