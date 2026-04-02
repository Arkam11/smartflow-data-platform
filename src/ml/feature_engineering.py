import os
import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)


def get_engine():
    db_url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_url)


def load_silver_data(engine) -> dict:
    """
    Load all Silver tables needed for feature engineering.
    We need orders, customers, order_items, and reviews.
    """
    logger.info("Loading Silver tables for feature engineering...")

    orders = pd.read_sql("SELECT * FROM silver.orders", engine)
    customers = pd.read_sql("SELECT * FROM silver.customers", engine)
    items = pd.read_sql("SELECT * FROM silver.order_items", engine)
    reviews = pd.read_sql("SELECT * FROM silver.reviews", engine)
    payments = pd.read_sql("SELECT * FROM bronze.raw_payments", engine)

    logger.info(f"  orders: {len(orders):,} rows")
    logger.info(f"  customers: {len(customers):,} rows")
    logger.info(f"  order_items: {len(items):,} rows")
    logger.info(f"  reviews: {len(reviews):,} rows")
    logger.info(f"  payments: {len(payments):,} rows")

    return {
        "orders": orders,
        "customers": customers,
        "items": items,
        "reviews": reviews,
        "payments": payments
    }


def define_churn_label(orders: pd.DataFrame,
                        churn_days: int = 180) -> pd.DataFrame:
    """
    Define the churn label for each customer.

    Logic:
    - Find each customer's FIRST order date and LAST order date
    - If (last_order_date - first_order_date) < churn_days AND
      they only ordered once OR their last order was long ago → churned = 1
    - Otherwise → churned = 0

    We use 180 days as the threshold — a customer who hasn't ordered
    in 6 months after their first purchase is considered churned.

    This is called "binary classification" — the label is either 0 or 1.
    """
    logger.info("Defining churn labels...")

    orders["order_date"] = pd.to_datetime(orders["order_date"], utc=True)

    # Get the reference date — the latest order in the dataset
    # We use this instead of today() because the dataset is historical (2016-2018)
    reference_date = orders["order_date"].max()
    logger.info(f"  Reference date (dataset max): {reference_date.date()}")

    # Aggregate per customer: count orders, first order, last order
    customer_orders = orders.groupby("customer_key").agg(
        total_orders=("order_id", "count"),
        first_order_date=("order_date", "min"),
        last_order_date=("order_date", "max"),
    ).reset_index()

    # Days since last order (from reference date)
    customer_orders["days_since_last_order"] = (
        reference_date - customer_orders["last_order_date"]
    ).dt.days

    # Days between first and last order (customer lifetime so far)
    customer_orders["customer_lifetime_days"] = (
        customer_orders["last_order_date"] - customer_orders["first_order_date"]
    ).dt.days

    # Churn label:
    # A customer churned if they haven't ordered in churn_days days
    # AND their lifetime is long enough to meaningfully classify
    customer_orders["churned"] = (
        customer_orders["days_since_last_order"] >= churn_days
    ).astype(int)

    churned_count = customer_orders["churned"].sum()
    total = len(customer_orders)
    logger.info(f"  Churned: {churned_count:,} ({churned_count/total:.1%}) | "
                f"Active: {total-churned_count:,} ({(total-churned_count)/total:.1%})")

    return customer_orders


def build_customer_features(data: dict) -> pd.DataFrame:
    """
    Build the feature table — one row per customer, one column per feature.

    Features we engineer:
    - total_orders          : how many times they ordered
    - avg_order_value       : average spend per order
    - total_spend           : total money spent
    - avg_rating            : average review score they gave
    - avg_days_to_deliver   : average delivery time they experienced
    - late_delivery_rate    : proportion of orders delivered late
    - payment_type_mode     : most common payment method (encoded)
    - state_code            : which state they're from (encoded)
    - days_since_last_order : recency — critical churn predictor
    - customer_lifetime_days: how long they've been a customer
    """
    logger.info("Engineering customer features...")

    orders    = data["orders"]
    customers = data["customers"]
    items     = data["items"]
    reviews   = data["reviews"]
    payments  = data["payments"]

    orders["order_date"] = pd.to_datetime(orders["order_date"], utc=True)

    # --- Order-level features ---
    order_features = orders.groupby("customer_key").agg(
        total_orders=("order_id", "count"),
        avg_days_to_deliver=("days_to_deliver", "mean"),
        late_delivery_count=("delivered_on_time",
                              lambda x: (x == False).sum()),
    ).reset_index()

    order_features["late_delivery_rate"] = (
        order_features["late_delivery_count"] /
        order_features["total_orders"]
    ).fillna(0)

    # --- Spend features (from order items) ---
    # Join items to orders to get customer_key
    items_with_customer = items.merge(
        orders[["order_id", "customer_key"]],
        on="order_id",
        how="left"
    )
    spend_features = items_with_customer.groupby("customer_key").agg(
        total_spend=("total_item_cost_brl", "sum"),
        avg_order_value=("total_item_cost_brl", "mean"),
    ).reset_index()

    # --- Review features ---
    reviews_with_customer = reviews.merge(
        orders[["order_id", "customer_key"]],
        on="order_id",
        how="left"
    )
    review_features = reviews_with_customer.groupby("customer_key").agg(
        avg_rating=("rating", "mean"),
        review_count=("review_id", "count"),
    ).reset_index()

    # --- Payment type features ---
    payments_with_customer = payments.merge(
        orders[["order_id", "customer_key"]],
        on="order_id",
        how="left"
    )
    # Get the most common payment type per customer
    payment_mode = (
        payments_with_customer.groupby("customer_key")["payment_type"]
        .agg(lambda x: x.mode().iloc[0] if len(x) > 0 else "unknown")
        .reset_index()
        .rename(columns={"payment_type": "primary_payment_type"})
    )

    # --- Customer location features ---
    location = customers[["customer_key", "state_code"]].drop_duplicates()

    # --- Churn label ---
    churn_labels = define_churn_label(orders)
    churn_labels = churn_labels[[
        "customer_key", "churned",
        "days_since_last_order", "customer_lifetime_days"
    ]]

    # --- Merge everything together ---
    logger.info("Merging all features...")
    features = churn_labels.copy()
    features = features.merge(order_features, on="customer_key", how="left")
    features = features.merge(spend_features,  on="customer_key", how="left")
    features = features.merge(review_features, on="customer_key", how="left")
    features = features.merge(payment_mode,    on="customer_key", how="left")
    features = features.merge(location,         on="customer_key", how="left")

    # --- Encode categorical columns ---
    # ML models need numbers — convert text categories to integers
    # Label encoding: each unique value gets a unique integer
    features["payment_type_encoded"] = (
        features["primary_payment_type"]
        .astype("category").cat.codes
    )
    features["state_encoded"] = (
        features["state_code"]
        .astype("category").cat.codes
    )

    # Fill remaining nulls with sensible defaults
    features["avg_rating"]          = features["avg_rating"].fillna(3.0)
    features["review_count"]        = features["review_count"].fillna(0)
    features["avg_days_to_deliver"] = features["avg_days_to_deliver"].fillna(
        features["avg_days_to_deliver"].median()
    )
    features["total_spend"]         = features["total_spend"].fillna(0)
    features["avg_order_value"]     = features["avg_order_value"].fillna(0)

    logger.info(f"Feature table built: {len(features):,} customers, "
                f"{len(features.columns)} columns")
    logger.info(f"Columns: {list(features.columns)}")

    return features


def save_feature_store(features: pd.DataFrame, engine):
    """
    Save the feature store to PostgreSQL.
    This becomes the single source of truth for ML features.
    """
    logger.info("Saving feature store to PostgreSQL...")

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS ml"))
        conn.commit()

    features.to_sql(
        name="feature_store",
        con=engine,
        schema="ml",
        if_exists="replace",
        index=False,
        chunksize=1000
    )
    logger.info(f"Saved ml.feature_store — {len(features):,} rows")


def run_feature_engineering():
    engine = get_engine()
    data = load_silver_data(engine)
    features = build_customer_features(data)
    save_feature_store(features, engine)
    return features


if __name__ == "__main__":
    run_feature_engineering()