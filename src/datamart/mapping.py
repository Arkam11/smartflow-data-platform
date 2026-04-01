"""
Data Mapping Document — SmartFlow Data Platform
================================================
This file defines all column mappings from Bronze (raw source)
to Silver (cleaned) to Gold (Data Mart) layers.

Each mapping entry contains:
  source_col   : exact column name as it arrives from the source
  target_col   : column name in the target table
  data_type    : target data type we cast it to
  transform    : what transformation is applied (if any)
  nullable     : whether nulls are allowed in the target
  description  : plain English explanation of the field
"""

# ===========================================================
# ORDERS MAPPING
# Source: bronze.raw_orders → Target: silver.orders
# ===========================================================
ORDERS_MAPPING = [
    {
        "source_col":  "order_id",
        "target_col":  "order_id",
        "data_type":   "STRING",
        "transform":   "none",
        "nullable":    False,
        "description": "Unique identifier for each order"
    },
    {
        "source_col":  "customer_id",
        "target_col":  "customer_key",
        "data_type":   "STRING",
        "transform":   "rename only",
        "nullable":    False,
        "description": "Foreign key linking order to customer"
    },
    {
        "source_col":  "order_status",
        "target_col":  "status_code",
        "data_type":   "STRING",
        "transform":   "lowercase + strip whitespace",
        "nullable":    False,
        "description": "Current status: delivered, shipped, cancelled, etc."
    },
    {
        "source_col":  "order_purchase_timestamp",
        "target_col":  "order_date",
        "data_type":   "TIMESTAMP",
        "transform":   "cast string to timestamp",
        "nullable":    False,
        "description": "Date and time the customer placed the order"
    },
    {
        "source_col":  "order_delivered_customer_date",
        "target_col":  "delivered_date",
        "data_type":   "TIMESTAMP",
        "transform":   "cast string to timestamp, allow null",
        "nullable":    True,
        "description": "Date order was delivered — null if not yet delivered"
    },
    {
        "source_col":  "order_estimated_delivery_date",
        "target_col":  "estimated_delivery_date",
        "data_type":   "TIMESTAMP",
        "transform":   "cast string to timestamp",
        "nullable":    True,
        "description": "Estimated delivery date given to customer at purchase"
    },
]

# ===========================================================
# CUSTOMERS MAPPING
# Source: bronze.raw_customers → Target: silver.customers
# ===========================================================
CUSTOMERS_MAPPING = [
    {
        "source_col":  "customer_id",
        "target_col":  "customer_key",
        "data_type":   "STRING",
        "transform":   "rename only",
        "nullable":    False,
        "description": "Primary key for customer dimension"
    },
    {
        "source_col":  "customer_unique_id",
        "target_col":  "customer_id",
        "data_type":   "STRING",
        "transform":   "rename only",
        "nullable":    False,
        "description": "True unique customer — one person may have multiple customer_keys"
    },
    {
        "source_col":  "customer_city",
        "target_col":  "city",
        "data_type":   "STRING",
        "transform":   "lowercase + strip whitespace + title case",
        "nullable":    True,
        "description": "City where the customer is located"
    },
    {
        "source_col":  "customer_state",
        "target_col":  "state_code",
        "data_type":   "STRING",
        "transform":   "uppercase",
        "nullable":    True,
        "description": "Brazilian state abbreviation e.g. SP, RJ, MG"
    },
    {
        "source_col":  "customer_zip_code_prefix",
        "target_col":  "zip_prefix",
        "data_type":   "STRING",
        "transform":   "cast integer to string, zero-pad to 5 digits",
        "nullable":    True,
        "description": "First 5 digits of postal code"
    },
]

# ===========================================================
# ORDER ITEMS MAPPING
# Source: bronze.raw_order_items → Target: silver.order_items
# ===========================================================
ORDER_ITEMS_MAPPING = [
    {
        "source_col":  "order_id",
        "target_col":  "order_id",
        "data_type":   "STRING",
        "transform":   "none",
        "nullable":    False,
        "description": "Foreign key linking item to its parent order"
    },
    {
        "source_col":  "product_id",
        "target_col":  "product_key",
        "data_type":   "STRING",
        "transform":   "rename only",
        "nullable":    False,
        "description": "Foreign key linking item to product dimension"
    },
    {
        "source_col":  "price",
        "target_col":  "item_price_brl",
        "data_type":   "DECIMAL(10,2)",
        "transform":   "rename + add currency suffix to name",
        "nullable":    False,
        "description": "Price of the item in Brazilian Real"
    },
    {
        "source_col":  "freight_value",
        "target_col":  "freight_cost_brl",
        "data_type":   "DECIMAL(10,2)",
        "transform":   "rename + add currency suffix to name",
        "nullable":    False,
        "description": "Shipping cost for this item in Brazilian Real"
    },
    {
        "source_col":  None,
        "target_col":  "total_item_cost_brl",
        "data_type":   "DECIMAL(10,2)",
        "transform":   "derived",
        "nullable":    True,
        "description": "Calculated total cost — item price plus freight (price + freight_value)"
    },
]

# ===========================================================
# REVIEWS MAPPING
# Source: bronze.raw_reviews → Target: silver.reviews
# ===========================================================
REVIEWS_MAPPING = [
    {
        "source_col":  "review_id",
        "target_col":  "review_id",
        "data_type":   "STRING",
        "transform":   "none",
        "nullable":    False,
        "description": "Unique identifier for the review"
    },
    {
        "source_col":  "order_id",
        "target_col":  "order_id",
        "data_type":   "STRING",
        "transform":   "none",
        "nullable":    False,
        "description": "Foreign key linking review to its order"
    },
    {
        "source_col":  "review_score",
        "target_col":  "rating",
        "data_type":   "INTEGER",
        "transform":   "rename only — values are 1 to 5",
        "nullable":    False,
        "description": "Customer satisfaction score from 1 (worst) to 5 (best)"
    },
    {
        "source_col":  "review_comment_message",
        "target_col":  "review_text",
        "data_type":   "TEXT",
        "transform":   "rename + strip whitespace",
        "nullable":    True,
        "description": "Free-text customer review — used for LLM annotation in Phase 4"
    },
    {
        "source_col":  "review_creation_date",
        "target_col":  "review_date",
        "data_type":   "TIMESTAMP",
        "transform":   "cast string to timestamp",
        "nullable":    True,
        "description": "Date the customer submitted the review"
    },
    {
        "source_col":  None,
        "target_col":  "sentiment_label",
        "data_type":   "STRING",
        "transform":   "derived",
        "nullable":    True,
        "description": "AI-generated sentiment: positive, neutral, or negative — populated in Phase 4"
    },
]

# ===========================================================
# EXCHANGE RATES MAPPING
# Source: bronze.raw_exchange_rates → Target: silver.exchange_rates
# ===========================================================
EXCHANGE_RATES_MAPPING = [
    {
        "source_col":  "base_currency",
        "target_col":  "base_currency",
        "data_type":   "STRING",
        "transform":   "none",
        "nullable":    False,
        "description": "The currency rates are expressed relative to (always USD)"
    },
    {
        "source_col":  "target_currency",
        "target_col":  "target_currency",
        "data_type":   "STRING",
        "transform":   "uppercase",
        "nullable":    False,
        "description": "The currency being converted to"
    },
    {
        "source_col":  "exchange_rate",
        "target_col":  "rate",
        "data_type":   "DECIMAL(18,6)",
        "transform":   "rename only",
        "nullable":    False,
        "description": "How many target currency units equal 1 USD"
    },
    {
        "source_col":  "fetched_at",
        "target_col":  "captured_at",
        "data_type":   "TIMESTAMP",
        "transform":   "rename only",
        "nullable":    False,
        "description": "When this rate was fetched from the API"
    },
]

# ===========================================================
# MASTER REGISTRY
# All mappings collected in one place for easy iteration
# ETL scripts in Phase 3 will import this to drive transformations
# ===========================================================
ALL_MAPPINGS = {
    "orders":         ORDERS_MAPPING,
    "customers":      CUSTOMERS_MAPPING,
    "order_items":    ORDER_ITEMS_MAPPING,
    "reviews":        REVIEWS_MAPPING,
    "exchange_rates": EXCHANGE_RATES_MAPPING,
}


def get_mapping(table_name: str) -> list:
    """
    Retrieve the mapping for a specific table.
    Used by ETL scripts in Phase 3 to know what transformations to apply.

    Example:
        from src.datamart.mapping import get_mapping
        orders_map = get_mapping("orders")
    """
    if table_name not in ALL_MAPPINGS:
        raise KeyError(f"No mapping found for table: '{table_name}'. "
                       f"Available: {list(ALL_MAPPINGS.keys())}")
    return ALL_MAPPINGS[table_name]


def print_mapping_summary():
    """
    Prints a human-readable summary of all mappings.
    Run this file directly to see the full mapping report.
    """
    print("\n" + "="*60)
    print("SMARTFLOW DATA MAPPING SUMMARY")
    print("="*60)

    for table, mappings in ALL_MAPPINGS.items():
        print(f"\nTable: {table.upper()} ({len(mappings)} columns)")
        print("-" * 60)
        print(f"  {'SOURCE COLUMN':<35} {'TARGET COLUMN':<30} {'TYPE'}")
        print(f"  {'-'*33} {'-'*28} {'-'*15}")

        for m in mappings:
            source = m["source_col"] if m["source_col"] else "(derived)"
            print(f"  {source:<35} {m['target_col']:<30} {m['data_type']}")

    print("\n" + "="*60)
    total = sum(len(v) for v in ALL_MAPPINGS.values())
    print(f"Total mappings defined: {total} columns across {len(ALL_MAPPINGS)} tables")
    print("="*60 + "\n")


if __name__ == "__main__":
    print_mapping_summary()