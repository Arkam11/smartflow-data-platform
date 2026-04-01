import os
import logging
import pandas as pd
from sqlalchemy import create_engine
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


def load_silver_table(table_name: str) -> pd.DataFrame:
    """Load a Silver table into pandas for validation."""
    engine = get_engine()
    df = pd.read_sql(f"SELECT * FROM silver.{table_name}", engine)
    logger.info(f"Loaded silver.{table_name} — {len(df):,} rows")
    return df


# ================================================================
# CORE VALIDATION ENGINE
# Each check function returns (passed: bool, message: str)
# ================================================================

def check_no_nulls(df: pd.DataFrame, column: str):
    null_count = df[column].isna().sum()
    passed = null_count == 0
    msg = f"no nulls in '{column}'" if passed else f"{null_count:,} nulls found in '{column}'"
    return passed, msg


def check_values_in_set(df: pd.DataFrame, column: str, allowed: set):
    invalid = df[~df[column].isin(allowed)][column].dropna()
    passed = len(invalid) == 0
    if passed:
        msg = f"all values in '{column}' are in allowed set"
    else:
        sample = invalid.unique()[:5].tolist()
        msg = f"{len(invalid):,} invalid values in '{column}' — sample: {sample}"
    return passed, msg


def check_values_between(df: pd.DataFrame, column: str,
                          min_val, max_val, mostly: float = 1.0):
    col = pd.to_numeric(df[column], errors='coerce').dropna()
    if len(col) == 0:
        return True, f"'{column}' has no non-null numeric values to check"
    out_of_range = col[(col < min_val) | (col > max_val)]
    fail_rate = len(out_of_range) / len(col)
    passed = fail_rate <= (1 - mostly)
    if passed:
        msg = f"'{column}' values are between {min_val} and {max_val}"
    else:
        msg = (f"{len(out_of_range):,} values in '{column}' outside "
               f"[{min_val}, {max_val}] — {fail_rate:.1%} failure rate")
    return passed, msg


def check_row_count(df: pd.DataFrame, min_rows: int, max_rows: int):
    count = len(df)
    passed = min_rows <= count <= max_rows
    msg = (f"row count {count:,} is within [{min_rows:,}, {max_rows:,}]"
           if passed else
           f"row count {count:,} is OUTSIDE expected range [{min_rows:,}, {max_rows:,}]")
    return passed, msg


def check_unique(df: pd.DataFrame, column: str):
    total = len(df[column].dropna())
    unique = df[column].dropna().nunique()
    passed = total == unique
    msg = (f"'{column}' is unique" if passed
           else f"'{column}' has {total - unique:,} duplicate values")
    return passed, msg


def run_checks(df: pd.DataFrame, table_name: str, checks: list) -> dict:
    """
    Run a list of check functions against a DataFrame.

    Each check in the list is a dict:
        description : human readable label
        fn          : the check function to call
        args        : positional args after df

    Returns a result dict with pass/fail counts and details.
    """
    logger.info(f"Validating silver.{table_name}...")
    passed_count = 0
    failed_count = 0
    details = {}

    for check in checks:
        desc = check["description"]
        fn   = check["fn"]
        args = check.get("args", [])
        kwargs = check.get("kwargs", {})

        try:
            passed, message = fn(df, *args, **kwargs)
            details[desc] = {"success": passed, "message": message}
            if passed:
                logger.info(f"  PASS — {desc}")
                passed_count += 1
            else:
                logger.warning(f"  FAIL — {desc}: {message}")
                failed_count += 1
        except Exception as e:
            logger.error(f"  ERROR — {desc}: {e}")
            details[desc] = {"success": False, "message": str(e)}
            failed_count += 1

    logger.info(f"  Result: {passed_count} passed, {failed_count} failed "
                f"out of {len(checks)} checks")

    return {
        "table":   table_name,
        "passed":  passed_count,
        "failed":  failed_count,
        "total":   len(checks),
        "success": failed_count == 0,
        "details": details,
    }


# ================================================================
# EXPECTATION DEFINITIONS PER TABLE
# ================================================================

def get_orders_checks() -> list:
    return [
        {
            "description": "order_id has no nulls",
            "fn": check_no_nulls,
            "args": ["order_id"]
        },
        {
            "description": "customer_key has no nulls",
            "fn": check_no_nulls,
            "args": ["customer_key"]
        },
        {
            "description": "order_date has no nulls",
            "fn": check_no_nulls,
            "args": ["order_date"]
        },
        {
            "description": "status_code only contains known values",
            "fn": check_values_in_set,
            "args": ["status_code", {
                "delivered", "shipped", "canceled", "unavailable",
                "invoiced", "processing", "created", "approved"
            }]
        },
        {
            "description": "days_to_deliver is between 0 and 365",
            "fn": check_values_between,
            "args": ["days_to_deliver", 0, 365],
            "kwargs": {"mostly": 0.99}
        },
        {
            "description": "orders table has expected row count",
            "fn": check_row_count,
            "args": [50000, 200000]
        },
        {
            "description": "order_id is unique",
            "fn": check_unique,
            "args": ["order_id"]
        },
    ]


def get_order_items_checks() -> list:
    return [
        {
            "description": "order_id has no nulls",
            "fn": check_no_nulls,
            "args": ["order_id"]
        },
        {
            "description": "product_key has no nulls",
            "fn": check_no_nulls,
            "args": ["product_key"]
        },
        {
            "description": "item_price_brl is between 0 and 10,000",
            "fn": check_values_between,
            "args": ["item_price_brl", 0, 10000],
            "kwargs": {"mostly": 0.999}
        },
        {
            "description": "freight_cost_brl is not negative",
            "fn": check_values_between,
            "args": ["freight_cost_brl", 0, 5000],
            "kwargs": {"mostly": 0.999}
        },
        {
            "description": "total_item_cost_brl is not negative",
            "fn": check_values_between,
            "args": ["total_item_cost_brl", 0, 15000],
            "kwargs": {"mostly": 0.999}
        },
    ]


def get_reviews_checks() -> list:
    return [
        {
            "description": "review_id has no nulls",
            "fn": check_no_nulls,
            "args": ["review_id"]
        },
        {
            "description": "order_id has no nulls",
            "fn": check_no_nulls,
            "args": ["order_id"]
        },
        {
            "description": "rating is 1 to 5 only",
            "fn": check_values_in_set,
            "args": ["rating", {1, 2, 3, 4, 5}]
        },
        {
            "description": "rating is between 1 and 5",
            "fn": check_values_between,
            "args": ["rating", 1, 5]
        },
    ]


# ================================================================
# MAIN RUNNER
# ================================================================

def run_validation() -> bool:
    logger.info("=" * 60)
    logger.info("Starting data quality validation")
    logger.info("=" * 60)

    validations = [
        ("orders",      get_orders_checks()),
        ("order_items", get_order_items_checks()),
        ("reviews",     get_reviews_checks()),
    ]

    all_results = []
    overall_passed = True

    for table_name, checks in validations:
        logger.info(f"\n{'=' * 40}")
        df = load_silver_table(table_name)
        result = run_checks(df, table_name, checks)
        all_results.append(result)
        if not result["success"]:
            overall_passed = False

    logger.info("\n" + "=" * 60)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 60)

    total_checks = 0
    total_passed = 0
    for r in all_results:
        status = "ALL PASSED" if r["success"] else f"{r['failed']} FAILED"
        logger.info(f"  {r['table']:<20} {r['passed']}/{r['total']} checks — {status}")
        total_checks += r["total"]
        total_passed += r["passed"]

    logger.info(f"\n  Overall: {total_passed}/{total_checks} checks passed")
    if overall_passed:
        logger.info("  Result: DATA QUALITY PASSED")
    else:
        logger.warning("  Result: DATA QUALITY FAILED")

    return overall_passed


if __name__ == "__main__":
    success = run_validation()
    exit(0 if success else 1)