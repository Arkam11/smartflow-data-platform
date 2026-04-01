import os
import logging
import requests
import pandas as pd
from datetime import datetime, timezone
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


# --- Fetch rates from the API ---
# requests.get() makes an HTTP GET call — like your browser visiting a URL
# response.json() parses the JSON text into a Python dictionary
# We raise an exception immediately if the API returns an error code
def fetch_exchange_rates(api_key: str) -> dict:
    url = f"https://openexchangerates.org/api/latest.json"
    params = {
        "app_id": api_key,
        "base": "USD"
    }

    logger.info("Calling Open Exchange Rates API...")

    response = requests.get(url, params=params, timeout=30)

    # raise_for_status() throws an exception if HTTP status is 4xx or 5xx
    # This catches things like wrong API key (401) or rate limit (429)
    response.raise_for_status()

    data = response.json()
    logger.info(f"API response received — {len(data['rates'])} currencies found")
    logger.info(f"Base currency: {data['base']}")
    logger.info(f"Rate timestamp: {datetime.fromtimestamp(data['timestamp'], tz=timezone.utc)}")

    return data


# --- Transform API response into a DataFrame ---
# The raw API gives us a nested dictionary: {"BRL": 4.97, "EUR": 0.92 ...}
# We need to flatten this into rows: [{currency: "BRL", rate: 4.97}, ...]
# We also add metadata columns — when we fetched it and what the base currency is
# This is important: without timestamps, you can't track rate changes over time
def transform_rates_to_dataframe(api_data: dict) -> pd.DataFrame:
    fetched_at = datetime.now(tz=timezone.utc)
    rate_timestamp = datetime.fromtimestamp(api_data['timestamp'], tz=timezone.utc)
    base_currency = api_data['base']

    rows = []
    for currency_code, rate_value in api_data['rates'].items():
        rows.append({
            "base_currency":    base_currency,
            "target_currency":  currency_code,
            "exchange_rate":    rate_value,
            "rate_timestamp":   rate_timestamp,
            "fetched_at":       fetched_at
        })

    df = pd.DataFrame(rows)
    logger.info(f"Transformed into DataFrame: {len(df)} rows")
    logger.info(f"Columns: {list(df.columns)}")

    # Log a few sample rates so we can visually verify the data looks right
    sample = df[df['target_currency'].isin(['BRL', 'EUR', 'GBP', 'LKR'])]
    for _, row in sample.iterrows():
        logger.info(f"  Sample rate: 1 USD = {row['exchange_rate']} {row['target_currency']}")

    return df


# --- Load DataFrame into Bronze table ---
# We use if_exists='append' here — NOT 'replace' like we did for CSV files
# Why? Because exchange rates change daily. We want to KEEP historical snapshots.
# Each run adds new rows with a new timestamp — building a history over time.
# If we used 'replace', we'd lose all previous snapshots every time we run.
def load_rates_to_bronze(df: pd.DataFrame, engine):
    logger.info("Loading exchange rates → bronze.raw_exchange_rates")

    # Create bronze schema if it doesn't exist yet
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.commit()

    df.to_sql(
        name="raw_exchange_rates",
        con=engine,
        schema="bronze",
        if_exists="append",   # append = keep history, never delete old rows
        index=False,
        chunksize=500
    )

    logger.info(f"Done — {len(df)} exchange rate rows appended to bronze.raw_exchange_rates")


# --- Verify what's in the table after loading ---
def verify_loaded_data(engine):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 
                COUNT(*)           AS total_rows,
                COUNT(DISTINCT target_currency) AS currencies,
                MIN(fetched_at)    AS first_fetch,
                MAX(fetched_at)    AS latest_fetch
            FROM bronze.raw_exchange_rates
        """))
        row = result.fetchone()
        logger.info(f"Verification — Total rows: {row[0]} | Currencies: {row[1]}")
        logger.info(f"Verification — First fetch: {row[2]} | Latest fetch: {row[3]}")


# --- Main function ---
def run_api_ingestion():
    api_key = os.getenv("EXCHANGE_RATES_API_KEY")

    if not api_key:
        raise ValueError("EXCHANGE_RATES_API_KEY not found in .env file")

    engine = get_engine()

    # Step 1: fetch from API
    api_data = fetch_exchange_rates(api_key)

    # Step 2: transform into tabular format
    df = transform_rates_to_dataframe(api_data)

    # Step 3: load into Bronze layer
    load_rates_to_bronze(df, engine)

    # Step 4: verify it landed correctly
    verify_loaded_data(engine)

    logger.info("API ingestion complete")


if __name__ == "__main__":
    run_api_ingestion()