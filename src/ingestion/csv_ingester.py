import os
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Database connection ---
# SQLAlchemy needs a "connection string" - think of it as the full address
# of your database including username, password, host, port and database name
def get_engine():
    db_url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_url)


# --- Create Bronze schema ---
# In PostgreSQL, a "schema" is like a namespace or folder for tables.
# We use "bronze" schema to hold all raw ingested data.
def create_bronze_schema(engine):
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
        conn.commit()
    logger.info("Bronze schema ready")


# --- Load a single CSV into a Bronze table ---
# pandas reads the CSV into a DataFrame (like an in-memory spreadsheet)
# then writes it to PostgreSQL using the engine connection
# if_exists='replace' means: if the table already exists, drop and recreate it
# this is fine for Bronze - we always reload raw data fresh
def load_csv_to_bronze(file_path: str, table_name: str, engine):
    logger.info(f"Loading {file_path} → bronze.{table_name}")

    df = pd.read_csv(file_path)

    logger.info(f"  Rows: {len(df):,}  |  Columns: {list(df.columns)}")

    # Drop table with CASCADE first to handle dependent views
    with engine.connect() as conn:
        conn.execute(text(
            f"DROP TABLE IF EXISTS bronze.{table_name} CASCADE"
        ))
        conn.commit()

    df.to_sql(
        name=table_name,
        con=engine,
        schema='bronze',
        if_exists='replace',
        index=False,            # don't write pandas row numbers as a column
        chunksize=1000          # write 1000 rows at a time - avoids memory issues
    )
    
    logger.info(f"  Done — bronze.{table_name} loaded successfully")
    return len(df)


# --- Main function - runs all CSV files ---
def run_csv_ingestion():
    raw_data_path = "data/raw"

    # This dictionary maps each CSV filename to its Bronze table name
    # This IS your first data mapping - source file to target table
    csv_files = {
        "olist_orders_dataset.csv":         "raw_orders",
        "olist_customers_dataset.csv":      "raw_customers",
        "olist_order_items_dataset.csv":    "raw_order_items",
        "olist_products_dataset.csv":       "raw_products",
        "olist_order_reviews_dataset.csv":  "raw_reviews",
        "olist_order_payments_dataset.csv": "raw_payments",
    }

    engine = get_engine()
    create_bronze_schema(engine)

    total_rows = 0
    for filename, table_name in csv_files.items():
        file_path = os.path.join(raw_data_path, filename)

        if not os.path.exists(file_path):
            logger.warning(f"File not found, skipping: {file_path}")
            continue

        rows = load_csv_to_bronze(file_path, table_name, engine)
        total_rows += rows

    logger.info(f"Ingestion complete — total rows loaded: {total_rows:,}")


if __name__ == "__main__":
    run_csv_ingestion()