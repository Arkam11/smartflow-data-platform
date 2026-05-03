import os
import logging
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Path to the PostgreSQL JDBC driver JAR file
# Spark needs this to read from and write to PostgreSQL
JDBC_JAR_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "drivers",
    "postgresql-42.7.3.jar"
)

# JDBC connection URL — this is how Spark addresses your PostgreSQL database
# Format: jdbc:postgresql://host:port/database_name
JDBC_URL = (
    f"jdbc:postgresql://"
    f"{os.getenv('DB_HOST', 'localhost')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'smartflow')}"
)

# Connection properties — credentials Spark passes to PostgreSQL
JDBC_PROPERTIES = {
    "user":     os.getenv("DB_USER", "smartflow_user"),
    "password": os.getenv("DB_PASSWORD", "smartflow_pass"),
    "driver":   "org.postgresql.Driver"
}


def get_spark(app_name: str = "SmartFlowETL") -> SparkSession:
    """
    Create and return a configured SparkSession.

    Why these specific settings:
    - master("local[*]")     : run locally using all available CPU cores
                               In production this would be "spark://cluster:7077"
    - spark.jars             : tell Spark where the PostgreSQL JDBC driver is
    - shuffle.partitions = 4 : how many partitions when joining/grouping data
                               Default is 200 — way too many for local work
    - adaptive.enabled       : lets Spark automatically optimise query plans
    - log level WARNING      : suppress the flood of INFO logs Spark produces

    Args:
        app_name: Name shown in Spark UI — use a descriptive name per job

    Returns:
        Active SparkSession
    """
    if not os.path.exists(JDBC_JAR_PATH):
        raise FileNotFoundError(
            f"PostgreSQL JDBC driver not found at: {JDBC_JAR_PATH}\n"
            f"Run: wget -O drivers/postgresql-42.7.3.jar "
            f"https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
        )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars", JDBC_JAR_PATH)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    # Set log level to WARNING to suppress Spark's very verbose INFO output
    # You'll still see your own logger.info() messages clearly
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"SparkSession started — app: {app_name} | "
                f"Spark version: {spark.version}")

    return spark


def read_bronze_table(spark: SparkSession, table_name: str):
    """
    Read a table from the Bronze schema into a Spark DataFrame.

    A Spark DataFrame is similar to a pandas DataFrame but:
    - It is LAZY — no data is actually read until you call an action
    - It can be distributed across many machines
    - Transformations build up a query plan, executed all at once

    Args:
        spark      : active SparkSession
        table_name : table name inside bronze schema e.g. "raw_orders"

    Returns:
        Spark DataFrame containing the Bronze table data
    """
    full_table = f"bronze.{table_name}"
    logger.info(f"Reading Bronze table: {full_table}")

    df = spark.read.jdbc(
        url=JDBC_URL,
        table=full_table,
        properties=JDBC_PROPERTIES
    )

    logger.info(f"  Schema: {[f.name for f in df.schema.fields]}")
    return df


def write_silver_table(df, table_name: str, mode: str = "overwrite"):
    """
    Write a Spark DataFrame to the Silver schema in PostgreSQL.
    Drops with CASCADE first to handle dependent dbt views.
    """
    full_table = f"silver.{table_name}"
    logger.info(f"Writing to Silver table: {full_table} (mode={mode})")

    # Drop with CASCADE to remove dependent dbt views before overwriting
    # dbt recreates these views automatically when you run dbt run
    from sqlalchemy import create_engine, text
    import os
    engine = create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {full_table} CASCADE"))
        conn.commit()

    df.write.jdbc(
        url=JDBC_URL,
        table=full_table,
        mode="overwrite",
        properties=JDBC_PROPERTIES
    )

    logger.info(f"  Done — {full_table} written successfully")