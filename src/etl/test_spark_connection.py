"""
Quick smoke test — run this to verify:
1. SparkSession starts without errors
2. Spark can read from PostgreSQL Bronze tables
3. JDBC driver is working correctly

Run with: python src/etl/test_spark_connection.py
"""
import logging
from src.etl.spark_session import get_spark, read_bronze_table

logging.basicConfig(
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
    level=logging.INFO
)

def main():
    print("\n--- Starting Spark smoke test ---\n")

    # Start Spark — first time takes 15-30 seconds as JVM initialises
    spark = get_spark("SmokTest")

    print(f"\nSpark version: {spark.version}")
    print(f"Python version: {spark.sparkContext.pythonVer}")

    # Read a small Bronze table
    df = read_bronze_table(spark, "raw_orders")

    # count() is an ACTION — this triggers actual data reading
    row_count = df.count()
    print(f"\nBronze raw_orders row count: {row_count:,}")

    # Show schema — this tells you Spark's inferred data types
    print("\nSchema detected by Spark:")
    df.printSchema()

    # Show 3 rows
    print("\nSample rows:")
    df.show(3, truncate=True)

    spark.stop()
    print("\n--- Smoke test complete ---")

if __name__ == "__main__":
    main()