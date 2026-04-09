-- Create all schemas needed by SmartFlow pipeline
-- This runs automatically on first PostgreSQL startup in Docker

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS ml;

-- Create a separate database for Airflow metadata
-- Airflow needs its own database separate from pipeline data
CREATE DATABASE airflow_db;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE smartflow TO smartflow_user;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO smartflow_user;