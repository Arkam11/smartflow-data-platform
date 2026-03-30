# SmartFlow Data Platform

An end-to-end data engineering platform built with modern tools and best practices.

## What this project covers
- ETL/ELT pipeline with Apache Spark (Bronze → Silver → Gold)
- Data Mart with star schema (dbt + PostgreSQL)
- ML pipeline for customer churn prediction (scikit-learn)
- AI-powered data annotation using LLM APIs
- Data quality validation with Great Expectations
- Pipeline orchestration with Apache Airflow
- Containerisation with Docker and Docker Compose
- CI/CD with GitHub Actions
- Unit testing with pytest

## Tech stack
Python 3.10 · PySpark 4.1 · PostgreSQL · dbt · Airflow · Docker · GitHub Actions

## Getting started
```bash
git clone https://github.com/YOUR_USERNAME/smartflow-data-platform.git
cd smartflow-data-platform
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Project structure
See STRUCTURE.md for full folder explanation.
