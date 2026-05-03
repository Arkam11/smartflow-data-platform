"""
Pipeline monitoring report generator.

Reads the monitoring.pipeline_runs table and generates:
1. A terminal summary showing recent pipeline health
2. An HTML report file saved to logs/monitoring_report.html

Run this any time to check pipeline health:
    python src/monitoring/report.py
"""

import os
import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from src.monitoring.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


def get_engine():
    db_url = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
        f"/{os.getenv('DB_NAME')}"
    )
    return create_engine(db_url)


def get_recent_runs(engine, limit: int = 10) -> pd.DataFrame:
    """Get the most recent pipeline runs."""
    query = """
        SELECT
            run_id,
            stage,
            status,
            rows_processed,
            ROUND(duration_seconds::numeric, 1) as duration_s,
            extra_metrics,
            error_message,
            created_at
        FROM monitoring.pipeline_runs
        ORDER BY created_at DESC
        LIMIT :limit
    """
    return pd.read_sql(text(query), engine, params={"limit": limit})


def get_run_summary(engine) -> pd.DataFrame:
    """Get summary statistics per run_id."""
    query = """
        SELECT
            run_id,
            MIN(created_at)                          AS run_start,
            MAX(created_at)                          AS run_end,
            COUNT(*)                                 AS total_stages,
            SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) AS passed,
            SUM(CASE WHEN status='failed'  THEN 1 ELSE 0 END) AS failed,
            SUM(rows_processed)                      AS total_rows,
            ROUND(SUM(duration_seconds)::numeric, 1) AS total_duration_s
        FROM monitoring.pipeline_runs
        GROUP BY run_id
        ORDER BY run_start DESC
        LIMIT 5
    """
    return pd.read_sql(text(query), engine)


def get_data_freshness(engine) -> dict:
    """Check how fresh the data is in each layer."""
    checks = {}

    tables = [
        ("bronze", "raw_orders",   "rowid"),
        ("silver", "orders",       "order_date"),
        ("gold",   "fact_orders",  "order_date"),
        ("ml",     "churn_scores", "customer_key"),
    ]

    with engine.connect() as conn:
        for schema, table, _ in tables:
            try:
                result = conn.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{table}")
                )
                count = result.fetchone()[0]
                checks[f"{schema}.{table}"] = {
                    "row_count": count,
                    "status": "ok" if count > 0 else "empty"
                }
            except Exception as e:
                checks[f"{schema}.{table}"] = {
                    "row_count": 0,
                    "status": "missing"
                }

    return checks


def print_terminal_report(engine):
    """Print a formatted monitoring report to the terminal."""
    print("\n" + "=" * 65)
    print("  SMARTFLOW PIPELINE MONITORING REPORT")
    print(f"  Generated: {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 65)

    # Data layer health
    print("\n DATA LAYER STATUS")
    print("-" * 65)
    freshness = get_data_freshness(engine)
    for table, info in freshness.items():
        icon = "✅" if info["status"] == "ok" else "❌"
        print(f"  {icon}  {table:<30} {info['row_count']:>10,} rows")

    # Recent runs summary
    print("\n RECENT PIPELINE RUNS")
    print("-" * 65)
    try:
        summary = get_run_summary(engine)
        if len(summary) == 0:
            print("  No pipeline runs recorded yet")
        else:
            for _, row in summary.iterrows():
                status = "✅ PASS" if row["failed"] == 0 else "❌ FAIL"
                print(f"  {status}  Run {row['run_id'][:8]}...  "
                      f"{row['passed']}/{row['total_stages']} stages  "
                      f"{row['total_rows']:,} rows  "
                      f"{row['total_duration_s']}s")
    except Exception as e:
        print(f"  No run history yet: {e}")

    # Recent stage details
    print("\n LAST 10 STAGE EXECUTIONS")
    print("-" * 65)
    try:
        recent = get_recent_runs(engine, limit=10)
        if len(recent) == 0:
            print("  No stage history yet")
        else:
            for _, row in recent.iterrows():
                icon = "✅" if row["status"] == "success" else "❌"
                print(f"  {icon}  {row['stage']:<25} "
                      f"{str(row['rows_processed']):>8} rows  "
                      f"{row['duration_s']}s  "
                      f"{str(row['created_at'])[:16]}")
    except Exception as e:
        print(f"  No stage history yet: {e}")

    print("\n" + "=" * 65)


def generate_html_report(engine) -> str:
    """Generate an HTML monitoring report and save to logs/."""
    freshness = get_data_freshness(engine)

    try:
        summary = get_run_summary(engine)
        recent = get_recent_runs(engine, limit=20)
    except Exception:
        summary = pd.DataFrame()
        recent = pd.DataFrame()

    rows_html = ""
    for table, info in freshness.items():
        color = "#22c55e" if info["status"] == "ok" else "#ef4444"
        rows_html += f"""
        <tr>
            <td>{table}</td>
            <td style="color:{color};font-weight:500">{info['status'].upper()}</td>
            <td>{info['row_count']:,}</td>
        </tr>"""

    run_rows_html = ""
    if len(summary) > 0:
        for _, row in summary.iterrows():
            color = "#22c55e" if row["failed"] == 0 else "#ef4444"
            run_rows_html += f"""
            <tr>
                <td>{str(row['run_id'])[:8]}...</td>
                <td style="color:{color}">{int(row['passed'])}/{int(row['total_stages'])}</td>
                <td>{int(row['total_rows']):,}</td>
                <td>{row['total_duration_s']}s</td>
                <td>{str(row['run_start'])[:16]}</td>
            </tr>"""
    else:
        run_rows_html = "<tr><td colspan='5'>No runs recorded yet</td></tr>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>SmartFlow Monitoring Dashboard</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif;
          background: #0f172a; color: #e2e8f0; margin: 0; padding: 24px; }}
  h1 {{ color: #38bdf8; margin-bottom: 4px; }}
  .subtitle {{ color: #94a3b8; margin-bottom: 32px; font-size: 14px; }}
  .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }}
  .card {{ background: #1e293b; border-radius: 12px; padding: 20px; }}
  .card h2 {{ color: #94a3b8; font-size: 13px; text-transform: uppercase;
              letter-spacing: 1px; margin-bottom: 16px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
  th {{ text-align: left; color: #64748b; padding: 6px 0;
        border-bottom: 1px solid #334155; font-weight: 500; }}
  td {{ padding: 8px 0; border-bottom: 1px solid #1e293b; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 4px;
            font-size: 12px; font-weight: 600; }}
  .pass {{ background: #166534; color: #86efac; }}
  .fail {{ background: #7f1d1d; color: #fca5a5; }}
</style>
</head>
<body>
<h1>SmartFlow Monitoring Dashboard</h1>
<p class="subtitle">Generated {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>

<div class="grid">
  <div class="card">
    <h2>Data Layer Status</h2>
    <table>
      <tr><th>Table</th><th>Status</th><th>Rows</th></tr>
      {rows_html}
    </table>
  </div>

  <div class="card">
    <h2>Recent Pipeline Runs</h2>
    <table>
      <tr><th>Run ID</th><th>Stages</th><th>Rows</th><th>Duration</th><th>Started</th></tr>
      {run_rows_html}
    </table>
  </div>
</div>
</body>
</html>"""

    output_path = Path("logs/monitoring_report.html")
    output_path.parent.mkdir(exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    logger.info(f"HTML report saved to {output_path}")
    return str(output_path)


def run_report():
    engine = get_engine()
    print_terminal_report(engine)
    html_path = generate_html_report(engine)
    print(f"\n  HTML report saved to: {html_path}")


if __name__ == "__main__":
    run_report()