import logging
import math
from datetime import datetime, timedelta, date

import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake_default"

SOURCE_DB = "USER_DB_GROUNDHOG"
SOURCE_SCHEMA = "raw"
SOURCE_TABLE = "nifc_fire_proj"

TARGET_DB = "USER_DB_GROUNDHOG"
TARGET_SCHEMA = "analytics"
TARGET_TABLE = "nifc_fire_forecast"
COMBINED_VIEW = "nifc_fire_actual_and_forecast"

DEFAULT_ARGS = {
    "owner": "SaminaMaraj",
    "email": ["kazisaminamaraj.mumu@sjsu.edu"],
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


def get_snowflake_connection():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn()


def clean_value(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if hasattr(v, "item"):
        return v.item()
    return v


@task
def extract_history() -> list[dict]:
    conn = get_snowflake_connection()
    cur = conn.cursor()

    query = f"""
        SELECT
            date,
            city,
            incident_count,
            total_acres
        FROM {SOURCE_DB}.{SOURCE_SCHEMA}.{SOURCE_TABLE}
        WHERE date >= DATEADD(year, -5, CURRENT_DATE())
    """

    try:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [c[0].lower() for c in cur.description]
        history = [dict(zip(cols, row)) for row in rows]
        logger.info("Extracted %d rows from %s.%s.%s", len(history), SOURCE_DB, SOURCE_SCHEMA, SOURCE_TABLE)
        return history
    finally:
        cur.close()
        conn.close()


@task
def forecast(history: list[dict], ds: str) -> list[dict]:
    if not history:
        logger.warning("No historical rows found; forecast output is empty.")
        return []

    run_date = datetime.utcnow().date()
    horizon_days = 90
    forecast_end = run_date + timedelta(days=horizon_days)

    df = pd.DataFrame(history)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["incident_count"] = pd.to_numeric(df["incident_count"], errors="coerce").fillna(0)
    df["total_acres"] = pd.to_numeric(df["total_acres"], errors="coerce").fillna(0)
    df = df.dropna(subset=["city", "date"])

    # Keep only the most recent 24 months as signal for short-horizon forecasting.
    recent_cutoff = run_date - timedelta(days=730)
    df = df[df["date"] >= recent_cutoff]

    if df.empty:
        logger.warning("Recent historical data empty after cutoff; forecast output is empty.")
        return []

    city_stats = (
        df.groupby("city")
        .agg(
            incident_avg=("incident_count", "mean"),
            acres_avg=("total_acres", "mean"),
        )
        .reset_index()
    )

    forecast_dates = pd.date_range(start=run_date + timedelta(days=1), end=forecast_end, freq="D").date
    records = []

    for _, row in city_stats.iterrows():
        city = row["city"]
        incident_avg = float(row["incident_avg"]) if pd.notna(row["incident_avg"]) else 0.0
        acres_avg = float(row["acres_avg"]) if pd.notna(row["acres_avg"]) else 0.0

        for idx, f_date in enumerate(forecast_dates, start=1):
            records.append(
                {
                    "date": f_date,
                    "city": city,
                    "incident_count": round(max(incident_avg, 0), 3),
                    "total_acres": round(max(acres_avg, 0), 3),
                    "avg_acres": None,
                    "max_acres": None,
                    "most_common_incident": None,
                    "most_common_agency": None,
                    "most_common_source": "forecast_mean",
                    "is_forecast": True,
                    "horizon_day": idx,
                    "forecast_generated_at": run_date,
                }
            )

    logger.info("Built %d forecast rows (%d-day horizon).", len(records), horizon_days)
    return records


@task
def load(records: list[dict]) -> None:
    if not records:
        logger.info("No forecast rows to load.")
        return

    conn = get_snowflake_connection()
    cur = conn.cursor()

    full_table = f"{TARGET_DB}.{TARGET_SCHEMA}.{TARGET_TABLE}"

    try:
        cur.execute("BEGIN")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {TARGET_DB}.{TARGET_SCHEMA}")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                date DATE NOT NULL,
                city VARCHAR NOT NULL,
                incident_count FLOAT,
                total_acres FLOAT,
                avg_acres FLOAT,
                max_acres FLOAT,
                most_common_incident VARCHAR,
                most_common_agency VARCHAR,
                most_common_source VARCHAR,
                is_forecast BOOLEAN,
                horizon_day INTEGER,
                forecast_generated_at DATE,
                PRIMARY KEY (date, city)
            )
            """
        )

        # Keep the latest forecast version only.
        cur.execute(f"TRUNCATE TABLE {full_table}")

        insert_sql = f"""
            INSERT INTO {full_table}
            (
                date, city, incident_count, total_acres, avg_acres, max_acres,
                most_common_incident, most_common_agency, most_common_source,
                is_forecast, horizon_day, forecast_generated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        values = [
            (
                clean_value(r["date"]),
                clean_value(r["city"]),
                clean_value(r["incident_count"]),
                clean_value(r["total_acres"]),
                clean_value(r["avg_acres"]),
                clean_value(r["max_acres"]),
                clean_value(r["most_common_incident"]),
                clean_value(r["most_common_agency"]),
                clean_value(r["most_common_source"]),
                clean_value(r["is_forecast"]),
                clean_value(r["horizon_day"]),
                clean_value(r["forecast_generated_at"]),
            )
            for r in records
        ]

        cur.executemany(insert_sql, values)

        # Unified view for downstream joins/queries with historical wildfire metrics.
        cur.execute(
            f"""
            CREATE OR REPLACE VIEW {TARGET_DB}.{TARGET_SCHEMA}.{COMBINED_VIEW} AS
            SELECT
                date,
                city,
                incident_count,
                total_acres,
                avg_acres,
                max_acres,
                most_common_incident,
                most_common_agency,
                most_common_source,
                FALSE AS is_forecast,
                0 AS horizon_day,
                CAST(NULL AS DATE) AS forecast_generated_at
            FROM {SOURCE_DB}.{SOURCE_SCHEMA}.{SOURCE_TABLE}

            UNION ALL

            SELECT
                date,
                city,
                incident_count,
                total_acres,
                avg_acres,
                max_acres,
                most_common_incident,
                most_common_agency,
                most_common_source,
                is_forecast,
                horizon_day,
                forecast_generated_at
            FROM {full_table}
            """
        )

        cur.execute("COMMIT")
        logger.info("Loaded %d rows into %s", len(values), full_table)

    except Exception:
        cur.execute("ROLLBACK")
        raise

    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="nifc_fire_forecast",
    description="Builds daily wildfire forecast table in analytics schema.",
    start_date=datetime(2026, 4, 30),
    schedule="45 3 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ETL", "fire", "forecast", "analytics"],
    default_args=DEFAULT_ARGS,
) as dag:
    historical_rows = extract_history()
    forecast_rows = forecast(historical_rows)
    load(forecast_rows)
