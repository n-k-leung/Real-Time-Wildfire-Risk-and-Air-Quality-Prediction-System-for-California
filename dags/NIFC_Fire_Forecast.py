import logging
import math
from datetime import datetime, timedelta, date

import numpy as np
import pandas as pd
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake_default"

SOURCE_DB = "user_db_coyote"
SOURCE_SCHEMA = "raw"
SOURCE_TABLE = "nifc_fire_proj"

TARGET_DB = "USER_DB_GROUNDHOG"
TARGET_SCHEMA = "analytics"
TARGET_TABLE = "nifc_fire_forecast_proj"
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


def safe_mode(series: pd.Series):
    mode = series.dropna().mode()
    return mode.iloc[0] if not mode.empty else None


def build_numeric_forecast(
    city_df: pd.DataFrame,
    value_col: str,
    run_date: date,
    future_dates: list[date],
) -> dict[date, float]:
    """
    Forecast a numeric signal with seasonal profile + recent trend:
    - Seasonal baseline from day-of-year averages over the recent 2 years
    - Linear trend from last 180 days
    """
    work = city_df[["date", value_col]].copy()
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce").fillna(0.0)

    if work.empty:
        return {d: 0.0 for d in future_dates}

    work["doy"] = pd.to_datetime(work["date"]).dt.dayofyear
    work["series_day_idx"] = (pd.to_datetime(work["date"]) - pd.Timestamp(run_date)).dt.days

    seasonal_lookup = (
        work.groupby("doy")[value_col]
        .mean()
        .to_dict()
    )
    default_level = float(work[value_col].mean()) if len(work) else 0.0

    trend_df = work.sort_values("date").tail(180)
    x = trend_df["series_day_idx"].to_numpy(dtype=float)
    y = trend_df[value_col].to_numpy(dtype=float)

    if len(trend_df) >= 2 and not np.allclose(x, x[0]):
        slope, intercept = np.polyfit(x, y, 1)
    else:
        slope, intercept = 0.0, default_level

    trend_weight = 0.35
    seasonal_weight = 0.65

    out: dict[date, float] = {}
    for d in future_dates:
        doy = d.timetuple().tm_yday
        seasonal_part = float(seasonal_lookup.get(doy, default_level))
        day_idx = float((d - run_date).days)
        trend_part = float(slope * day_idx + intercept)
        pred = seasonal_weight * seasonal_part + trend_weight * trend_part
        out[d] = round(max(pred, 0.0), 3)

    return out


@task
def extract_history() -> list[dict]:
    conn = get_snowflake_connection()
    cur = conn.cursor()

    query = f"""
        SELECT
            date,
            city,
            incident_count,
            total_acres,
            avg_acres,
            max_acres,
            most_common_incident,
            most_common_agency,
            most_common_source
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

    run_date = datetime.strptime(ds, "%Y-%m-%d").date()
    horizon_days = 90
    forecast_end = run_date + timedelta(days=horizon_days)

    df = pd.DataFrame(history)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["incident_count"] = pd.to_numeric(df["incident_count"], errors="coerce").fillna(0)
    df["total_acres"] = pd.to_numeric(df["total_acres"], errors="coerce").fillna(0)
    df["avg_acres"] = pd.to_numeric(df["avg_acres"], errors="coerce").fillna(0)
    df["max_acres"] = pd.to_numeric(df["max_acres"], errors="coerce").fillna(0)
    df = df.dropna(subset=["city", "date"])

    # Keep only the most recent 24 months as signal for short-horizon forecasting.
    recent_cutoff = run_date - timedelta(days=730)
    df = df[df["date"] >= recent_cutoff]

    if df.empty:
        logger.warning("Recent historical data empty after cutoff; forecast output is empty.")
        return []

    forecast_dates = list(pd.date_range(start=run_date + timedelta(days=1), end=forecast_end, freq="D").date)
    records = []

    for city in sorted(df["city"].dropna().unique()):
        city_df = df[df["city"] == city].copy()

        incident_forecast = build_numeric_forecast(city_df, "incident_count", run_date, forecast_dates)
        total_acres_forecast = build_numeric_forecast(city_df, "total_acres", run_date, forecast_dates)
        avg_acres_forecast = build_numeric_forecast(city_df, "avg_acres", run_date, forecast_dates)
        max_acres_forecast = build_numeric_forecast(city_df, "max_acres", run_date, forecast_dates)

        # Forecast categorical columns via seasonal monthly mode with global fallback.
        city_df["month"] = pd.to_datetime(city_df["date"]).dt.month
        month_modes = (
            city_df.groupby("month")
            .agg(
                most_common_incident=("most_common_incident", safe_mode),
                most_common_agency=("most_common_agency", safe_mode),
                most_common_source=("most_common_source", safe_mode),
            )
            .reset_index()
        )
        month_mode_lookup = {
            int(r["month"]): {
                "most_common_incident": r["most_common_incident"],
                "most_common_agency": r["most_common_agency"],
                "most_common_source": r["most_common_source"],
            }
            for _, r in month_modes.iterrows()
        }
        overall_incident = safe_mode(city_df["most_common_incident"])
        overall_agency = safe_mode(city_df["most_common_agency"])
        overall_source = safe_mode(city_df["most_common_source"])

        for idx, f_date in enumerate(forecast_dates, start=1):
            cat = month_mode_lookup.get(
                f_date.month,
                {
                    "most_common_incident": overall_incident,
                    "most_common_agency": overall_agency,
                    "most_common_source": overall_source,
                },
            )
            records.append(
                {
                    "date": f_date,
                    "city": city,
                    "incident_count": incident_forecast[f_date],
                    "total_acres": total_acres_forecast[f_date],
                    "avg_acres": avg_acres_forecast[f_date],
                    "max_acres": max_acres_forecast[f_date],
                    "most_common_incident": cat["most_common_incident"],
                    "most_common_agency": cat["most_common_agency"],
                    "most_common_source": cat["most_common_source"] or "forecast_seasonal_trend",
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
    start_date=pendulum.now("America/Los_Angeles"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ETL", "fire", "forecast", "analytics"],
    default_args=DEFAULT_ARGS,
) as dag:
    historical_rows = extract_history()
    forecast_rows = forecast(historical_rows)
    load(forecast_rows)
