import logging
import math
from datetime import datetime, timedelta

import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID = "snowflake_con"

SOURCE_DB = "USER_DB_GROUNDHOG"
RAW_SCHEMA = "RAW"
ANALYTICS_SCHEMA = "ANALYTICS"

AQI_TABLE = "AQI_DATA_PROJ"
WEATHER_TABLE = "WEATHER_DATA_PROJ"
FIRE_FORECAST_TABLE = "NIFC_FIRE_FORECAST"

TARGET_TABLE = "AQI_FORECAST"

DEFAULT_ARGS = {
    "owner": "SaminaMaraj",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
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
def extract_data():
    conn = get_snowflake_connection()
    cur = conn.cursor()

    try:
        # Historical AQI + weather
        cur.execute(f"""
            SELECT
                a.city,
                a.aqi,
                w.wind_speed_10m_max,
                w.precipitation_sum,
                w.temp_mean
            FROM {SOURCE_DB}.{RAW_SCHEMA}.{AQI_TABLE} a
            LEFT JOIN {SOURCE_DB}.{RAW_SCHEMA}.{WEATHER_TABLE} w
                ON a.date = w.date AND a.city = w.city
            WHERE a.date >= DATEADD(year, -5, CURRENT_DATE())
              AND a.aqi IS NOT NULL
        """)
        hist_rows = cur.fetchall()
        hist_cols = [c[0].lower() for c in cur.description]
        history = [dict(zip(hist_cols, row)) for row in hist_rows]

        # Forecast wildfire
        cur.execute(f"""
            SELECT
                date,
                city,
                incident_count,
                total_acres
            FROM {SOURCE_DB}.{ANALYTICS_SCHEMA}.{FIRE_FORECAST_TABLE}
            WHERE is_forecast = TRUE
        """)
        fire_rows = cur.fetchall()
        fire_cols = [c[0].lower() for c in cur.description]
        fire = [dict(zip(fire_cols, row)) for row in fire_rows]

        return {"history": history, "fire": fire}

    finally:
        cur.close()
        conn.close()


@task
def forecast_aqi(data):
    history = data["history"]
    fire = data["fire"]

    if not history or not fire:
        logger.warning("Missing data")
        return []

    run_date = datetime.utcnow().date()

    hist_df = pd.DataFrame(history)
    fire_df = pd.DataFrame(fire)

    hist_df["aqi"] = pd.to_numeric(hist_df["aqi"], errors="coerce").fillna(0)
    hist_df["wind_speed_10m_max"] = pd.to_numeric(hist_df["wind_speed_10m_max"], errors="coerce")
    hist_df["precipitation_sum"] = pd.to_numeric(hist_df["precipitation_sum"], errors="coerce")
    hist_df["temp_mean"] = pd.to_numeric(hist_df["temp_mean"], errors="coerce")

    hist_df.fillna(hist_df.mean(numeric_only=True), inplace=True)

    # using average aqi as a basis of what predicited aqi should be around
    base_aqi = (
        hist_df.groupby("city")
        .agg(base_aqi=("aqi", "mean"))
        .reset_index()
    )

    # normalize weather on scale 0 to 1 so features can be used
    def normalize(col):
        return (col - col.min()) / (col.max() - col.min())

    hist_df["wind_norm"] = normalize(hist_df["wind_speed_10m_max"])
    hist_df["temp_norm"] = normalize(hist_df["temp_mean"])
    hist_df["precip_norm"] = normalize(hist_df["precipitation_sum"])

    # multiplier that adjusts wildfire impact based on weather findings so this affects the aqi prediction
    weather_factor = (
        hist_df.groupby("city")
        .apply(lambda x: (
            0.5 * x["wind_norm"].mean() +  # wind spreads smoke
            0.3 * x["temp_norm"].mean() +  # heat worsens AQI
            0.2 * (1 - x["precip_norm"].mean())  # rain reduces AQI
        ))
        .reset_index(name="weather_factor")
    )

    base_aqi = base_aqi.merge(weather_factor, on="city", how="left")

    # taking into account the wildfire forecast data
    df = fire_df.merge(base_aqi, on="city", how="left")

    records = []
    # expected pollution contribution from fires
    for _, row in df.iterrows():
        fire_impact = (
            2.0 * row["incident_count"] +
            0.02 * row["total_acres"]
        )

        weather_adj = row["weather_factor"] if pd.notna(row["weather_factor"]) else 0

        predicted_aqi = row["base_aqi"] + fire_impact * (1 + weather_adj)

        records.append(
            {
                "date": row["date"],
                "city": row["city"],
                "aqi": round(predicted_aqi, 2),
                "is_forecast": True,
                "forecast_generated_at": run_date,
            }
        )

    logger.info("Generated %d AQI forecast rows", len(records))
    return records


@task
def load(con, target_table, records):
    try:
        cur = con.cursor()

        cur.execute("BEGIN;")

        cur.execute(f"""
            CREATE OR REPLACE TABLE {target_table} (
                date DATE,
                city VARCHAR,
                aqi FLOAT,
                is_forecast BOOLEAN,
                forecast_generated_at DATE,
                PRIMARY KEY (date, city)
            );
        """)

        cur.execute(f"DELETE FROM {target_table};")

        insert_sql = f"""
            INSERT INTO {target_table} (
                date, city, aqi, is_forecast, forecast_generated_at
            ) VALUES (
                %s, %s, %s, %s, %s
            );
        """

        data = [
            (
                r["date"], r["city"], r["aqi"],
                r["is_forecast"], r["forecast_generated_at"]
            )
            for r in records
        ]

        cur.executemany(insert_sql, data)
        cur.execute("COMMIT;")

        print(f"Loaded {len(records)} records into {target_table}")

    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error loading records into {target_table}: {e}")
        raise e

    finally:
        cur.close()

with DAG(
    dag_id="aqi_forecast",
    description="AQI forecast using wildfire forecast, historical weather, and historical aqi",
    start_date=datetime(2026, 4, 30),
    schedule="05 4 * * *", # to run after wildfire forecast
    catchup=False,
    tags=["aqi", "forecast"],
    default_args=DEFAULT_ARGS,
) as dag:

    data = extract_data()
    forecast = forecast_aqi(data)
    load(get_snowflake_connection(), f"{SOURCE_DB}.{ANALYTICS_SCHEMA}.{TARGET_TABLE}", forecast)