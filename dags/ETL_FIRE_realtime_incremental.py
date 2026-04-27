# dags/nasa_firms_incremental.py

from __future__ import annotations
import logging
import math
from datetime import datetime, timedelta
from io import StringIO

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────
# NASA FIRMS VIIRS NOAA-20 — last 24 h for the contiguous US + Alaska
FIRMS_BASE_URL    = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
# FIRMS area API expects a bounding box: "west,south,east,north"
AREA_OF_INTEREST  = "-170,18,-66,72"
SOURCE            = "VIIRS_NOAA20_NRT"             # MODIS_NRT | VIIRS_SNPP_NRT | VIIRS_NOAA20_NRT
DAY_RANGE         = 1                              # how many days back to fetch each run
SNOWFLAKE_CONN_ID = "snowflake_default"
TARGET_TABLE      = "USER_DB_CHEETAH.RAW.NASA_FIRMS_VIIRS"

# ── Tasks ────────────────────────────────────────────────────────────────────

def fetch_firms_data(**context):
    """Call NASA FIRMS CSV API for the execution date window."""
    map_key   = Variable.get("nasa_firms_map_key")
    exec_date = context["ds"]                        # only used for logging/file naming

    # FIRMS area endpoint does not accept an execution-date path segment.
    url = f"{FIRMS_BASE_URL}/{map_key}/{SOURCE}/{AREA_OF_INTEREST}/{DAY_RANGE}"
    logger.info("Fetching FIRMS data: %s", url)

    resp = requests.get(url, timeout=60)
    if not resp.ok:
        raise RuntimeError(f"FIRMS API error {resp.status_code}: {resp.text[:300]}")

    if not resp.text.strip():
        logger.warning("Empty response from FIRMS API for date %s", exec_date)
        context["ti"].xcom_push(key="row_count", value=0)
        return

    df = pd.read_csv(StringIO(resp.text))
    logger.info("Raw rows fetched: %d", len(df))

    # Normalise column names
    df.columns = [c.upper() for c in df.columns]

    # Add a record_id for deduplication (replicate Snowflake VIRTUAL column logic in Python)
    import hashlib
    def make_id(row):
        key = f"{row.get('LATITUDE','')}{row.get('LONGITUDE','')}{row.get('ACQ_DATE','')}{row.get('ACQ_TIME','')}"
        return hashlib.md5(key.encode()).hexdigest()

    df["RECORD_ID"] = df.apply(make_id, axis=1)

    tmp_path = f"/tmp/firms_{exec_date}.parquet"
    df.to_parquet(tmp_path, index=False)
    context["ti"].xcom_push(key="tmp_path",  value=tmp_path)
    context["ti"].xcom_push(key="row_count", value=len(df))


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two lat/lon points."""
    r = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def enrich_city_from_reference(df: pd.DataFrame, hook: SnowflakeHook) -> pd.DataFrame:
    """Assign nearest city centroid from CITY_GEO_REF using fire lat/lon."""
    city_ref = hook.get_pandas_df(
        "SELECT CITY, LATITUDE, LONGITUDE FROM USER_DB_CHEETAH.RAW.CITY_GEO_REF"
    )
    if city_ref.empty:
        df["CITY"] = None
        return df

    city_points = [
        (str(r["CITY"]), float(r["LATITUDE"]), float(r["LONGITUDE"]))
        for _, r in city_ref.iterrows()
    ]

    def nearest_city(lat: float, lon: float) -> str | None:
        if pd.isna(lat) or pd.isna(lon):
            return None
        best_city = None
        best_dist = float("inf")
        for city, c_lat, c_lon in city_points:
            dist = haversine_km(float(lat), float(lon), c_lat, c_lon)
            if dist < best_dist:
                best_dist = dist
                best_city = city
        return best_city

    df["CITY"] = df.apply(lambda r: nearest_city(r.get("LATITUDE"), r.get("LONGITUDE")), axis=1)
    return df


def merge_into_snowflake(**context):
    """MERGE (upsert) new FIRMS records into Snowflake — idempotent."""
    row_count = context["ti"].xcom_pull(task_ids="fetch_firms_data", key="row_count")
    if not row_count:
        logger.info("No rows to load, skipping merge.")
        return

    tmp_path = context["ti"].xcom_pull(task_ids="fetch_firms_data", key="tmp_path")
    if not tmp_path:
        logger.info("No staging file produced, skipping merge.")
        return
    df = pd.read_parquet(tmp_path)

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    df = enrich_city_from_reference(df, hook)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # 1. Load into a transient staging table
    stage_table = "USER_DB_CHEETAH.RAW.FIRMS_STAGE_TMP"

    from snowflake.connector.pandas_tools import write_pandas
    write_pandas(
        conn=conn,
        df=df,
        table_name="FIRMS_STAGE_TMP",
        database="USER_DB_CHEETAH",
        schema="RAW",
        auto_create_table=True,
        overwrite=True,
        chunk_size=5_000,
    )
    logger.info("Staged %d rows into %s", len(df), stage_table)

    # 2. MERGE into target (dedup on record_id)
    merge_sql = f"""
        MERGE INTO {TARGET_TABLE} AS tgt
        USING {stage_table}       AS src
          ON  tgt.RECORD_ID = src.RECORD_ID
        WHEN NOT MATCHED THEN INSERT (
            LATITUDE, LONGITUDE, BRIGHT_TI4, SCAN, TRACK,
            ACQ_DATE, ACQ_TIME, SATELLITE, INSTRUMENT,
            CONFIDENCE, VERSION, BRIGHT_TI5, FRP, DAYNIGHT, CITY
        ) VALUES (
            src.LATITUDE, src.LONGITUDE, src.BRIGHT_TI4, src.SCAN, src.TRACK,
            src.ACQ_DATE, src.ACQ_TIME, src.SATELLITE, src.INSTRUMENT,
            src.CONFIDENCE, src.VERSION, src.BRIGHT_TI5, src.FRP, src.DAYNIGHT, src.CITY
        );
    """
    cursor.execute(merge_sql)
    logger.info("MERGE complete: %s", cursor.fetchone())

    # 3. Drop staging table
    cursor.execute(f"DROP TABLE IF EXISTS {stage_table}")
    cursor.close()


def check_data_freshness(**context):
    """Assert that the target table has records for today's execution date."""
    exec_date = context["ds"]
    hook      = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    result    = hook.get_first(
        f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE ACQ_DATE = '{exec_date}'"
    )
    count = result[0]
    logger.info("Rows for %s: %d", exec_date, count)
    # Warn but don't fail — some days may have zero fires reported
    if count == 0:
        logger.warning("No fire records for %s — API may have returned empty.", exec_date)


# ── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner":            "data-eng",
    "retries":          3,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": True,
}

with DAG(
    dag_id="nasa_firms_incremental",
    description="Daily incremental load of NASA FIRMS active fire data into Snowflake",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",      # runs once per day at midnight UTC
    catchup=True,                    # backfill from start_date if needed
    max_active_runs=3,               # allow parallel backfill runs
    tags=["fire", "incremental", "nasa", "snowflake"],
) as dag:

    fetch = PythonOperator(
        task_id="fetch_firms_data",
        python_callable=fetch_firms_data,
    )

    merge = PythonOperator(
        task_id="merge_into_snowflake",
        python_callable=merge_into_snowflake,
    )

    check = PythonOperator(
        task_id="check_data_freshness",
        python_callable=check_data_freshness,
    )

    fetch >> merge >> check