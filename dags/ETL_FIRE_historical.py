# dags/fire_data_transform_with_city.py

from __future__ import annotations
import logging
import math
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)

SNOWFLAKE_CONN_ID  = "snowflake_default"
DATABASE           = "USER_DB_CHEETAH"
RAW_SCHEMA         = "RAW"
WEATHER_AQI_DB     = "USER_DB_COYOTE"
MAX_DISTANCE_KM    = 100   # only assign fire to a city if within 100 km
                            # tune this — smaller = more precise, larger = more coverage


# ── Haversine distance ────────────────────────────────────────────────────────

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two lat/lon points."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def assign_nearest_city(
    fire_lat: float,
    fire_lon: float,
    city_ref: pd.DataFrame,
    max_dist_km: float = MAX_DISTANCE_KM,
) -> tuple[str | None, float | None]:
    """
    Find the nearest city centroid to a fire location.
    Returns (city_name, distance_km) or (None, None) if beyond max_dist_km.
    """
    if city_ref.empty:
        return None, None

    city_ref = city_ref.copy()
    city_ref["dist_km"] = city_ref.apply(
        lambda r: haversine_km(fire_lat, fire_lon, r["LATITUDE"], r["LONGITUDE"]),
        axis=1,
    )
    nearest = city_ref.loc[city_ref["dist_km"].idxmin()]
    dist    = nearest["dist_km"]

    if dist <= max_dist_km:
        return nearest["CITY"], round(dist, 2)
    return None, None


# ── Task 1: Load city reference ───────────────────────────────────────────────

def load_city_reference(**context):
    """Pull city centroids from Snowflake into memory."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    df   = hook.get_pandas_df(
        f"SELECT CITY, LATITUDE, LONGITUDE FROM {DATABASE}.{RAW_SCHEMA}.CITY_GEO_REF"
    )
    logger.info("Loaded %d city reference records", len(df))
    tmp = "/tmp/city_ref.parquet"
    df.to_parquet(tmp, index=False)
    context["ti"].xcom_push(key="city_ref_path", value=tmp)


# ── Task 2a: Transform CA Fire Perimeters ────────────────────────────────────

def transform_ca_perimeters(**context):
    """
    Pull raw CA fire perimeter data, extract centroid from geometry,
    assign nearest city, write transformed rows to staging.
    """
    import json
    from shapely.geometry import shape

    city_ref_path = context["ti"].xcom_pull(
        task_ids="load_city_reference",
        key="city_ref_path",
    )
    city_ref      = pd.read_parquet(city_ref_path)

    hook   = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    raw_df = hook.get_pandas_df(
        f"""
        SELECT
            OBJECTID, FIRE_NAME, GIS_ACRES,
            ALARM_DATE::DATE  AS ALARM_DATE,
            CONT_DATE::DATE   AS CONT_DATE,
            CAUSE, AGENCY,
            GEOM_WKT          -- stored as raw GeoJSON string
        FROM {DATABASE}.{RAW_SCHEMA}.CA_FIRE_PERIMETERS
        WHERE ALARM_DATE IS NOT NULL
        """
    )
    logger.info("Raw CA perimeter rows: %d", len(raw_df))

    rows = []
    for _, r in raw_df.iterrows():
        # ── Extract centroid from GeoJSON geometry ──
        try:
            geom_dict = json.loads(r["GEOM_WKT"]) if r["GEOM_WKT"] else None
            if geom_dict:
                geom      = shape(geom_dict)
                centroid  = geom.centroid
                fire_lat, fire_lon = centroid.y, centroid.x
            else:
                continue   # skip rows without geometry
        except Exception as e:
            logger.warning("Geometry parse failed for OBJECTID=%s: %s", r["OBJECTID"], e)
            continue

        # ── Assign nearest city ──
        city, dist_km = assign_nearest_city(fire_lat, fire_lon, city_ref)
        if city is None:
            logger.debug("No city within %d km for fire at (%s, %s)", MAX_DISTANCE_KM, fire_lat, fire_lon)
            continue

        # ── One row per day the fire was active ──
        alarm = pd.to_datetime(r["ALARM_DATE"])
        cont  = pd.to_datetime(r["CONT_DATE"]) if pd.notna(r["CONT_DATE"]) else alarm
        duration = max((cont - alarm).days, 0)

        for day_offset in range(duration + 1):
            active_date = (alarm + timedelta(days=day_offset)).date()
            rows.append({
                "DATE":          active_date,
                "CITY":          city,
                "LATITUDE":      round(fire_lat, 6),
                "LONGITUDE":     round(fire_lon, 6),
                "FIRE_NAME":     r["FIRE_NAME"],
                "GIS_ACRES":     r["GIS_ACRES"],
                "ALARM_DATE":    r["ALARM_DATE"],
                "CONT_DATE":     r["CONT_DATE"],
                "DURATION_DAYS": duration,
                "CAUSE":         r["CAUSE"],
                "AGENCY":        r["AGENCY"],
                "BRIGHT_TI4":    None,
                "FRP":           None,
                "CONFIDENCE":    None,
                "DAYNIGHT":      None,
                "SATELLITE":     None,
                "DATA_SOURCE":   "CA_PERIMETER",
                "DISTANCE_KM":   dist_km,
            })

    result_df = pd.DataFrame(rows)
    logger.info("Transformed CA perimeter rows (with city, daily): %d", len(result_df))
    tmp = "/tmp/ca_perimeters_transformed.parquet"
    result_df.to_parquet(tmp, index=False)
    context["ti"].xcom_push(key="ca_transformed_path", value=tmp)


# ── Task 2b: Transform NASA FIRMS ─────────────────────────────────────────────

def transform_nasa_firms(**context):
    """
    Pull raw FIRMS data, assign nearest city to each fire point,
    aggregate to daily city-level metrics.
    """
    city_ref_path = context["ti"].xcom_pull(
        task_ids="load_city_reference",
        key="city_ref_path",
    )
    city_ref      = pd.read_parquet(city_ref_path)

    hook   = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    raw_df = hook.get_pandas_df(
        f"""
        SELECT
            LATITUDE, LONGITUDE,
            ACQ_DATE::DATE AS ACQ_DATE,
            ACQ_TIME, SATELLITE,
            BRIGHT_TI4, FRP, CONFIDENCE, DAYNIGHT, CITY
        FROM {DATABASE}.{RAW_SCHEMA}.NASA_FIRMS_VIIRS
        WHERE ACQ_DATE IS NOT NULL
        """
    )
    logger.info("Raw FIRMS rows: %d", len(raw_df))

    rows = []
    for _, r in raw_df.iterrows():
        source_city = r.get("CITY")
        if pd.notna(source_city) and str(source_city).strip():
            city = str(source_city).strip()
            dist_km = 0.0
        else:
            city, dist_km = assign_nearest_city(r["LATITUDE"], r["LONGITUDE"], city_ref)
            if city is None:
                continue
        rows.append({
            "DATE":          r["ACQ_DATE"],
            "CITY":          city,
            "LATITUDE":      r["LATITUDE"],
            "LONGITUDE":     r["LONGITUDE"],
            "FIRE_NAME":     None,
            "GIS_ACRES":     None,
            "ALARM_DATE":    None,
            "CONT_DATE":     None,
            "DURATION_DAYS": None,
            "CAUSE":         None,
            "AGENCY":        None,
            "BRIGHT_TI4":    r["BRIGHT_TI4"],
            "FRP":           r["FRP"],
            "CONFIDENCE":    r["CONFIDENCE"],
            "DAYNIGHT":      r["DAYNIGHT"],
            "SATELLITE":     r["SATELLITE"],
            "DATA_SOURCE":   "NASA_FIRMS",
            "DISTANCE_KM":   dist_km,
        })

    result_df = pd.DataFrame(rows)
    logger.info("Transformed FIRMS rows (with city): %d", len(result_df))
    tmp = "/tmp/firms_transformed.parquet"
    result_df.to_parquet(tmp, index=False)
    context["ti"].xcom_push(key="firms_transformed_path", value=tmp)


# ── Task 3: Load both into Snowflake ─────────────────────────────────────────

def load_fire_data_to_snowflake(**context):
    """Merge both transformed datasets into FIRE_DATA_PROJ."""
    ca_path = context["ti"].xcom_pull(
        task_ids="transform_ca_perimeters",
        key="ca_transformed_path",
    )
    firms_path = context["ti"].xcom_pull(
        task_ids="transform_nasa_firms",
        key="firms_transformed_path",
    )

    ca_df    = pd.read_parquet(ca_path)    if ca_path    else pd.DataFrame()
    firms_df = pd.read_parquet(firms_path) if firms_path else pd.DataFrame()
    combined = pd.concat([ca_df, firms_df], ignore_index=True)
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()

    if combined.empty:
        # write_pandas cannot load an empty DataFrame; keep table consistent and continue.
        hook.run(f"TRUNCATE TABLE IF EXISTS {DATABASE}.{RAW_SCHEMA}.FIRE_DATA_PROJ")
        logger.info("No transformed fire rows; truncated FIRE_DATA_PROJ and skipped load.")
        return

    combined.columns = [c.upper() for c in combined.columns]
    for col in ["DATE", "ALARM_DATE", "CONT_DATE"]:
        if col in combined.columns:
            combined[col] = pd.to_datetime(combined[col], errors="coerce").dt.date

    expected_cols = [
        "DATE", "CITY", "LATITUDE", "LONGITUDE", "FIRE_NAME", "GIS_ACRES",
        "ALARM_DATE", "CONT_DATE", "DURATION_DAYS", "CAUSE", "AGENCY",
        "BRIGHT_TI4", "FRP", "CONFIDENCE", "DAYNIGHT", "SATELLITE",
        "DATA_SOURCE", "DISTANCE_KM",
    ]
    for col in expected_cols:
        if col not in combined.columns:
            combined[col] = None
    combined = combined[expected_cols]
    combined = combined.astype(object).where(pd.notna(combined), None)

    logger.info("Total rows to load: %d", len(combined))
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {DATABASE}.{RAW_SCHEMA}.FIRE_DATA_PROJ")
        insert_sql = f"""
        INSERT INTO {DATABASE}.{RAW_SCHEMA}.FIRE_DATA_PROJ (
            DATE, CITY, LATITUDE, LONGITUDE, FIRE_NAME, GIS_ACRES,
            ALARM_DATE, CONT_DATE, DURATION_DAYS, CAUSE, AGENCY,
            BRIGHT_TI4, FRP, CONFIDENCE, DAYNIGHT, SATELLITE,
            DATA_SOURCE, DISTANCE_KM
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        rows = [tuple(r) for r in combined.itertuples(index=False, name=None)]
        cursor.executemany(insert_sql, rows)
        conn.commit()
        logger.info("Loaded %d rows into FIRE_DATA_PROJ", len(rows))
    finally:
        cursor.close()
        conn.close()


# ── Task 4: Build final ML-ready joined table ─────────────────────────────────

# def build_ml_feature_table(**context):
#     """
#     Join FIRE_DATA_PROJ + WEATHER_DATA_PROJ + AQI_PROJ on (DATE, CITY).
#     Output: one row per (date, city) with weather + fire + AQI features.
#     """
#     hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

#     create_sql = f"""
#     CREATE OR REPLACE TABLE {DATABASE}.{RAW_SCHEMA}.FIRE_AQI_WEATHER_FEATURES AS

#     WITH fire_daily AS (
#         -- Aggregate fire metrics to one row per (date, city)
#         SELECT
#             DATE,
#             CITY,
#             COUNT(*)                        AS FIRE_COUNT,
#             SUM(GIS_ACRES)                  AS TOTAL_ACRES_BURNED,
#             AVG(FRP)                        AS AVG_FRP,           -- fire radiative power
#             MAX(FRP)                        AS MAX_FRP,
#             AVG(BRIGHT_TI4)                 AS AVG_BRIGHTNESS,
#             MIN(DISTANCE_KM)                AS NEAREST_FIRE_KM,
#             MAX(CASE WHEN DATA_SOURCE = 'CA_PERIMETER' THEN 1 ELSE 0 END) AS HAS_PERIMETER,
#             MAX(CASE WHEN DATA_SOURCE = 'NASA_FIRMS'   THEN 1 ELSE 0 END) AS HAS_FIRMS_POINT
#         FROM {DATABASE}.{RAW_SCHEMA}.FIRE_DATA_PROJ
#         GROUP BY DATE, CITY
#     ),

#     weather AS (
#         SELECT
#             DATE,
#             CITY,
#             TEMP_MAX, TEMP_MEAN, TEMP_MIN,
#             APPARENT_TEMP_MAX,
#             PRECIPITATION_SUM,
#             RAIN_SUM,
#             PRECIPITATION_HOURS,
#             PRECIPITATION_PROBABILITY_MAX,
#             WEATHER_CODE,
#             WIND_SPEED_10M_MAX,
#             UV_INDEX_MAX,
#             UV_INDEX_CLEAR_SKY_MAX
#         FROM {WEATHER_AQI_DB}.{RAW_SCHEMA}.WEATHER_DATA_PROJ
#     ),

#     aqi AS (
#         -- Pivot AQI parameters into columns (PM2.5 and PM10 are most fire-relevant)
#         SELECT
#             DATE,
#             CITY,
#             MAX(CASE WHEN PARAMETER = 'pm25' THEN AQI END) AS AQI_PM25,
#             MAX(CASE WHEN PARAMETER = 'pm10' THEN AQI END) AS AQI_PM10,
#             MAX(CASE WHEN PARAMETER = 'o3'   THEN AQI END) AS AQI_O3,
#             MAX(CASE WHEN PARAMETER = 'no2'  THEN AQI END) AS AQI_NO2,
#             MAX(AQI)                                        AS AQI_MAX   -- target variable
#         FROM {WEATHER_AQI_DB}.{RAW_SCHEMA}.AQI_PROJ
#         GROUP BY DATE, CITY
#     )

#     SELECT
#         w.DATE,
#         w.CITY,

#         -- ── Weather features ──
#         w.TEMP_MAX,
#         w.TEMP_MEAN,
#         w.TEMP_MIN,
#         w.APPARENT_TEMP_MAX,
#         w.PRECIPITATION_SUM,
#         w.RAIN_SUM,
#         w.PRECIPITATION_HOURS,
#         w.PRECIPITATION_PROBABILITY_MAX,
#         w.WEATHER_CODE,
#         w.WIND_SPEED_10M_MAX,
#         w.UV_INDEX_MAX,
#         w.UV_INDEX_CLEAR_SKY_MAX,

#         -- ── Fire features (NULL = no fire that day near city) ──
#         COALESCE(f.FIRE_COUNT,          0)    AS FIRE_COUNT,
#         COALESCE(f.TOTAL_ACRES_BURNED,  0)    AS TOTAL_ACRES_BURNED,
#         COALESCE(f.AVG_FRP,             0)    AS AVG_FRP,
#         COALESCE(f.MAX_FRP,             0)    AS MAX_FRP,
#         COALESCE(f.AVG_BRIGHTNESS,      0)    AS AVG_BRIGHTNESS,
#         f.NEAREST_FIRE_KM,                    -- NULL means no fire within radius
#         COALESCE(f.HAS_PERIMETER,       0)    AS HAS_PERIMETER,
#         COALESCE(f.HAS_FIRMS_POINT,     0)    AS HAS_FIRMS_POINT,
#         CASE WHEN f.DATE IS NOT NULL THEN 1 ELSE 0 END AS FIRE_ACTIVE,  -- binary flag

#         -- ── AQI targets ──
#         a.AQI_PM25,
#         a.AQI_PM10,
#         a.AQI_O3,
#         a.AQI_NO2,
#         a.AQI_MAX                             -- primary prediction target

#     FROM weather          w
#     LEFT JOIN fire_daily  f ON f.DATE = w.DATE AND f.CITY = w.CITY
#     LEFT JOIN aqi         a ON a.DATE = w.DATE AND a.CITY = w.CITY

#     -- Only keep rows where we have an AQI target (for supervised learning)
#     WHERE a.AQI_MAX IS NOT NULL

#     ORDER BY w.DATE, w.CITY;
#     """

#     hook.run(create_sql)
#     result = hook.get_first(
#         f"SELECT COUNT(*) FROM {DATABASE}.{RAW_SCHEMA}.FIRE_AQI_WEATHER_FEATURES"
#     )
#     logger.info("ML feature table rows: %d", result[0])


# ── DAG ───────────────────────────────────────────────────────────────────────

default_args = {
    "owner":        "data-eng",
    "retries":      2,
    "retry_delay":  timedelta(minutes=5),
}

with DAG(
    dag_id="fire_data_transform_city_join",
    description="Reverse geocode fire data → assign city → join with AQI + Weather",
    default_args=default_args,
    start_date=datetime(2026, 4, 1),
    schedule=None,    # run after both historical + incremental DAGs complete
    catchup=False,
    tags=["fire", "transform", "ml-features"],
) as dag:

    t_city = PythonOperator(
        task_id="load_city_reference",
        python_callable=load_city_reference,
    )
    t_ca = PythonOperator(
        task_id="transform_ca_perimeters",
        python_callable=transform_ca_perimeters,
    )
    t_firms = PythonOperator(
        task_id="transform_nasa_firms",
        python_callable=transform_nasa_firms,
    )
    t_load = PythonOperator(
        task_id="load_fire_data_to_snowflake",
        python_callable=load_fire_data_to_snowflake,
    )
    # t_ml = PythonOperator(
    #     task_id="build_ml_feature_table",
    #     python_callable=build_ml_feature_table,
    # )

    t_city >> [t_ca, t_firms] >> t_load