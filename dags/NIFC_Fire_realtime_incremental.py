"""NIFC wildfire daily incremental ETL."""

import math
import logging
from datetime import datetime, timedelta, date, timezone

import requests
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)

NIFC_QUERY_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
    "WFIGS_Interagency_Perimeters_YearToDate/FeatureServer/0/query"
)

SNOWFLAKE_CONN_ID = "snowflake_con"
TARGET_DB    = "user_db_coyote"
TARGET_SCHEMA = "raw"
TARGET_TABLE  = "nifc_fire_proj"
FULL_TABLE   = f"{TARGET_DB}.{TARGET_SCHEMA}.{TARGET_TABLE}"

def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    return hook.get_conn()


def fetch_fire_records_for_city(city_name: str, lat: float, lon: float, target_date: date) -> list[dict]:
    """
    Pulls all wildfire perimeter features within a 1-degree bounding box
    around the city that were active on target_date.

    Handles pagination automatically — the API caps at 2000 records per
    request and signals more data via exceededTransferLimit=true.
    """

    bbox_offset = 1.0
    xmin = lon - bbox_offset
    ymin = lat - bbox_offset
    xmax = lon + bbox_offset
    ymax = lat + bbox_offset

    day_start_utc = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=timezone.utc)
    day_end_utc = datetime(target_date.year, target_date.month, target_date.day, 23, 59, 59, tzinfo=timezone.utc)
    day_start_ms = int(day_start_utc.timestamp() * 1000)
    day_end_ms = int(day_end_utc.timestamp() * 1000)

    where_clause = (
        f"attr_FireDiscoveryDateTime >= {day_start_ms} "
        f"AND attr_FireDiscoveryDateTime <= {day_end_ms}"
    )

    all_records = []
    offset = 0
    page_size = 2000  # max the API allows per request

    logger.info("Fetching fire data for %s on %s ...", city_name, target_date)

    while True:
        params = {
            "f":               "json",
            "where":           where_clause,
            "geometry":        f"{xmin},{ymin},{xmax},{ymax}",
            "geometryType":    "esriGeometryEnvelope",
            "inSR":            "4326",
            "spatialRel":      "esriSpatialRelIntersects",
            "outFields":       (
                "attr_IncidentName,"
                "attr_CalculatedAcres,"
                "attr_FireDiscoveryDateTime,"
                "attr_POOResponsibleAgency,"
                "attr_FireBehaviorGeneral,"
                "attr_IncidentTypeCategory,"
                "poly_IncidentName,"
                "poly_DateCurrent"
            ),
            "returnGeometry":    "false",
            "resultOffset":      offset,
            "resultRecordCount": page_size,
        }

        try:
            response = requests.get(NIFC_QUERY_URL, params=params, timeout=90)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.error("HTTP error fetching %s: %s", city_name, exc)
            raise

        payload = response.json()

        # Surface any ArcGIS-level errors clearly
        if "error" in payload:
            raise RuntimeError(
                f"ArcGIS query error for {city_name}: {payload['error']}"
            )

        features = payload.get("features", [])
        logger.info("  Page offset=%d returned %d features", offset, len(features))

        for feature in features:
            attrs = feature.get("attributes", {}) or {}

            # Resolve incident name (try both field variants)
            incident_name = (
                attrs.get("attr_IncidentName")
                or attrs.get("poly_IncidentName")
            )

            # Resolve acres
            raw_acres = attrs.get("attr_CalculatedAcres")

            # Resolve discovery date from epoch milliseconds to date
            raw_ts_ms = attrs.get("attr_FireDiscoveryDateTime")
            try:
                fire_date = datetime.utcfromtimestamp(raw_ts_ms / 1000).date() if raw_ts_ms else None
            except (TypeError, ValueError, OSError):
                fire_date = None

            all_records.append({
                "city":          city_name,
                "fire_date":     fire_date,
                "incident_name": incident_name,
                "acres":         float(raw_acres) if raw_acres is not None else None,
                "agency":        attrs.get("attr_POOResponsibleAgency"),
                "fire_behavior": attrs.get("attr_FireBehaviorGeneral"),
                "incident_type": attrs.get("attr_IncidentTypeCategory"),
            })

        # If the API says there's more, move to next page; otherwise stop
        if not payload.get("exceededTransferLimit", False):
            break

        offset += len(features)

    logger.info("Total records fetched for %s: %d", city_name, len(all_records))
    return all_records


def most_common(series: pd.Series):
    mode = series.dropna().mode()
    return mode.iloc[0] if not mode.empty else None


def clean_value(v):
    """Convert NaN / numpy types to plain Python None or scalar."""
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    # Convert numpy int/float to plain Python types
    if hasattr(v, "item"):
        return v.item()
    return v


@task
def extract(cities: list[dict], ds: str) -> list[dict]:
    """
    ds is the Airflow logical date (YYYY-MM-DD string), which for a @daily
    schedule is always yesterday when catchup=False. We use it as our target
    date so every run knows exactly which day to fetch.
    """
    # Prefer Airflow data interval to avoid "future date" confusion from timezone shifts.
    context = get_current_context()
    interval_start = context.get("data_interval_start")
    if interval_start is not None:
        local_dt = interval_start.in_timezone("America/Los_Angeles")
        target_date = date(local_dt.year, local_dt.month, local_dt.day)
    else:
        target_date = datetime.strptime(ds, "%Y-%m-%d").date()
    logger.info("Extract task running for date: %s", target_date)

    all_records = []

    for city in cities:
        city_name = city["name"]
        lat = float(city["lat"])
        lon = float(city["lon"])

        try:
            records = fetch_fire_records_for_city(city_name, lat, lon, target_date)
            all_records.extend(records)
        except Exception as exc:
            # Log and continue — don't fail the whole DAG for one city
            logger.error("Failed to fetch data for %s: %s", city_name, exc)

    logger.info("Total raw records extracted across all cities: %d", len(all_records))
    return all_records


@task
def transform(records: list[dict], ds: str) -> list[dict]:
    """
    Aggregates raw fire records into one summary row per city per day.
    On days with no fires the city still gets a row with zeros / nulls
    so downstream queries never have gaps.
    """
    # Keep target date aligned with extract's data interval logic.
    context = get_current_context()
    interval_start = context.get("data_interval_start")
    if interval_start is not None:
        local_dt = interval_start.in_timezone("America/Los_Angeles")
        target_date = date(local_dt.year, local_dt.month, local_dt.day)
    else:
        target_date = datetime.strptime(ds, "%Y-%m-%d").date()

    if not records:
        logger.info("No records to transform — building zero-fill rows for all cities.")

    df = pd.DataFrame(records) if records else pd.DataFrame(
        columns=["city", "fire_date", "incident_name", "acres", "agency", "fire_behavior", "incident_type"]
    )

    df["fire_date"] = pd.to_datetime(df["fire_date"], errors="coerce").dt.date
    df["acres"]     = pd.to_numeric(df["acres"], errors="coerce")

    df = df[df["fire_date"] == target_date]

    city_names = ["Los Angeles", "Fresno", "Riverside"]

    if df.empty:
        logger.info("No fire records for %s — producing zero rows.", target_date)
        daily_df = pd.DataFrame([
            {
                "date":                 target_date,
                "city":                 city,
                "incident_count":       0,
                "total_acres":          0.0,
                "avg_acres":            None,
                "max_acres":            None,
                "most_common_incident": None,
                "most_common_agency":   None,
                "most_common_source":     None,
            }
            for city in city_names
        ])
    else:
        daily_df = (
            df.groupby("city")
            .agg(
                incident_count=   ("incident_name", "count"),
                total_acres=      ("acres",          "sum"),
                avg_acres=        ("acres",          "mean"),
                max_acres=        ("acres",          "max"),
                most_common_incident=("incident_name", most_common),
                most_common_agency=  ("agency",        most_common),
                most_common_source=    ("incident_type", most_common),
            )
            .reset_index()
        )
        daily_df["date"] = target_date

        for city in city_names:
            if city not in daily_df["city"].values:
                daily_df = pd.concat([
                    daily_df,
                    pd.DataFrame([{
                        "date":                 target_date,
                        "city":                 city,
                        "incident_count":       0,
                        "total_acres":          0.0,
                        "avg_acres":            None,
                        "max_acres":            None,
                        "most_common_incident": None,
                        "most_common_agency":   None,
                        "most_common_source":     None,
                    }])
                ], ignore_index=True)

    daily_df["avg_acres"] = pd.to_numeric(daily_df["avg_acres"], errors="coerce").round(3)
    daily_df["total_acres"] = pd.to_numeric(daily_df["total_acres"], errors="coerce").fillna(0).round(3)
    daily_df["max_acres"] = pd.to_numeric(daily_df["max_acres"], errors="coerce").round(3)

    daily_df = daily_df.astype(object).where(pd.notnull(daily_df), None)

    logger.info("Transform complete. Output rows: %d", len(daily_df))
    return daily_df.to_dict(orient="records")


@task
def load(records: list[dict]) -> None:
    """Upsert transformed records into Snowflake."""
    if not records:
        logger.info("No records to load.")
        return

    conn = get_snowflake_cursor()
    cur  = conn.cursor()

    stage_table = f"{TARGET_DB}.{TARGET_SCHEMA}.nifc_fire_incremental_stage"

    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
                date                   DATE        NOT NULL,
                city                   VARCHAR     NOT NULL,
                incident_count         INTEGER,
                total_acres            FLOAT,
                avg_acres              FLOAT,
                max_acres              FLOAT,
                most_common_incident   VARCHAR,
                most_common_agency     VARCHAR,
                most_common_source       VARCHAR,
                PRIMARY KEY (date, city)
            )
        """)

        cur.execute(f"""
            CREATE OR REPLACE TEMPORARY TABLE {stage_table} (
                date                   DATE,
                city                   VARCHAR,
                incident_count         INTEGER,
                total_acres            FLOAT,
                avg_acres              FLOAT,
                max_acres              FLOAT,
                most_common_incident   VARCHAR,
                most_common_agency     VARCHAR,
                most_common_source       VARCHAR
            )
        """)

        insert_sql = f"""
            INSERT INTO {stage_table}
            (date, city, incident_count, total_acres, avg_acres, max_acres,
             most_common_incident, most_common_agency, most_common_source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data_tuples = [
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
            )
            for r in records
        ]

        cur.executemany(insert_sql, data_tuples)
        logger.info("Staged %d rows into %s", len(data_tuples), stage_table)

        cur.execute(f"""
            MERGE INTO {FULL_TABLE} AS tgt
            USING {stage_table} AS src
               ON tgt.date = src.date
              AND tgt.city = src.city
            WHEN MATCHED THEN UPDATE SET
                tgt.incident_count       = src.incident_count,
                tgt.total_acres          = src.total_acres,
                tgt.avg_acres            = src.avg_acres,
                tgt.max_acres            = src.max_acres,
                tgt.most_common_incident = src.most_common_incident,
                tgt.most_common_agency   = src.most_common_agency,
                tgt.most_common_source     = src.most_common_source
            WHEN NOT MATCHED THEN INSERT (
                date, city, incident_count, total_acres, avg_acres, max_acres,
                most_common_incident, most_common_agency, most_common_source
            ) VALUES (
                src.date, src.city, src.incident_count, src.total_acres, src.avg_acres, src.max_acres,
                src.most_common_incident, src.most_common_agency, src.most_common_source
            )
        """)

        conn.commit()
        logger.info("Successfully merged %d rows into %s", len(data_tuples), FULL_TABLE)

    except Exception as exc:
        conn.rollback()
        logger.error("Load failed, rolling back: %s", exc)
        raise

    finally:
        cur.close()
        conn.close()


with DAG(
    dag_id="nifc_fire_incremental",
    description=(
        "Daily incremental wildfire ETL for Los Angeles, Fresno, and Riverside "
        "using the WFIGS real-time perimeter endpoint. Upserts into Snowflake."
    ),
    default_args={
        "owner":            "SaminaMaraj",
        "email":            ["kazisaminamaraj.mumu@sjsu.edu"],
        "email_on_failure": False,
        "retries":          2,
        "retry_delay":      timedelta(minutes=5),
    },
    start_date=datetime(2026, 4, 27),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ETL", "fire", "nifc", "incremental"],
) as dag:

    cities = [
        {
            "name": "Los Angeles",
            "lat":  Variable.get("LATITUDE_LOSANGELES",  default_var="34.0522"),
            "lon":  Variable.get("LONGITUDE_LOSANGELES", default_var="-118.2437"),
        },
        {
            "name": "Fresno",
            "lat":  Variable.get("LATITUDE_FRESNO",  default_var="36.7378"),
            "lon":  Variable.get("LONGITUDE_FRESNO", default_var="-119.7871"),
        },
        {
            "name": "Riverside",
            "lat":  Variable.get("LATITUDE_RIVERSIDE",  default_var="33.9806"),
            "lon":  Variable.get("LONGITUDE_RIVERSIDE", default_var="-117.3755"),
        },
    ]

    raw_records    = extract(cities)
    clean_records  = transform(raw_records)
    load(clean_records)