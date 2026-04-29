from datetime import datetime, timedelta
import requests
import pandas as pd
import math

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    "owner": "SaminaMaraj",
    "email": ["kazisaminamaraj.mumu@sjsu.edu"],
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False   # avoids SMTP crash
}

NIFC_QUERY_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
    "InterAgencyFirePerimeterHistory_All_Years_View/FeatureServer/0/query"
)


def get_cursor():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_con")
    return hook.get_conn().cursor()

def get_nifc_historical(city_name, lat, lon, fire_year_start):
    offset = 1.0
    xmin, ymin = float(lon) - offset, float(lat) - offset
    xmax, ymax = float(lon) + offset, float(lat) + offset

    params = {
        "f": "json",
        "where": f"FIRE_YEAR_INT >= {fire_year_start}",
        "geometry": f"{xmin},{ymin},{xmax},{ymax}",
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": "IRWINID,INCIDENT,GIS_ACRES,DATE_CUR,FIRE_YEAR_INT,AGENCY,SOURCE",
        "returnGeometry": "false",
        "resultRecordCount": 2000
    }

    r = requests.get(NIFC_QUERY_URL, params=params, timeout=60)
    r.raise_for_status()

    data = r.json()

    records = []
    for f in data.get("features", []):
        a = f.get("attributes", {})

        records.append({
            "city": city_name,
            "irwin_id": a.get("IRWINID"),
            "incident": a.get("INCIDENT"),
            "gis_acres": a.get("GIS_ACRES"),
            "date_cur": a.get("DATE_CUR"),
            "fire_year": a.get("FIRE_YEAR_INT"),
            "agency": a.get("AGENCY"),
            "source": a.get("SOURCE"),
        })

    return records

def most_common(series):
    m = series.mode()
    return m.iloc[0] if not m.empty else None


def clean_value(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    return v

@task
def extract(cities):
    all_records = []

    current_year = datetime.utcnow().year
    fire_year_start = current_year - 6

    for c in cities:
        try:
            recs = get_nifc_historical(
                c["name"],
                float(c["lat"]),
                float(c["lon"]),
                fire_year_start
            )
            all_records.extend(recs)
        except Exception as e:
            print(f"Skipping {c['name']}: {e}")

    return all_records


@task
def transform(records, ds=None):

    today = datetime.strptime(ds, "%Y-%m-%d").date()
    cutoff_date = today - timedelta(days=5 * 365)

    df = pd.DataFrame(records)

    if df.empty:
        return []

    df["date"] = pd.to_datetime(df["date_cur"], errors="coerce").dt.date
    df["gis_acres"] = pd.to_numeric(df["gis_acres"], errors="coerce")
    df["fire_year"] = pd.to_numeric(df["fire_year"], errors="coerce")

    df = df[df["date"].notna()]
    df = df[(df["date"] < today) & (df["date"] >= cutoff_date)]
    df = df.dropna(subset=["city"])

    # Aggregate
    daily_df = (
        df.groupby(["city", "date"])
        .agg(
            incident_count=("irwin_id", "count"),
            total_acres=("gis_acres", "sum"),
            avg_acres=("gis_acres", "mean"),
            max_acres=("gis_acres", "max"),
            most_common_incident=("incident", most_common),
            most_common_agency=("agency", most_common),
            most_common_source=("source", most_common),
        )
        .reset_index()
    )

    cities = daily_df["city"].unique()

    all_dates = pd.date_range(
        start=cutoff_date,
        end=today - timedelta(days=1)
    ).date

    full_index = pd.MultiIndex.from_product(
        [cities, all_dates],
        names=["city", "date"]
    )

    daily_df = (
        daily_df
        .set_index(["city", "date"])
        .reindex(full_index)
        .reset_index()
    )

    daily_df["incident_count"] = daily_df["incident_count"].fillna(0)
    daily_df["total_acres"] = daily_df["total_acres"].fillna(0)

    return daily_df.where(pd.notnull(daily_df), None).to_dict("records")

@task
def load(records, db, schema, table):

    if not records:
        return

    cur = get_cursor()

    try:
        cur.execute("BEGIN")

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {db}.{schema}.{table} (
            date DATE,
            city VARCHAR,
            incident_count INTEGER,
            total_acres FLOAT,
            avg_acres FLOAT,
            max_acres FLOAT,
            most_common_incident VARCHAR,
            most_common_agency VARCHAR,
            most_common_source VARCHAR,
            PRIMARY KEY (date, city)
        )
        """)

        cur.execute(f"TRUNCATE TABLE {db}.{schema}.{table}")

        sql = f"""
        INSERT INTO {db}.{schema}.{table}
        (date, city, incident_count, total_acres, avg_acres, max_acres,
         most_common_incident, most_common_agency, most_common_source)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        data = [
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

        cur.executemany(sql, data)
        cur.execute("COMMIT")

        print(f"Loaded {len(data)} rows")

    except Exception as e:
        cur.execute("ROLLBACK")
        raise e


with DAG(
    dag_id="NIFC_Fire_Historical",
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    tags=["ETL", "fire", "historical"],
    default_args=default_args,
) as dag:

    cities = [
        {
            "name": "Los Angeles",
            "lat": Variable.get("LATITUDE_LOSANGELES"),
            "lon": Variable.get("LONGITUDE_LOSANGELES"),
        },
        {
            "name": "Fresno",
            "lat": Variable.get("LATITUDE_FRESNO"),
            "lon": Variable.get("LONGITUDE_FRESNO"),
        },
        {
            "name": "Riverside",
            "lat": Variable.get("LATITUDE_RIVERSIDE"),
            "lon": Variable.get("LONGITUDE_RIVERSIDE"),
        },
    ]

    db = "user_db_coyote"
    schema = "raw"
    table = "nifc_fire_historical_proj"

    raw = extract(cities)
    clean = transform(raw)
    load(clean, db, schema, table)