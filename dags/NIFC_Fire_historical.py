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
}

NIFC_QUERY_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
    "InterAgencyFirePerimeterHistory_All_Years_View/FeatureServer/0/query"
)

def return_snowflake_conn(con_id):
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    return hook.get_conn().cursor()

def get_nifc_historical(city_name, lat, lon, fire_year_start):
    offset = 1.0
    xmin = float(lon) - offset
    ymin = float(lat) - offset
    xmax = float(lon) + offset
    ymax = float(lat) + offset

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
    if r.status_code != 200:
        raise Exception(f"NIFC API error: {r.status_code} -> {r.text[:300]}")

    data = r.json()
    if "features" not in data:
        raise Exception(f"Unexpected NIFC response for {city_name}: {data}")

    records = []
    for feature in data["features"]:
        attrs = feature.get("attributes", {})
        records.append({
            "city": city_name,
            "irwin_id": attrs.get("IRWINID"),
            "incident": attrs.get("INCIDENT"),
            "gis_acres": attrs.get("GIS_ACRES"),
            "date_cur": attrs.get("DATE_CUR"),
            "fire_year": attrs.get("FIRE_YEAR_INT"),
            "agency": attrs.get("AGENCY"),
            "source": attrs.get("SOURCE"),
        })

    return records

def most_common(series):
    mode = series.mode()
    return mode.iloc[0] if not mode.empty else None

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
    fire_year_start = current_year - 6   # slightly wider to avoid edge cutoff

    for city in cities:
        lat = float(city["lat"])
        lon = float(city["lon"])
        city_name = city["name"]

        try:
            city_records = get_nifc_historical(city_name, lat, lon, fire_year_start)
            all_records.extend(city_records)
        except Exception as e:
            print(f"Skipping {city_name}: {e}")

    return all_records


@task
def transform(records):
    import pandas as pd
    from datetime import datetime, timedelta

    df = pd.DataFrame(records)

    if df.empty:
        return []

    df["date"] = pd.to_datetime(df["date_cur"], errors="coerce").dt.date
    df["gis_acres"] = pd.to_numeric(df["gis_acres"], errors="coerce")
    df["fire_year"] = pd.to_numeric(df["fire_year"], errors="coerce")

    # Remove invalid dates
    df = df[df["date"].notna()]

    today = datetime.utcnow().date() - timedelta(days=1)
    cutoff_date = today - timedelta(days=5 * 365)

    df = df[df["date"] < today]
    df = df[df["date"] >= cutoff_date]

    df = df.dropna(subset=["city"])

    # Dataset has multiple wildfires per day so aggregate one row per city per day
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

    today = datetime.utcnow().date() - timedelta(days=1)
    cutoff_date = today - timedelta(days=5 * 365)

    all_dates = pd.date_range(
        start=cutoff_date,
        end=today - timedelta(days=1)   # exclude today
    ).date

    cities = daily_df["city"].unique()

    full_index = pd.MultiIndex.from_product(
        [cities, all_dates], names=["city", "date"]
    )

    daily_df = (
        daily_df
        .set_index(["city", "date"])
        .reindex(full_index)
        .reset_index()
    )

    # Fill missing values for no fire days
    daily_df["incident_count"] = daily_df["incident_count"].fillna(0)
    daily_df["total_acres"] = daily_df["total_acres"].fillna(0)

    # Place null values when day has no wildfire
    daily_df["avg_acres"] = daily_df["avg_acres"]
    daily_df["max_acres"] = daily_df["max_acres"]
    daily_df["most_common_incident"] = daily_df["most_common_incident"]
    daily_df["most_common_agency"] = daily_df["most_common_agency"]
    daily_df["most_common_source"] = daily_df["most_common_source"]

    daily_df = daily_df.astype(object).where(pd.notnull(daily_df), None)

    return daily_df.to_dict(orient="records")


@task
def load(records, database, schema, table):
    if not records:
        return

    cur = return_snowflake_conn("snowflake_con")

    try:
        cur.execute("BEGIN")

        cur.execute(f"""
        CREATE OR REPLACE TABLE {database}.{schema}.{table} (
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

        cur.execute(f"TRUNCATE TABLE {database}.{schema}.{table}")

        insert_sql = f"""
        INSERT INTO {database}.{schema}.{table}
        (date, city, incident_count, total_acres, avg_acres, max_acres,
        most_common_incident, most_common_agency, most_common_source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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

        cur.executemany(insert_sql, data)

        cur.execute("COMMIT")
        print(f"Loaded {len(data)} records into {table}")

    except Exception as e:
        cur.execute("ROLLBACK")
        raise e

with DAG(
    dag_id="nifc_fire_historical",
    start_date=datetime(2023, 4, 1),
    catchup=False,
    schedule=None,
    tags=["ETL", "fire", "historical"],
    default_args=default_args,
) as dag:

    cities = [
        {"name": "Los Angeles", "lat": Variable.get("LATITUDE_LOSANGELES"), "lon": Variable.get("LONGITUDE_LOSANGELES")},
        {"name": "Fresno", "lat": Variable.get("LATITUDE_FRESNO"), "lon": Variable.get("LONGITUDE_FRESNO")},
        {"name": "Riverside", "lat": Variable.get("LATITUDE_RIVERSIDE"), "lon": Variable.get("LONGITUDE_RIVERSIDE")},
    ]

    db = "user_db_coyote"
    schema = "raw"
    table = "nifc_fire_proj"

    raw = extract(cities)
    clean = transform(raw)
    load(clean, db, schema, table)