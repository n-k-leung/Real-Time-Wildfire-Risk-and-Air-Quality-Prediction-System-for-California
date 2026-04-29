from datetime import datetime, timedelta
import requests
import pandas as pd

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

# NIFC ArcGIS FeatureServer query endpoint
NIFC_QUERY_URL = (
    "https://services3.arcgis.com/T4QMspbfLg3qTGWY/arcgis/rest/services/"
    "InterAgencyFirePerimeterHistory_All_Years_View/FeatureServer/0/query"
)

def return_snowflake_conn(con_id):
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    return hook.get_conn().cursor()

def get_nifc_historical(city_name, lat, lon, fire_year_start):
    """
    Query NIFC historical fire perimeter data for a city region using a small
    bounding box around the city.
    """
    # small bbox around city center; adjust later if needed
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

@task
def extract(cities):
    all_records = []

    # Historical perimeter data; keep recent years only for a smaller first ETL
    current_year = datetime.utcnow().year
    fire_year_start = current_year - 5

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
    df = pd.DataFrame(records)

    if df.empty:
        return []

    # Convert DATE_CUR to date where possible
    df["date"] = pd.to_datetime(df["date_cur"], errors="coerce").dt.date

    # Numeric cleanup
    df["gis_acres"] = pd.to_numeric(df["gis_acres"], errors="coerce")
    df["fire_year"] = pd.to_numeric(df["fire_year"], errors="coerce")

    # Keep only rows with at least a city and some historical identifier/date info
    df = df.dropna(subset=["city"])

    # Optional light dedupe
    df = df.drop_duplicates(subset=["city", "incident", "date", "fire_year"])

    result_df = df[
        [
            "date",
            "city",
            "irwin_id",
            "incident",
            "gis_acres",
            "fire_year",
            "agency",
            "source",
        ]
    ]

    return result_df.to_dict(orient="records")

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
            irwin_id VARCHAR,
            incident VARCHAR,
            gis_acres FLOAT,
            fire_year INTEGER,
            agency VARCHAR,
            source VARCHAR
        )
        """)

        cur.execute(f"TRUNCATE TABLE {database}.{schema}.{table}")

        insert_sql = f"""
        INSERT INTO {database}.{schema}.{table}
        (date, city, irwin_id, incident, gis_acres, fire_year, agency, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        data = [
            (
                r["date"],
                r["city"],
                r["irwin_id"],
                r["incident"],
                r["gis_acres"],
                r["fire_year"],
                r["agency"],
                r["source"],
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
    dag_id="NIFC_Fire_Historical",
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

    db = "user_db_dog"
    schema = "raw"
    table = "nifc_fire_historical_proj"

    raw = extract(cities)
    clean = transform(raw)
    load(clean, db, schema, table)