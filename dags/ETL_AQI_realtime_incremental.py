from datetime import datetime, timedelta
import requests
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


AIRNOW_API_KEY = "A776542E-74BA-4F39-BDFD-A51014FE5E9A"

def get_logical_date():
    from airflow.operators.python import get_current_context
    context = get_current_context()
    return str(context["logical_date"])[:10]


def return_snowflake_conn(con_id):
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    return hook.get_conn().cursor()

def get_airnow_historical(lat, lon, start_date, end_date):
    url = "https://www.airnowapi.org/aq/data/"

    params = {
        "startDate": start_date,
        "endDate": end_date,
        "parameters": "PM25,PM10,OZONE,NO2,CO,SO2",
        "BBOX": f"{lon-0.5},{lat-0.5},{lon+0.5},{lat+0.5}",
        "dataType": "A",
        "format": "application/json",
        "verbose": 0,
        "monitorType": 0,
        "includerawconcentrations": 0,
        "API_KEY": AIRNOW_API_KEY,
    }

    r = requests.get(url, params=params)

    if r.status_code != 200:
        raise Exception(f"AirNow API error: {r.status_code} -> {r.text[:300]}")

    return r.json()

def get_airnow(lat, lon):
    url = "https://www.airnowapi.org/aq/observation/latLong/current/"
    params = {
        "format": "application/json",
        "latitude": lat,
        "longitude": lon,
        "distance": 25,
        "API_KEY": AIRNOW_API_KEY,
    }

    r = requests.get(url, params=params)

    if r.status_code != 200:
        raise Exception(f"AirNow API error: {r.status_code} -> {r.text[:300]}")

    return r.json()

@task
def extract(cities):
    import time
    records = []

    yesterday = datetime.utcnow().date() - timedelta(days=1)
    start_str = yesterday.strftime("%Y-%m-%dT00")
    end_str = yesterday.strftime("%Y-%m-%dT23")

    for city in cities:
        lat = float(city["lat"])
        lon = float(city["lon"])
        city_name = city["name"]

        # Historical yesterday data
        try:
            hist_data = get_airnow_historical(lat, lon, start_str, end_str)
            for row in hist_data:
                records.append({
                    "date": row.get("UTC")[:10],
                    "latitude": row.get("Latitude"),
                    "longitude": row.get("Longitude"),
                    "city": city_name,
                    "parameter": row.get("Parameter"),
                    "aqi": row.get("AQI"),
                })
            print(f"Fetched historical for {city_name} {yesterday}: {len(hist_data)} rows")
        except Exception as e:
            print(f"Skipping historical {city_name} {yesterday}: {e}")

        # Current data for today
        try:
            current_data = get_airnow(lat, lon)
            for row in current_data:
                records.append({
                    "date": row.get("DateObserved"),
                    "latitude": lat,
                    "longitude": lon,
                    "city": city_name,
                    "parameter": row.get("ParameterName"),
                    "aqi": row.get("AQI"),
                })
            print(f"Fetched current for {city_name} today: {len(current_data)} rows")
        except Exception as e:
            print(f"Skipping current {city_name}: {e}")

        time.sleep(1)  # avoid API rate limit

    return records

@task
def transform(records):
    df = pd.DataFrame(records)

    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Keep only yesterday and today
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    df = df[df["date"].isin([yesterday, today])]

    if df.empty:
        return []

    # find row with max AQI per day per city
    idx = df.groupby(["date", "city"])["aqi"].idxmax()
    result_df = df.loc[idx, ["date", "city", "parameter", "aqi"]]

    return result_df.to_dict(orient="records")


@task
def load(records, database, schema, table):
    if not records:
        print("[LOAD] No data to load")
        return

    con = return_snowflake_conn("snowflake_con")

    try:
        con.execute("BEGIN;")

        con.execute(f"USE DATABASE {database};")
        con.execute(f"USE SCHEMA {schema};")

        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                date DATE,
                city VARCHAR,
                parameter VARCHAR,
                aqi FLOAT,
                PRIMARY KEY (date, city)
            );
        """)

        # Temporary stage table
        stage_table = table + "_STAGE"
        con.execute(f"CREATE OR REPLACE TEMP TABLE {stage_table} LIKE {table};")

        insert_sql = f"""
            INSERT INTO {stage_table} (date, city, parameter, aqi)
            VALUES (%s, %s, %s, %s);
        """

        data = [(r["date"], r["city"], r["parameter"], r["aqi"]) for r in records]
        con.executemany(insert_sql, data)

        # MERGE upsert
        con.execute(f"""
            MERGE INTO {table} t
            USING {stage_table} s
            ON t.date = s.date AND t.city = s.city
            WHEN MATCHED THEN UPDATE SET t.parameter = s.parameter, t.aqi = s.aqi
            WHEN NOT MATCHED THEN INSERT (date, city, parameter, aqi)
            VALUES (s.date, s.city, s.parameter, s.aqi);
        """)

        print(f"Inserted/updated rows for yesterday/today: {len(records)}")
        con.execute("COMMIT;")

    except Exception as e:
        con.execute("ROLLBACK;")
        raise e

with DAG(
    dag_id="AQIData_Incremental",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    schedule="30 3 * * *",
    max_active_runs=1,
    tags=["airnow", "aqi", "etl"]
) as dag:

    cities = [
        {"name": "Los Angeles", "lat": Variable.get("LATITUDE_LOSANGELES"), "lon": Variable.get("LONGITUDE_LOSANGELES")},
        {"name": "Fresno", "lat": Variable.get("LATITUDE_FRESNO"), "lon": Variable.get("LONGITUDE_FRESNO")},
        {"name": "Riverside", "lat": Variable.get("LATITUDE_RIVERSIDE"), "lon": Variable.get("LONGITUDE_RIVERSIDE")},
    ]

    db = "user_db_coyote"
    schema = "raw"
    table = "aqi_proj"

    raw = extract(cities)
    clean = transform(raw)
    load(clean, db, schema, table)