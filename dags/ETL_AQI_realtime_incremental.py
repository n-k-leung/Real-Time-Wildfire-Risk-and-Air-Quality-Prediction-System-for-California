from datetime import datetime
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
    records = []

    for city in cities:
        lat = float(city["lat"])
        lon = float(city["lon"])
        city_name = city["name"]

        data = get_airnow(lat, lon)

        for row in data:
            records.append({
                "date": row.get("DateObserved"),
                "city": city_name,
                "parameter": row.get("ParameterName"),
                "aqi": row.get("AQI"),
            })

    return records


@task
def transform(records):
    df = pd.DataFrame(records)

    df["date"] = pd.to_datetime(df["date"]).dt.date
    # find row with max AQI per day per city
    idx = df.groupby(["date", "city"])["aqi"].idxmax()

    result_df = df.loc[idx, ["date", "city", "parameter", "aqi"]]

    return result_df.to_dict(orient="records")


@task
def load(records, database, schema, table):
    cur = return_snowflake_conn("snowflake_con")

    try:
        cur.execute("BEGIN")

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} (
            date DATE,
            city VARCHAR,
            parameter VARCHAR,
            aqi FLOAT
        )
        """)

        # cur.execute(f"DELETE FROM {database}.{schema}.{table}")

        insert_sql = f"""
        INSERT INTO {database}.{schema}.{table}
        (date, city, parameter, aqi)
        VALUES (%s, %s, %s, %s)
        """

        data = [
            (
                r["date"],
                r["city"],
                r["parameter"],
                r["aqi"],
            )
            for r in records
        ]

        cur.executemany(insert_sql, data)

        cur.execute("COMMIT")

    except Exception as e:
        cur.execute("ROLLBACK")
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
        {"name": "Cupertino", "lat": Variable.get("LATITUDE_CUPERTINO"), "lon": Variable.get("LONGITUDE_CUPERTINO")},
        {"name": "San Jose", "lat": Variable.get("LATITUDE_SANJOSE"), "lon": Variable.get("LONGITUDE_SANJOSE")},
        {"name": "San Francisco", "lat": Variable.get("LATITUDE_SANFRANCISCO"), "lon": Variable.get("LONGITUDE_SANFRANCISCO")},
    ]

    db = "user_db_coyote"
    schema = "raw"
    table = "aqi_proj"

    raw = extract(cities)
    clean = transform(raw)
    load(clean, db, schema, table)