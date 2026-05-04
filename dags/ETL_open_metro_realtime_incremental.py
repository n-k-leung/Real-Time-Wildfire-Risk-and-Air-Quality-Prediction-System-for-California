from datetime import datetime
import os
import requests
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def get_logical_date():
    context = get_current_context()
    return str(context['logical_date'])[:10]


def return_snowflake_conn(con_id):
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    return hook.get_conn().cursor()


def get_weather(date, latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": date,
        "end_date": date,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_mean",
            "temperature_2m_min",
            "apparent_temperature_max",
            "apparent_temperature_mean",
            "apparent_temperature_min",
            "precipitation_sum",
            "rain_sum",
            "precipitation_hours",
            "precipitation_probability_max",
            "precipitation_probability_mean",
            "precipitation_probability_min",
            "weather_code",
            "wind_speed_10m_max",
            "wind_direction_10m_dominant",
            "uv_index_max",
            "uv_index_clear_sky_max",
        ],
        "timezone": "America/Los_Angeles"
    }

    r = requests.get(url, params=params)

    if r.status_code != 200:
        raise Exception(f"API error: {r.status_code} -> {r.text[:300]}")

    data = r.json()

    if "daily" not in data:
        raise Exception(f"No 'daily' in response: {data}")

    return pd.DataFrame({
        "date": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_mean": data["daily"]["temperature_2m_mean"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "apparent_temp_max": data["daily"]["apparent_temperature_max"],
        "apparent_temp_mean": data["daily"]["apparent_temperature_mean"],
        "apparent_temp_min": data["daily"]["apparent_temperature_min"],
        "precipitation_sum": data["daily"]["precipitation_sum"],
        "rain_sum": data["daily"]["rain_sum"],
        "precipitation_hours": data["daily"]["precipitation_hours"],
        "precipitation_probability_max": data["daily"]["precipitation_probability_max"],
        "precipitation_probability_mean": data["daily"]["precipitation_probability_mean"],
        "precipitation_probability_min": data["daily"]["precipitation_probability_min"],
        "weather_code": data["daily"]["weather_code"],
        "wind_speed_10m_max": data["daily"]["wind_speed_10m_max"],
        "wind_direction_10m_dominant": data["daily"]["wind_direction_10m_dominant"],
        "uv_index_max": data["daily"]["uv_index_max"],
        "uv_index_clear_sky_max": data["daily"]["uv_index_clear_sky_max"],
    })


def save_weather_data(city, latitude, longitude, date, file_path):
    df = get_weather(date, latitude, longitude)

    df["latitude"] = float(latitude)
    df["longitude"] = float(longitude)
    df["city"] = city

    df = df[
        [
            "latitude",
            "longitude",
            "date",
            "temp_max",
            "temp_mean",
            "temp_min",
            "apparent_temp_max",
            "apparent_temp_mean",
            "apparent_temp_min",
            "precipitation_sum",
            "rain_sum",
            "precipitation_hours",
            "precipitation_probability_max",
            "precipitation_probability_mean",
            "precipitation_probability_min",
            "weather_code",
            "wind_speed_10m_max",
            "uv_index_max",
            "uv_index_clear_sky_max",
            "city"
        ]
    ]

    df.to_csv(file_path, index=False)


@task
def extract(cities):
    date = get_logical_date()
    paths = []

    for city in cities:
        name = city["name"].replace(" ", "_")
        lat = city["lat"]
        lon = city["lon"]

        path = f"/tmp/{name}_{date}.csv"

        save_weather_data(name, lat, lon, date, path)
        paths.append(path)

    return paths


def populate_table_via_stage(cur, database, schema, table, file_path):
    stage = f"TEMP_STAGE_{table}"
    file_name = os.path.basename(file_path)

    cur.execute(f"USE SCHEMA {database}.{schema}")

    cur.execute(f"DROP STAGE IF EXISTS {stage}")
    cur.execute(f"CREATE TEMPORARY STAGE {stage}")

    cur.execute(f"PUT file://{file_path} @{stage} AUTO_COMPRESS=TRUE")

    cur.execute(f"""
        COPY INTO {schema}.{table}
        FROM @{stage}/{file_name}.gz
        FILE_FORMAT = (
            TYPE = CSV
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
        )
    """)


@task
def load(file_paths, database, schema, table):
    date = get_logical_date()
    cur = return_snowflake_conn("snowflake_con")

    try:
        cur.execute("BEGIN")

        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} (
            latitude FLOAT,
            longitude FLOAT,
            date DATE,
            temp_max FLOAT,
            temp_mean FLOAT,
            temp_min FLOAT,
            apparent_temp_max FLOAT,
            apparent_temp_mean FLOAT,
            apparent_temp_min FLOAT,
            precipitation_sum FLOAT,
            rain_sum FLOAT,
            precipitation_hours FLOAT,
            precipitation_probability_max FLOAT,
            precipitation_probability_mean FLOAT,
            precipitation_probability_min FLOAT,
            weather_code VARCHAR,
            wind_speed_10m_max FLOAT,
            uv_index_max FLOAT,
            uv_index_clear_sky_max FLOAT,
            city VARCHAR,
            PRIMARY KEY (latitude, longitude, date, city)
        )
        """)

        cur.execute(f"""
            DELETE FROM {database}.{schema}.{table}
            WHERE date = '{date}'
        """)

        for path in file_paths:
            populate_table_via_stage(cur, database, schema, table, path)

        cur.execute("COMMIT")

    except Exception as e:
        cur.execute("ROLLBACK")
        raise e


with DAG(
    dag_id="WeatherData_Incremental",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    schedule="30 3 * * *",
    max_active_runs=1,
    tags=["ETL"]
) as dag:

    cities = [
        {"name": "Los Angeles", "lat": Variable.get("LATITUDE_LOSANGELES"), "lon": Variable.get("LONGITUDE_LOSANGELES")},
        {"name": "Fresno", "lat": Variable.get("LATITUDE_FRESNO"), "lon": Variable.get("LONGITUDE_FRESNO")},
        {"name": "Riverside", "lat": Variable.get("LATITUDE_RIVERSIDE"), "lon": Variable.get("LONGITUDE_RIVERSIDE")},
    ]

    db = "user_db_groundhog"
    schema = "raw"
    table = "weather_data_proj"

    files = extract(cities)
    load(files, db, schema, table)