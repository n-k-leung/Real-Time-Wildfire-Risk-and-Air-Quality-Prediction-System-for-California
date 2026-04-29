from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime, date
import requests
import time


default_args = {
    "owner": "natleung",
    "email": ["natalie.leung@sjsu.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def return_snowflake_conn(con_id):
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    return hook.get_conn().cursor()

def safe_request(url, params, retries=5):
    delay = 5

    for _ in range(retries):
        r = requests.get(url, params=params)

        if r.status_code == 200:
            return r

        if r.status_code == 429:
            time.sleep(delay)
            delay *= 2
            continue

        raise RuntimeError(f"API error: {r.status_code} -> {r.text[:200]}")

    raise RuntimeError("Too many retries (429)")

@task
def extract(cities):
    url = "https://api.open-meteo.com/v1/forecast"

    yesterday = (date.today() - timedelta(days=1)).isoformat()
    today = date.today().isoformat()

    all_data = []

    for city in cities:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "start_date": yesterday,
            "end_date": today,
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

        r = safe_request(url, params)
        daily = r.json().get("daily", {})

        if not daily:
            continue

        for i in range(len(daily.get("time", []))):
            all_data.append({
                "latitude": city["lat"],
                "longitude": city["lon"],
                "date": daily["time"][i],
                "temp_max": daily["temperature_2m_max"][i],
                "temp_mean": daily["temperature_2m_mean"][i],
                "temp_min": daily["temperature_2m_min"][i],
                "apparent_temp_max": daily["apparent_temperature_max"][i],
                "apparent_temp_mean": daily["apparent_temperature_mean"][i],
                "apparent_temp_min": daily["apparent_temperature_min"][i],
                "precipitation_sum": daily["precipitation_sum"][i],
                "rain_sum": daily["rain_sum"][i],
                "precipitation_hours": daily["precipitation_hours"][i],
                "precipitation_probability_max": daily["precipitation_probability_max"][i],
                "precipitation_probability_mean": daily["precipitation_probability_mean"][i],
                "precipitation_probability_min": daily["precipitation_probability_min"][i],
                "weather_code": daily["weather_code"][i],
                "wind_speed_10m_max": daily["wind_speed_10m_max"][i],
                "wind_direction_10m_dominant": daily["wind_direction_10m_dominant"][i],
                "uv_index_max": daily["uv_index_max"][i],
                "uv_index_clear_sky_max": daily["uv_index_clear_sky_max"][i],
                "city": city["name"],
            })

    print(f"Rows fetched: {len(all_data)}")
    return all_data

@task
def transform(records):
    for r in records:
        r["temp_max"] = round(r["temp_max"], 2) if r["temp_max"] is not None else None
        r["temp_mean"] = round(r["temp_mean"], 2) if r["temp_mean"] is not None else None
        r["temp_min"] = round(r["temp_min"], 2) if r["temp_min"] is not None else None
        r["apparent_temp_max"] = round(r["apparent_temp_max"], 2) if r["apparent_temp_max"] is not None else None
        r["apparent_temp_mean"] = round(r["apparent_temp_mean"], 2) if r["apparent_temp_mean"] is not None else None
        r["apparent_temp_min"] = round(r["apparent_temp_min"], 2) if r["apparent_temp_min"] is not None else None
        r["precipitation_sum"] = round(r["precipitation_sum"], 2) if r["precipitation_sum"] is not None else None
        r["rain_sum"] = round(r["rain_sum"], 2) if r["rain_sum"] is not None else None
        r["wind_speed_10m_max"] = round(r["wind_speed_10m_max"], 2) if r["wind_speed_10m_max"] is not None else None
        r["uv_index_max"] = round(r["uv_index_max"], 2) if r["uv_index_max"] is not None else None
        r["uv_index_clear_sky_max"] = round(r["uv_index_clear_sky_max"], 2) if r["uv_index_clear_sky_max"] is not None else None
        r["weather_code"] = str(int(r["weather_code"])) if r["weather_code"] is not None else None

    return records

@task
def load(target_table, records):
    con = return_snowflake_conn("snowflake_con")
    if not records:
        print("[LOAD] No data to load")
        return

    try:
        con.execute("BEGIN;")

        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
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
                wind_direction_10m_dominant FLOAT,
                uv_index_max FLOAT,
                uv_index_clear_sky_max FLOAT,
                city VARCHAR,
                PRIMARY KEY (latitude, longitude, date, city)
            );
        """)

        stage_table = target_table + "_STAGE"

        con.execute(f"CREATE OR REPLACE TEMP TABLE {stage_table} LIKE {target_table};")

        insert_sql = f"""
            INSERT INTO {stage_table} VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            );
        """

        data = [
            (
                r["latitude"], r["longitude"], r["date"],
                r["temp_max"], r["temp_mean"], r["temp_min"],
                r["apparent_temp_max"], r["apparent_temp_mean"], r["apparent_temp_min"],
                r["precipitation_sum"], r["rain_sum"],
                r["precipitation_hours"],
                r["precipitation_probability_max"],
                r["precipitation_probability_mean"],
                r["precipitation_probability_min"],
                r["weather_code"],
                r["wind_speed_10m_max"],
                r["wind_direction_10m_dominant"],
                r["uv_index_max"],
                r["uv_index_clear_sky_max"],
                r["city"],
            )
            for r in records
        ]

        con.executemany(insert_sql, data)

        con.execute(f"""
            MERGE INTO {target_table} t
            USING {stage_table} s
            ON t.latitude = s.latitude
            AND t.longitude = s.longitude
            AND t.date = s.date
            AND t.city = s.city

            WHEN MATCHED THEN UPDATE SET
                t.temp_max = s.temp_max,
                t.temp_mean = s.temp_mean,
                t.temp_min = s.temp_min,
                t.apparent_temp_max = s.apparent_temp_max,
                t.apparent_temp_mean = s.apparent_temp_mean,
                t.apparent_temp_min = s.apparent_temp_min,
                t.precipitation_sum = s.precipitation_sum,
                t.rain_sum = s.rain_sum,
                t.precipitation_hours = s.precipitation_hours,
                t.precipitation_probability_max = s.precipitation_probability_max,
                t.precipitation_probability_mean = s.precipitation_probability_mean,
                t.precipitation_probability_min = s.precipitation_probability_min,
                t.weather_code = s.weather_code,
                t.wind_speed_10m_max = s.wind_speed_10m_max,
                t.wind_direction_10m_dominant = s.wind_direction_10m_dominant,
                t.uv_index_max = s.uv_index_max,
                t.uv_index_clear_sky_max = s.uv_index_clear_sky_max

            WHEN NOT MATCHED THEN INSERT (
                latitude,
                longitude,
                date,
                temp_max,
                temp_mean,
                temp_min,
                apparent_temp_max,
                apparent_temp_mean,
                apparent_temp_min,
                precipitation_sum,
                rain_sum,
                precipitation_hours,
                precipitation_probability_max,
                precipitation_probability_mean,
                precipitation_probability_min,
                weather_code,
                wind_speed_10m_max,
                wind_direction_10m_dominant,
                uv_index_max,
                uv_index_clear_sky_max,
                city
            )
            VALUES (
                s.latitude,
                s.longitude,
                s.date,
                s.temp_max,
                s.temp_mean,
                s.temp_min,
                s.apparent_temp_max,
                s.apparent_temp_mean,
                s.apparent_temp_min,
                s.precipitation_sum,
                s.rain_sum,
                s.precipitation_hours,
                s.precipitation_probability_max,
                s.precipitation_probability_mean,
                s.precipitation_probability_min,
                s.weather_code,
                s.wind_speed_10m_max,
                s.wind_direction_10m_dominant,
                s.uv_index_max,
                s.uv_index_clear_sky_max,
                s.city
            )
        """)
        inserted = len(records)
        print(f"Inserted/updated rows count: {inserted}")

        con.execute("COMMIT;")

    except Exception as e:
        con.execute("ROLLBACK;")
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

    db = "user_db_coyote"
    schema = "raw"
    table = "weather_data_proj"

    raw = extract(cities)
    transformed = transform(raw)
    load(f"{db}.{schema}.{table}", transformed)