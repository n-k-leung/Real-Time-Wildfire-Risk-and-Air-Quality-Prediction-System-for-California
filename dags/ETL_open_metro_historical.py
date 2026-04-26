from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime, date
import requests

default_args = {
    'owner': 'natleung',
    'email': ['natalie.leung@sjsu.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def return_snowflake_conn(con_id):
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    conn = hook.get_conn()
    return conn.cursor()

# Extract task now handles multiple cities
@task
def extract(cities):
    all_city_data = []
    url = "https://archive-api.open-meteo.com/v1/archive"

    # end_date = date.today()
    # start_date = end_date - timedelta(days=30)

    for city in cities:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "past_days": 365,
            "forecast_days": 0,
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
        response = requests.get(url, params=params)
        if response.status_code != 200:
            raise RuntimeError(f"API request failed for {city['name']}: {response.status_code}")
        
        all_city_data.append({
            "city": city["name"],
            "latitude": city["lat"],
            "longitude": city["lon"],
            "daily": response.json().get("daily", {})
        })
    
    return all_city_data

# Transform task now takes the whole list from extract
@task
def transform(all_city_data):
    all_records = []
    for city_data in all_city_data:
        daily = city_data["daily"]
        city_name = city_data["city"]
        lat = city_data["latitude"]
        lon = city_data["longitude"]

        for i in range(len(daily.get("time", []))):
            all_records.append({
                "latitude": lat,
                "longitude": lon,
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
                "uv_index_max": daily["uv_index_max"][i],
                "uv_index_clear_sky_max": daily["uv_index_clear_sky_max"][i],
                "city": city_name,
            })
    return all_records

@task
def load(con, target_table, records):
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
                weather_code VARCHAR(3),
                wind_speed_10m_max FLOAT,
                uv_index_max FLOAT,
                uv_index_clear_sky_max FLOAT,
                city VARCHAR(100),
                PRIMARY KEY (latitude, longitude, date, city)
            );
        """)
        con.execute(f"DELETE FROM {target_table};")

        insert_sql = f"""
            INSERT INTO {target_table} (
                latitude, longitude, date, temp_max, temp_mean, temp_min, apparent_temp_max, apparent_temp_mean, apparent_temp_min, precipitation_sum, rain_sum,
                precipitation_hours, precipitation_probability_max, precipitation_probability_mean, precipitation_probability_min, weather_code, wind_speed_10m_max, uv_index_max, uv_index_clear_sky_max, city
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            );
        """
        data = [
            (
                r["latitude"], r["longitude"], r["date"], r["temp_max"], r["temp_mean"], r["temp_min"], r["apparent_temp_max"], r["apparent_temp_mean"], r["apparent_temp_min"], r["precipitation_sum"], r["rain_sum"],
                r["precipitation_hours"], r["precipitation_probability_max"], r["precipitation_probability_mean"], r["precipitation_probability_min"], r["weather_code"], r["wind_speed_10m_max"], r["uv_index_max"], r["uv_index_clear_sky_max"],r["city"],
            )
            for r in records
        ]

        con.executemany(insert_sql, data)
        con.execute("COMMIT;")
        print(f"Loaded {len(data)} records into {target_table}")
    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise

with DAG(
    dag_id='WeatherData_Historical',
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['ETL'],
    default_args=default_args,
    schedule=None
) as dag:

    cities = [
        {"name": "Cupertino", "lat": Variable.get("LATITUDE_CUPERTINO"), "lon": Variable.get("LONGITUDE_CUPERTINO")},
        {"name": "San Jose", "lat": Variable.get("LATITUDE_SANJOSE"), "lon": Variable.get("LONGITUDE_SANJOSE")},
        {"name": "San Francisco", "lat": Variable.get("LATITUDE_SANFRANCISCO"), "lon": Variable.get("LONGITUDE_SANFRANCISCO")},
    ]

    target_table = "raw.weather_data_proj"
    cur = return_snowflake_conn("snowflake_con")

    raw_data = extract(cities)
    transformed_data = transform(raw_data)
    load(cur, target_table, transformed_data)