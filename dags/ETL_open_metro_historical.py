from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime, date
import requests
import time




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
# doing individual leads to 429 error so add delay
def safe_request(url, params, retries=5):
    delay = 5


    for _ in range(retries):
        response = requests.get(url, params=params)


        if response.status_code == 200:
            return response


        if response.status_code == 429:
            time.sleep(delay)
            delay *= 2
            continue


        raise RuntimeError(
            f"API request failed: {response.status_code} -> {response.text[:200]}"
        )


    raise RuntimeError("Too many 429 retries from API")


@task
def extract(cities):
    all_city_data = []
    url = "https://archive-api.open-meteo.com/v1/archive"


    end_date = date.today() - timedelta(days=1)
    total_days = 365 * 5


    for city in cities:
        city_data = {
            "city": city["name"],
            "latitude": city["lat"],
            "longitude": city["lon"],
            "daily": {}
        }
        # take in chuncks to avoid 429 error
        for offset in range(0, total_days, 365):
            chunk_end = end_date - timedelta(days=offset)
            chunk_start = chunk_end - timedelta(days=364)


            params = {
                "latitude": city["lat"],
                "longitude": city["lon"],
                "start_date": chunk_start.isoformat(),
                "end_date": chunk_end.isoformat(),
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


            response = safe_request(url, params)
            chunk = response.json().get("daily", {})


            if not city_data["daily"]:
                city_data["daily"] = chunk
            else:
                for k, v in chunk.items():
                    city_data["daily"].setdefault(k, []).extend(v)


            time.sleep(1.5)


        all_city_data.append(city_data)


    return all_city_data


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
                "wind_direction_10m_dominant": daily["wind_direction_10m_dominant"][i],
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
            CREATE OR REPLACE TABLE {target_table} (
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
                wind_direction_10m_dominant FLOAT,
                uv_index_max FLOAT,
                uv_index_clear_sky_max FLOAT,
                city VARCHAR(100),
                PRIMARY KEY (latitude, longitude, date, city)
            );
        """)


        con.execute(f"DELETE FROM {target_table};")


        insert_sql = f"""
            INSERT INTO {target_table} (
                latitude, longitude, date, temp_max, temp_mean, temp_min,
                apparent_temp_max, apparent_temp_mean, apparent_temp_min,
                precipitation_sum, rain_sum,
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
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
        con.execute("COMMIT;")


        print(f"Loaded {len(records)} records into {target_table}")


    except Exception as e:
        con.execute("ROLLBACK;")
        raise e


with DAG(
    dag_id='WeatherData_Historical',
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['ETL'],
    default_args=default_args,
    schedule=None
) as dag:


    cities = [
        {"name": "Los Angeles", "lat": Variable.get("LATITUDE_LOSANGELES"), "lon": Variable.get("LONGITUDE_LOSANGELES")},
        {"name": "Fresno", "lat": Variable.get("LATITUDE_FRESNO"), "lon": Variable.get("LONGITUDE_FRESNO")},
        {"name": "Riverside", "lat": Variable.get("LATITUDE_RIVERSIDE"), "lon": Variable.get("LONGITUDE_RIVERSIDE")},
    ]


    target_table = "raw.weather_data_proj"
    cur = return_snowflake_conn("snowflake_con")


    raw_data = extract(cities)
    transformed_data = transform(raw_data)
    load(cur, target_table, transformed_data)