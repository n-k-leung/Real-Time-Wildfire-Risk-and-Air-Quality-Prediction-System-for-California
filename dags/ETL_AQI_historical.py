from datetime import datetime, timedelta, date
import math
import requests
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    "owner": "priyank",
    "email": ["priyank.mehta@sjsu.edu"],
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

LOOKBACK_DAYS = 1825
CITY_RADIUS_KM = 60
REQUEST_TIMEOUT = 60
SNOWFLAKE_CONN_ID = "snowflake_con"
TARGET_TABLE = "raw.aqi_data_proj"


def return_snowflake_conn(con_id):
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    return hook.get_conn().cursor()


def haversine_km_vectorized(lat1, lon1, lat2, lon2):
    r = 6371.0
    lat1 = np.radians(lat1.astype(float))
    lon1 = np.radians(lon1.astype(float))
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    return 2 * r * np.arcsin(np.sqrt(a))


def build_daily_file_url(file_base_url, day):
    yyyy = day.strftime("%Y")
    yyyymmdd = day.strftime("%Y%m%d")
    return f"{file_base_url}/airnow/{yyyy}/{yyyymmdd}/daily_data_v2.dat"


def fetch_daily_file(url):
    r = requests.get(url, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        raise Exception(f"Daily file download error {r.status_code} for {url}")
    return r.text


def split_date_ranges(start_date, end_date, chunk_days=30):
    current = start_date
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end_date)
        yield current, chunk_end
        current = chunk_end + timedelta(days=1)


@task
def get_runtime_config():
    return {
        "file_base_url": Variable.get("AIRNOW_FILE_BASE_URL", default_var="https://files.airnowtech.org"),
        "cities": [
            {
                "name": "Los Angeles",
                "lat": float(Variable.get("LATITUDE_LOSANGELES")),
                "lon": float(Variable.get("LONGITUDE_LOSANGELES")),
            },
            {
                "name": "Fresno",
                "lat": float(Variable.get("LATITUDE_FRESNO")),
                "lon": float(Variable.get("LONGITUDE_FRESNO")),
            },
            {
                "name": "Riverside",
                "lat": float(Variable.get("LATITUDE_RIVERSIDE")),
                "lon": float(Variable.get("LONGITUDE_RIVERSIDE")),
            },
        ],
    }


@task
def build_date_ranges():
    end_date = datetime.utcnow().date() - timedelta(days=1)
    start_date = end_date - timedelta(days=LOOKBACK_DAYS - 1)

    return [
        {
            "start_date": chunk_start.isoformat(),
            "end_date": chunk_end.isoformat(),
        }
        for chunk_start, chunk_end in split_date_ranges(start_date, end_date, chunk_days=30)
    ]


@task
def init_table(target_table):
    con = return_snowflake_conn(SNOWFLAKE_CONN_ID)

    try:
        con.execute("BEGIN;")
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            date DATE,
            city VARCHAR(100),
            site_name VARCHAR(255),
            parameter VARCHAR(50),
            aqi FLOAT,
            latitude FLOAT,
            longitude FLOAT,
            distance_km FLOAT,
            aqsid VARCHAR(50)
        );
        """)
        con.execute(f"DELETE FROM {target_table};")
        con.execute("COMMIT;")
        print(f"Initialized table {target_table}")
    except Exception as e:
        con.execute("ROLLBACK;")
        raise e


@task
def extract_transform_load_chunk(config, date_range, target_table):
    file_base_url = config["file_base_url"].rstrip("/")
    cities = config["cities"]

    start_date = date.fromisoformat(date_range["start_date"])
    end_date = date.fromisoformat(date_range["end_date"])

    chunk_records = []
    current_day = start_date

    while current_day <= end_date:
        url = build_daily_file_url(file_base_url, current_day)

        try:
            raw_text = fetch_daily_file(url)

            df = pd.read_csv(
                pd.io.common.StringIO(raw_text),
                sep="|",
                header=None,
                names=[
                    "valid_date",
                    "aqsid",
                    "site_name",
                    "parameter_name",
                    "reporting_units",
                    "value",
                    "averaging_period",
                    "data_source",
                    "aqi",
                    "aqi_category",
                    "latitude",
                    "longitude",
                    "full_aqsid",
                ],
                dtype=str,
                engine="python",
            )

            if df.empty:
                print(f"No rows in file for {current_day}")
                current_day += timedelta(days=1)
                continue

            df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")
            df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
            df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

            df = df.dropna(subset=["aqi", "latitude", "longitude"])
            df = df[df["aqi"] >= 0]

            if df.empty:
                print(f"No valid AQI rows for {current_day}")
                current_day += timedelta(days=1)
                continue

            city_frames = []

            for city in cities:
                city_df = df.copy()
                city_df["distance_km"] = haversine_km_vectorized(
                    city_df["latitude"],
                    city_df["longitude"],
                    city["lat"],
                    city["lon"],
                )
                city_df = city_df[city_df["distance_km"] <= CITY_RADIUS_KM].copy()
                if not city_df.empty:
                    city_df["city"] = city["name"]
                    city_frames.append(city_df)

            if city_frames:
                matched_df = pd.concat(city_frames, ignore_index=True)

                matched_df["date"] = pd.to_datetime(current_day)
                matched_df = (
                    matched_df.sort_values(["date", "city", "aqi"], ascending=[True, True, False])
                    .groupby(["date", "city"], as_index=False)
                    .first()
                )

                matched_df["date"] = matched_df["date"].dt.date

                chunk_records.extend(
                    matched_df[
                        ["date", "city", "site_name", "parameter_name", "aqi", "latitude", "longitude", "distance_km", "aqsid"]
                    ]
                    .rename(columns={"parameter_name": "parameter"})
                    .to_dict(orient="records")
                )

                print(f"Processed {current_day}: loaded {len(matched_df)} city rows from {url}")
            else:
                print(f"Processed {current_day}: no matching city rows from {url}")

        except Exception as e:
            print(f"Skipping {current_day}: {e}")

        current_day += timedelta(days=1)

    if not chunk_records:
        print(f"No records found for chunk {start_date} to {end_date}")
        return 0

    con = return_snowflake_conn(SNOWFLAKE_CONN_ID)

    try:
        con.execute("BEGIN;")

        insert_sql = f"""
        INSERT INTO {target_table}
        (date, city, site_name, parameter, aqi, latitude, longitude, distance_km, aqsid)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        data = [
            (
                r["date"],
                r["city"],
                r["site_name"],
                r["parameter"],
                r["aqi"],
                float(r["latitude"]) if r["latitude"] is not None else None,
                float(r["longitude"]) if r["longitude"] is not None else None,
                float(r["distance_km"]) if r["distance_km"] is not None else None,
                r["aqsid"],
            )
            for r in chunk_records
        ]

        con.executemany(insert_sql, data)
        con.execute("COMMIT;")
        print(f"Inserted {len(data)} rows for chunk {start_date} to {end_date}")
        return len(data)

    except Exception as e:
        con.execute("ROLLBACK;")
        raise e


with DAG(
    dag_id="AQIData_Historical_FileProduct",
    start_date=datetime(2023, 4, 1),
    catchup=False,
    schedule=None,
    tags=["ETL", "AQI", "AirNow", "FileProduct"],
    default_args=default_args,
) as dag:

    runtime_config = get_runtime_config()
    date_ranges = build_date_ranges()
    init = init_table(TARGET_TABLE)

    load_chunks = extract_transform_load_chunk.partial(
        config=runtime_config,
        target_table=TARGET_TABLE,
    ).expand(date_range=date_ranges)

    init >> load_chunks