from datetime import datetime, timedelta
import requests
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'natleung',
    'email': ['natalie.leung@sjsu.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

AIRNOW_API_KEY = "A776542E-74BA-4F39-BDFD-A51014FE5E9A"


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


@task
def extract(cities):
    records = []

    end_date = datetime.utcnow().date() - timedelta(days=1)
    # changing to start_date = end_date - timedelta(days=364) gives errors
    # [2026-04-26, 21:46:56 UTC] {logging_mixin.py:190} INFO - Skipping San Francisco 2025-07-25: AirNow API error: 429 -> {"WebServiceError":[{"Message":"Web service request limit exceeded.  See web service documentation at www.airnowapi.org for details."}]}
    start_date = end_date - timedelta(days=29)

    for city in cities:
        lat = float(city["lat"])
        lon = float(city["lon"])
        city_name = city["name"]

        current_date = start_date

        while current_date <= end_date:
            start_str = current_date.strftime("%Y-%m-%dT00")
            end_str = current_date.strftime("%Y-%m-%dT23")

            try:
                data = get_airnow_historical(lat, lon, start_str, end_str)

                for row in data:
                    records.append({
                        "date": row.get("UTC"),
                        "latitude": row.get("Latitude"),
                        "longitude": row.get("Longitude"),
                        "city": city_name,
                        "parameter": row.get("Parameter"),
                        "aqi": row.get("AQI"),
                    })

            except Exception as e:
                print(f"Skipping {city_name} {current_date}: {e}")

            current_date += timedelta(days=1)

    return records


# @task
# def transform(records):
#     df = pd.DataFrame(records)

#     if df.empty:
#         return []

#     df["date"] = pd.to_datetime(df["date"]).dt.date

#     # find row with max AQI per day per city
#     idx = df.groupby(["date", "city"])["aqi"].idxmax()

#     result_df = df.loc[idx, ["date", "city", "parameter", "aqi"]]

#     return result_df.to_dict(orient="records")
@task
def transform(records):
    import pandas as pd

    df = pd.DataFrame(records)

    if df.empty:
        return []
    df["date"] = pd.to_datetime(df["date"]).dt.date

    df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")

    df_valid = df.dropna(subset=["aqi"])

    result_df = (
        df_valid
        .sort_values("aqi", ascending=False)
        .groupby(["date", "city"], as_index=False)
        .first()
    )

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
            parameter VARCHAR,
            aqi FLOAT
        )
        """)

        cur.execute(f"TRUNCATE TABLE {database}.{schema}.{table}")

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
        print(f"Loaded {len(data)} records into {table}")

    except Exception as e:
        cur.execute("ROLLBACK")
        raise e


with DAG(
    dag_id='AQIData_Historical',
    start_date=datetime(2023, 4, 1),
    catchup=False,
    schedule=None,
    tags=['ETL'],
    default_args=default_args,
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