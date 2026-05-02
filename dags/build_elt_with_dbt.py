"""
dbt DAG for the weather analytics project (build_mau).
Runs after the WeatherData_ETL DAG loads fresh data into Snowflake each day.
Order: dbt run -> dbt test -> dbt snapshot
"""

from pendulum import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook


DBT_PROJECT_DIR = "/opt/airflow/dbt"


conn = BaseHook.get_connection('snowflake_con')
with DAG(
    "BuildELT_dbt_proj",
    start_date=datetime(2026, 2, 27),
    description="Runs dbt run, test, and snapshot for the project analytics project after ETL loads.",
    schedule="20 4 * * *",  # 15 min after forecastings
    catchup=False,
    default_args={
        "owner": "natleung",
        "email": ["natalie.leung@sjsu.com"],
        "retries": 1,
        "env": {
            "DBT_USER": conn.login,
            "DBT_PASSWORD": conn.password,
            "DBT_ACCOUNT": conn.extra_dejson.get("account"),
            "DBT_DATABASE": conn.extra_dejson.get("database"),
            "DBT_WAREHOUSE": conn.extra_dejson.get("warehouse"),
            "DBT_TYPE": "snowflake"
        }
    },
) as dag:
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"/home/airflow/.local/bin/dbt run --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"/home/airflow/.local/bin/dbt test --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"/home/airflow/.local/bin/dbt snapshot --profiles-dir {DBT_PROJECT_DIR} --project-dir {DBT_PROJECT_DIR}",
    )

    dbt_run >> dbt_test >> dbt_snapshot