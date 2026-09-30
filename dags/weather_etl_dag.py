from __future__ import annotations

from datetime import timedelta
from typing import Any

import pendulum
import requests

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


DEFAULT_CITIES = [
    {"city": "Seattle", "latitude": 47.6062, "longitude": -122.3321},
    {"city": "Phoenix", "latitude": 33.4484, "longitude": -112.0740},
]

HOURLY_FIELDS = [
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "wind_speed_10m",
    "weather_code",
]


@dag(
    dag_id="weather_etl_dag",
    description="Extract two-city weather data from Open-Meteo and load it idempotently into Snowflake.",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "team_weather",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["weather", "open-meteo", "snowflake", "etl"],
)
def weather_etl_dag():
    @task()
    def extract_weather() -> list[dict[str, Any]]:
        """
        Fetch hourly weather for two cities from Open-Meteo.

        Airflow Variables used:
          - OPEN_METEO_BASE_URL
          - WEATHER_CITIES (JSON)
          - OPEN_METEO_PAST_DAYS
          - OPEN_METEO_FORECAST_DAYS
        """
        base_url = Variable.get(
            "OPEN_METEO_BASE_URL",
            default_var="https://api.open-meteo.com/v1/forecast",
        )
        cities = Variable.get(
            "WEATHER_CITIES",
            default_var=DEFAULT_CITIES,
            deserialize_json=True,
        )
        past_days = int(Variable.get("OPEN_METEO_PAST_DAYS", default_var="14"))
        forecast_days = int(Variable.get("OPEN_METEO_FORECAST_DAYS", default_var="7"))

        if not isinstance(cities, list) or len(cities) != 2:
            raise AirflowException(
                "WEATHER_CITIES must be a JSON list containing exactly two city objects."
            )

        all_records: list[dict[str, Any]] = []

        for city_cfg in cities:
            city = city_cfg["city"]
            latitude = float(city_cfg["latitude"])
            longitude = float(city_cfg["longitude"])

            params = {
                "latitude": latitude,
                "longitude": longitude,
                "hourly": ",".join(HOURLY_FIELDS),
                "timezone": "auto",
                "past_days": past_days,
                "forecast_days": forecast_days,
            }

            response = requests.get(base_url, params=params, timeout=45)
            response.raise_for_status()
            payload = response.json()

            if "hourly" not in payload or "time" not in payload["hourly"]:
                raise AirflowException(
                    f"Open-Meteo response for {city} does not contain expected hourly data."
                )

            hourly = payload["hourly"]
            n = len(hourly["time"])

            for field in HOURLY_FIELDS:
                if field not in hourly:
                    raise AirflowException(
                        f"Missing field '{field}' in Open-Meteo response for {city}."
                    )
                if len(hourly[field]) != n:
                    raise AirflowException(
                        f"Length mismatch for '{field}' in Open-Meteo response for {city}."
                    )

            for i in range(n):
                all_records.append(
                    {
                        "city": city,
                        "latitude": float(payload.get("latitude", latitude)),
                        "longitude": float(payload.get("longitude", longitude)),
                        "weather_time": hourly["time"][i],
                        "temperature_c": hourly["temperature_2m"][i],
                        "relative_humidity_pct": hourly["relative_humidity_2m"][i],
                        "precipitation_mm": hourly["precipitation"][i],
                        "rain_mm": hourly["rain"][i],
                        "wind_speed_kmh": hourly["wind_speed_10m"][i],
                        "weather_code": hourly["weather_code"][i],
                    }
                )

        if not all_records:
            raise AirflowException("Open-Meteo returned zero weather records.")

        return all_records

    @task()
    def load_to_snowflake(records: list[dict[str, Any]]) -> dict[str, int]:
        """
        Load to Snowflake with an idempotent MERGE.

        Idempotency key:
            (CITY, WEATHER_TIME)

        Transaction behavior:
          1. Load incoming rows into a temporary staging table.
          2. BEGIN transaction.
          3. MERGE staging rows into RAW_WEATHER.
          4. COMMIT on success.
          5. ROLLBACK and raise on failure.
        """
        if not records:
            raise AirflowException("No records were passed to the Snowflake loader.")

        conn_id = Variable.get(
            "SNOWFLAKE_CONN_ID",
            default_var="snowflake_weather",
        )
        hook = SnowflakeHook(snowflake_conn_id=conn_id)
        conn = hook.get_conn()
        cursor = conn.cursor()

        create_stage_sql = """
        CREATE OR REPLACE TEMPORARY TABLE WEATHER_RAW_STAGE (
            CITY VARCHAR(100) NOT NULL,
            LATITUDE FLOAT,
            LONGITUDE FLOAT,
            WEATHER_TIME TIMESTAMP_NTZ NOT NULL,
            TEMPERATURE_C FLOAT,
            RELATIVE_HUMIDITY_PCT FLOAT,
            PRECIPITATION_MM FLOAT,
            RAIN_MM FLOAT,
            WIND_SPEED_KMH FLOAT,
            WEATHER_CODE INTEGER
        )
        """

        insert_stage_sql = """
        INSERT INTO WEATHER_RAW_STAGE (
            CITY,
            LATITUDE,
            LONGITUDE,
            WEATHER_TIME,
            TEMPERATURE_C,
            RELATIVE_HUMIDITY_PCT,
            PRECIPITATION_MM,
            RAIN_MM,
            WIND_SPEED_KMH,
            WEATHER_CODE
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        merge_sql = """
        MERGE INTO RAW_WEATHER AS T
        USING WEATHER_RAW_STAGE AS S
            ON T.CITY = S.CITY
           AND T.WEATHER_TIME = S.WEATHER_TIME

        WHEN MATCHED THEN UPDATE SET
            T.LATITUDE = S.LATITUDE,
            T.LONGITUDE = S.LONGITUDE,
            T.TEMPERATURE_C = S.TEMPERATURE_C,
            T.RELATIVE_HUMIDITY_PCT = S.RELATIVE_HUMIDITY_PCT,
            T.PRECIPITATION_MM = S.PRECIPITATION_MM,
            T.RAIN_MM = S.RAIN_MM,
            T.WIND_SPEED_KMH = S.WIND_SPEED_KMH,
            T.WEATHER_CODE = S.WEATHER_CODE,
            T.LOADED_AT = CURRENT_TIMESTAMP()

        WHEN NOT MATCHED THEN INSERT (
            CITY,
            LATITUDE,
            LONGITUDE,
            WEATHER_TIME,
            TEMPERATURE_C,
            RELATIVE_HUMIDITY_PCT,
            PRECIPITATION_MM,
            RAIN_MM,
            WIND_SPEED_KMH,
            WEATHER_CODE,
            LOADED_AT
        )
        VALUES (
            S.CITY,
            S.LATITUDE,
            S.LONGITUDE,
            S.WEATHER_TIME,
            S.TEMPERATURE_C,
            S.RELATIVE_HUMIDITY_PCT,
            S.PRECIPITATION_MM,
            S.RAIN_MM,
            S.WIND_SPEED_KMH,
            S.WEATHER_CODE,
            CURRENT_TIMESTAMP()
        )
        """

        values = [
            (
                r["city"],
                r["latitude"],
                r["longitude"],
                r["weather_time"],
                r["temperature_c"],
                r["relative_humidity_pct"],
                r["precipitation_mm"],
                r["rain_mm"],
                r["wind_speed_kmh"],
                r["weather_code"],
            )
            for r in records
        ]

        try:
            # DDL is intentionally executed before BEGIN because Snowflake DDL can
            # implicitly commit. The target table must already exist via sql/create_raw_weather.sql.
            cursor.execute(create_stage_sql)
            cursor.executemany(insert_stage_sql, values)

            cursor.execute("BEGIN")
            cursor.execute(merge_sql)
            cursor.execute("COMMIT")

        except Exception:
            try:
                cursor.execute("ROLLBACK")
            finally:
                cursor.close()
                conn.close()
            raise

        cursor.close()
        conn.close()

        return {"incoming_records": len(records)}

    @task()
    def validate_raw_table(load_info: dict[str, int]) -> dict[str, int]:
        """
        Validate that the target contains no duplicate business keys.
        """
        conn_id = Variable.get(
            "SNOWFLAKE_CONN_ID",
            default_var="snowflake_weather",
        )
        hook = SnowflakeHook(snowflake_conn_id=conn_id)

        duplicate_sql = """
        SELECT COUNT(*)
        FROM (
            SELECT CITY, WEATHER_TIME, COUNT(*) AS C
            FROM RAW_WEATHER
            GROUP BY CITY, WEATHER_TIME
            HAVING COUNT(*) > 1
        )
        """

        city_count_sql = """
        SELECT COUNT(DISTINCT CITY)
        FROM RAW_WEATHER
        """

        duplicate_rows = int(hook.get_first(duplicate_sql)[0])
        city_count = int(hook.get_first(city_count_sql)[0])

        if duplicate_rows != 0:
            raise AirflowException(
                f"Idempotency validation failed: found {duplicate_rows} duplicate keys."
            )
        if city_count < 2:
            raise AirflowException(
                f"Expected data for at least two cities, found {city_count}."
            )

        return {
            "incoming_records": int(load_info["incoming_records"]),
            "duplicate_keys": duplicate_rows,
            "distinct_cities": city_count,
        }

    @task.short_circuit()
    def should_trigger_dbt() -> bool:
        """
        Keep False while teammate B is developing.
        Set Airflow Variable TRIGGER_DBT=true for final integration.
        """
        return (
            Variable.get("TRIGGER_DBT", default_var="false")
            .strip()
            .lower()
            == "true"
        )

    extracted = extract_weather()
    loaded = load_to_snowflake(extracted)
    validated = validate_raw_table(loaded)
    trigger_enabled = should_trigger_dbt()

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id=Variable.get(
            "DBT_DAG_ID",
            default_var="weather_dbt_dag",
        ),
        wait_for_completion=False,
        reset_dag_run=False,
    )

    validated >> trigger_enabled >> trigger_dbt


weather_etl_dag()
