# Member A Report Draft

## 1. Problem Statement

The project builds a small end-to-end weather analytics pipeline that retrieves weather data for two cities from the Open-Meteo API, orchestrates extraction and loading with Apache Airflow, stores raw data in Snowflake, and provides a stable raw-data interface for downstream dbt transformations and BI visualization.

## 2. Requirements

The A-side pipeline must collect weather information for exactly two cities, execute as an Airflow DAG, use Airflow Connections and Variables appropriately, load the data into Snowflake, and support safe repeated execution. The loading process must use an explicit SQL transaction together with exception handling so that failures are rolled back and repeated runs do not create duplicate business records.

## 3. System Specifications

The data source is the Open-Meteo forecast endpoint. The orchestration layer is Apache Airflow. Snowflake is used as the cloud data warehouse. The ETL process stores hourly observations/forecast values in a raw table. The downstream boundary is the Snowflake table `WEATHER_ANALYTICS.RAW.RAW_WEATHER`.

## 4. Overall System Architecture

Open-Meteo API
→ Airflow `weather_etl_dag`
→ Snowflake `RAW_WEATHER`
→ Airflow triggers `weather_dbt_dag`
→ dbt analytics models
→ BI dashboard

Member A owns the pipeline up to and including `RAW_WEATHER`. Member B owns the transformation and visualization stages after that table.

## 5. Data Source

The pipeline requests hourly weather variables for Seattle and Phoenix. The fields include temperature, relative humidity, precipitation, rain, wind speed, and weather code. The API URL and city configuration are stored as Airflow Variables rather than hard-coded operational secrets.

## 6. RAW_WEATHER Table Structure

| Field | Type | Constraint / role |
|---|---|---|
| CITY | VARCHAR(100) | NOT NULL; business key part 1 |
| LATITUDE | FLOAT | location metadata |
| LONGITUDE | FLOAT | location metadata |
| WEATHER_TIME | TIMESTAMP_NTZ | NOT NULL; business key part 2 |
| TEMPERATURE_C | FLOAT | hourly temperature |
| RELATIVE_HUMIDITY_PCT | FLOAT | hourly relative humidity |
| PRECIPITATION_MM | FLOAT | hourly total precipitation |
| RAIN_MM | FLOAT | hourly rain |
| WIND_SPEED_KMH | FLOAT | hourly 10 m wind speed |
| WEATHER_CODE | INTEGER | weather condition code |
| LOADED_AT | TIMESTAMP_NTZ | load audit timestamp |

The logical uniqueness key is `(CITY, WEATHER_TIME)`. The ETL does not rely on the database constraint alone for deduplication. Instead, the load procedure uses an idempotent `MERGE`.

## 7. Airflow ETL Pipeline

The Airflow DAG extracts data for the two configured cities, converts each hourly API result to a common record structure, loads the records into Snowflake, validates the raw table, and can then trigger the dbt DAG. Retries are configured on the DAG so temporary API or infrastructure failures can be retried.

## 8. Airflow Connections and Variables

The Snowflake account configuration is stored in an Airflow Connection with the connection ID `snowflake_weather`. Airflow Variables are used for the Open-Meteo URL, two-city configuration, past/forecast day window, the Snowflake connection ID, and the downstream dbt DAG ID.

This separation avoids hard-coding runtime configuration inside the DAG source code.

## 9. Transaction and Idempotency

Each ETL run first loads incoming records into a temporary Snowflake staging table. After staging succeeds, the target update is executed inside an explicit transaction. A `MERGE` uses `(CITY, WEATHER_TIME)` as the match condition. Existing records are updated and new records are inserted.

If the target operation succeeds, the transaction is committed. If any exception occurs, the code executes `ROLLBACK` and re-raises the exception so Airflow marks the task as failed. Therefore, rerunning the same time range does not create duplicate logical records and partial target-table updates are avoided.

## 10. Evidence to Include

The final report should insert:
- Airflow Graph/Grid screenshot
- Airflow Variables screenshot
- Airflow Connection screenshot with secrets hidden
- Snowflake two-city data query
- duplicate-key validation result
- optional second successful run demonstrating idempotency
