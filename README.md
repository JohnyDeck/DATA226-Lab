# Weather Prediction Analytics

## DATA 226 Team Project

This project implements an end-to-end weather analytics pipeline using **Open-Meteo, Apache Airflow, Snowflake, dbt, and a BI tool**.

Weather data is collected from two cities through the Open-Meteo API, orchestrated by Apache Airflow, stored in Snowflake, transformed with dbt, and finally visualized through a BI dashboard.

---

## 1. System Architecture

```mermaid
flowchart LR
    A[Open-Meteo API] --> B[Airflow ETL DAG]
    B --> C[Snowflake RAW_WEATHER]
    C --> D[dbt Airflow DAG]
    D --> E[dbt Models]
    E --> F[Analytics Tables]
    F --> G[BI Dashboard]
```

The pipeline follows this data flow:

`Open-Meteo API`  
→ `Airflow ETL`  
→ `Snowflake RAW_WEATHER`  
→ `dbt Transformation`  
→ `Analytics Tables`  
→ `BI Dashboard`

---

## 2. Technology Stack

| Component | Technology |
|---|---|
| Weather Data Source | Open-Meteo API |
| Workflow Orchestration | Apache Airflow |
| Data Warehouse | Snowflake |
| Data Transformation | dbt |
| Visualization | BI Tool |
| Local Environment | Docker Compose |
| Version Control | GitHub |

---

## 3. Team Members and Responsibilities

### Member A — Xinghan Chen

Responsible for the ingestion and ETL layer:

- Open-Meteo API integration
- Weather extraction for two cities
- Apache Airflow ETL DAG
- Airflow Connections and Variables
- Snowflake raw table design
- SQL transaction handling
- Exception handling with rollback and raise
- Idempotent loading using Snowflake `MERGE`
- ETL validation
- Airflow and Snowflake screenshots
- ETL/database sections of the report

### Member B — Abhijith Reddy

Responsible for the transformation and visualization layer:

- dbt project
- dbt models
- dbt tests
- dbt snapshot
- Weather analytical metrics
- dbt Airflow DAG
- BI dashboard
- Dashboard filters
- dbt and BI screenshots
- dbt/BI sections of the report

---

## 4. Repository Structure

```text
DATA226-Lab/
├── dags/
│   ├── weather_etl_dag.py
│   └── weather_dbt_dag.py          # Added by Member B
│
├── dbt/
│   └── weather_analytics/           # Added by Member B
│
├── docs/
│   ├── A_TASK_CHECKLIST.md
│   └── SNOWFLAKE_CONNECTION_SETUP.md
│
├── report/
│   └── A_report_draft.md
│
├── screenshots/
│   ├── airflow_etl/
│   ├── dbt/
│   └── dashboard/
│
├── sql/
│   └── create_raw_weather.sql
│
├── airflow_variables.example.json
├── docker-compose.yaml
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 5. Weather Data Source

Weather data is retrieved from the Open-Meteo Forecast API:

`https://api.open-meteo.com/v1/forecast`

The current implementation collects hourly weather data for:

- Seattle
- Phoenix

The cities are configured through the Airflow variable:

`WEATHER_CITIES`

The current ETL configuration retrieves:

- 14 past days
- 7 forecast days
- Hourly weather records

---

## 6. Raw Weather Table

The ETL pipeline loads data into:

`WEATHER_ANALYTICS.RAW.RAW_WEATHER`

### Table Schema

| Column | Type | Description |
|---|---|---|
| `CITY` | VARCHAR | City name |
| `LATITUDE` | FLOAT | Latitude |
| `LONGITUDE` | FLOAT | Longitude |
| `WEATHER_TIME` | TIMESTAMP_NTZ | Hourly weather timestamp |
| `TEMPERATURE_C` | FLOAT | Temperature in Celsius |
| `RELATIVE_HUMIDITY_PCT` | FLOAT | Relative humidity |
| `PRECIPITATION_MM` | FLOAT | Total precipitation |
| `RAIN_MM` | FLOAT | Rainfall |
| `WIND_SPEED_KMH` | FLOAT | Wind speed |
| `WEATHER_CODE` | INTEGER | Open-Meteo weather code |
| `LOADED_AT` | TIMESTAMP_NTZ | Snowflake load timestamp |

The logical business key is:

`CITY + WEATHER_TIME`

This key is used during the Snowflake `MERGE` operation to prevent duplicate weather records.

---

## 7. Airflow ETL Pipeline

The main ingestion DAG is:

`weather_etl_dag`

The ETL flow is:

```text
extract_weather
      ↓
load_to_snowflake
      ↓
validate_raw_table
      ↓
should_trigger_dbt
      ↓
trigger_dbt_dag
```

### Task Description

`extract_weather` retrieves hourly weather data from Open-Meteo.

`load_to_snowflake` loads the extracted records into Snowflake.

`validate_raw_table` verifies that data from both cities exists and checks for duplicate business keys.

`should_trigger_dbt` controls whether the downstream dbt DAG should be triggered.

`trigger_dbt_dag` starts the dbt transformation pipeline after the ETL process completes.

---

## 8. Airflow Variables

The project uses Airflow Variables instead of hard-coding runtime configuration.

| Variable | Purpose |
|---|---|
| `OPEN_METEO_BASE_URL` | Open-Meteo API endpoint |
| `WEATHER_CITIES` | Two-city configuration |
| `OPEN_METEO_PAST_DAYS` | Number of historical days |
| `OPEN_METEO_FORECAST_DAYS` | Number of forecast days |
| `SNOWFLAKE_CONN_ID` | Airflow Snowflake connection ID |
| `TRIGGER_DBT` | Enables downstream dbt DAG |
| `DBT_DAG_ID` | dbt Airflow DAG ID |

An example configuration is provided in:

`airflow_variables.example.json`

---

## 9. Snowflake Connection

The Airflow Snowflake connection ID is:

`snowflake_weather`

The current implementation uses **RSA key-pair authentication** instead of storing a Snowflake password inside the project.

Typical configuration:

```text
Login: AIRFLOW_USER
Warehouse: COMPUTE_WH
Database: WEATHER_ANALYTICS
Schema: RAW
Role: ACCOUNTADMIN
Private Key Path: /opt/airflow/keys/rsa_key.pem
```

Private keys and credentials are excluded from Git through `.gitignore`.

No passwords, private keys, or authentication tokens should be committed to this repository.

---

## 10. Transaction and Idempotency

The loading process is designed to be idempotent.

Incoming weather records are first loaded into a temporary Snowflake staging table. The final update is performed inside an explicit SQL transaction.

The transaction follows the pattern:

```sql
BEGIN;

MERGE INTO RAW_WEATHER ...

COMMIT;
```

If an exception occurs, the Python task executes:

```text
ROLLBACK
→ raise exception
→ Airflow task marked as failed
```

The Snowflake `MERGE` uses:

`CITY + WEATHER_TIME`

as the matching condition.

Existing records are updated while only new records are inserted.

---

## 11. ETL Validation Results

A successful pipeline execution produced:

| City | Records |
|---|---:|
| Phoenix | 504 |
| Seattle | 504 |
| **Total** | **1008** |

The loaded weather period was:

`2026-09-16 00:00:00`  
to  
`2026-10-06 23:00:00`

Idempotency validation produced:

```text
TOTAL_ROWS  = 1008
UNIQUE_KEYS = 1008
```

A separate duplicate-key query using:

```sql
GROUP BY CITY, WEATHER_TIME
HAVING COUNT(*) > 1;
```

returned no results.

This confirms that repeated execution of the ETL DAG does not create duplicate logical weather records.

---

## 12. Running the Airflow Environment

### Start Airflow

From the project root:

```bash
docker compose up airflow-init
docker compose up -d
```

Check the containers:

```bash
docker compose ps
```

Airflow is available locally at:

`http://localhost:8080`

### Create the Snowflake Table

Run:

`sql/create_raw_weather.sql`

in Snowflake before executing the ETL pipeline.

### Configure Airflow

Create the Snowflake connection:

`Connection ID: snowflake_weather`

Then create the Airflow Variables using:

`airflow_variables.example.json`

### Run the ETL DAG

Trigger:

`weather_etl_dag`

from the Airflow Web UI.

---

## 13. Screenshots

Airflow and Snowflake validation screenshots are stored under:

`screenshots/airflow_etl/`

Current evidence includes:

```text
01_airflow_variables.png
02_airflow_etl_graph_success.png
03_snowflake_connection.png
04_snowflake_two_cities_summary.png
05_idempotency_total_vs_unique_keys.png
06_duplicate_check_no_results.png
```

Additional dbt and BI screenshots will be stored under:

```text
screenshots/dbt/
screenshots/dashboard/
```

---

## 14. dbt Transformation

The dbt layer will use:

`WEATHER_ANALYTICS.RAW.RAW_WEATHER`

as its source table.

The dbt project will implement:

- staging models
- analytical models
- tests
- snapshot
- weather metrics such as moving averages, temperature anomaly, and rolling rainfall

The dbt project will also be executed through a separate Airflow DAG:

`weather_dbt_dag`

The ETL DAG will trigger the dbt DAG after successful ingestion.

---

## 15. BI Dashboard

The BI layer will visualize the metrics generated by dbt.

The final dashboard will include interactive weather analytics and filtering, including city and/or date-range filtering.

At least two dashboard screenshots will be included in the final submission to demonstrate the dashboard in action.

---

## 16. Security Notes

Sensitive information must never be committed to GitHub.

The repository excludes:

```text
keys/
*.pem
*.key
.env
logs/
airflow.db
```

Snowflake authentication uses an RSA private key stored locally and mounted into the Airflow container at runtime.

---

## 17. Current Project Status

### Completed

- Open-Meteo API integration
- Two-city weather extraction
- Airflow ETL DAG
- Airflow Variables
- Airflow Snowflake Connection
- RSA key-pair authentication
- Snowflake RAW table
- Transaction handling
- Idempotent `MERGE`
- ETL validation
- Duplicate validation
- Airflow/Snowflake screenshots

### Remaining Integration

- dbt models
- dbt tests
- dbt snapshot
- dbt Airflow DAG
- BI dashboard
- BI screenshots
- Final end-to-end DAG integration

---

## 18. Final Integration

Once the dbt DAG is available, change:

```text
TRIGGER_DBT=false
```

to:

```text
TRIGGER_DBT=true
```

The final workflow will then become:

```text
Open-Meteo
    ↓
weather_etl_dag
    ↓
RAW_WEATHER
    ↓
weather_dbt_dag
    ↓
dbt transformations
    ↓
analytics tables
    ↓
BI dashboard
```
