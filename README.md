# Weather Prediction Analytics — Team Project

## System flow

Open-Meteo API  
→ Airflow ETL (`weather_etl_dag`)  
→ Snowflake `WEATHER_ANALYTICS.RAW.RAW_WEATHER`  
→ teammate B's dbt DAG  
→ transformed analytics tables  
→ BI dashboard

## Team boundary

### Member A
- Open-Meteo extraction for two cities
- Airflow ETL DAG
- Airflow Connections and Variables
- Snowflake RAW table
- SQL transaction + exception handling + rollback/raise
- Idempotent loading
- ETL validation
- Airflow screenshots
- ETL/database/report sections

### Member B
- dbt project/models/tests/snapshot
- Analytical weather metrics
- dbt Airflow DAG
- BI dashboard and filters
- dbt and BI screenshots
- dbt/BI report sections

## Shared interface

The fixed interface is:

`WEATHER_ANALYTICS.RAW.RAW_WEATHER`

Do not rename its columns after B starts development.

## A-side setup

1. In Snowflake, run:
   `sql/create_raw_weather.sql`

2. In Airflow, create a Snowflake connection:
   - Connection ID: `snowflake_weather`
   - Account
   - Username
   - Password/authentication
   - Warehouse
   - Database: `WEATHER_ANALYTICS`
   - Schema: `RAW`
   - Role (if needed)

3. Create Airflow Variables using:
   `airflow_variables.example.json`

4. Install Python/provider dependencies from:
   `requirements.txt`

5. Put:
   `dags/weather_etl_dag.py`
   under Airflow's `dags/` directory.

6. Run the DAG once manually and verify:
   - both city extraction tasks succeed
   - Snowflake contains both cities
   - duplicate-key validation returns zero duplicates

7. Run the DAG again.
   The row count should not double for the same `(CITY, WEATHER_TIME)` values.
   The `MERGE` updates matched rows and inserts only new rows.

8. While B is still developing, keep:
   `TRIGGER_DBT=false`

9. For final integration, B creates:
   `weather_dbt_dag`
   and then set:
   `TRIGGER_DBT=true`

## Suggested screenshots for Member A

1. Airflow Graph/Grid view showing `weather_etl_dag` successful
2. Airflow Connection page showing the connection ID (do not expose the password)
3. Airflow Variables page showing variable names
4. Snowflake query result showing records for two cities
5. Snowflake duplicate validation query showing zero duplicate keys
6. Optional: second Airflow run showing success, used to explain idempotency

## Important

Do not commit:
- Snowflake passwords
- private keys
- tokens
- local Airflow metadata database
- logs
