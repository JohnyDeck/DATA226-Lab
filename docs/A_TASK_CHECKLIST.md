# Member A — Submission Checklist

## Code
- [ ] `dags/weather_etl_dag.py`
- [ ] Open-Meteo API extraction works for exactly two cities
- [ ] Snowflake connection uses an Airflow Connection
- [ ] Runtime configuration uses Airflow Variables
- [ ] SQL transaction is visible in code (`BEGIN`, `COMMIT`, `ROLLBACK`)
- [ ] Python exception handling re-raises errors
- [ ] Idempotency is implemented with `MERGE`
- [ ] Duplicate validation is included
- [ ] DAG can trigger B's dbt DAG after ETL for final integration

## Snowflake
- [ ] `RAW_WEATHER` table created
- [ ] Both cities present
- [ ] `(CITY, WEATHER_TIME)` is the business key
- [ ] Running ETL twice does not create duplicate keys

## Screenshots
- [ ] Airflow pipeline Graph/Grid view
- [ ] Successful DAG run
- [ ] Airflow Connection ID visible, secret hidden
- [ ] Airflow Variables visible
- [ ] Snowflake two-city data visible
- [ ] Duplicate check returns zero

## Report sections owned by A
- [ ] Problem statement
- [ ] Requirements
- [ ] System specifications
- [ ] Overall system architecture/data flow
- [ ] Data source
- [ ] Detailed RAW table structure
- [ ] Airflow ETL implementation
- [ ] Connections and Variables
- [ ] Transaction and idempotency
