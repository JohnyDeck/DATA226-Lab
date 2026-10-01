# Member B Report Draft

## 1. Role and Scope

Member B completed the downstream analytics and visualization stages after the raw weather data was loaded into Snowflake. This included dbt transformations, testing, snapshotting, Airflow integration, and Tableau dashboard development.

## 2. dbt Project Configuration

A dbt project named `weather_analytics` was created under the `dbt/` directory. The project uses the raw Snowflake table:

`WEATHER_ANALYTICS.RAW.RAW_WEATHER`

The dbt project is organized into staging models, mart models, tests, and snapshots.

## 3. Staging Model

The staging model `stg_raw_weather.sql` prepares the raw weather data for downstream analysis. It standardizes the weather date and time fields and provides a clean interface for the analytical models.

## 4. Analytical Models

Two mart models were created:

- `fct_weather_daily.sql` creates daily weather-level metrics.
- `mart_weather_metrics.sql` calculates analytical measures for Tableau, including:
  - Temperature anomaly
  - Seven-day moving average temperature
  - Seven-day rolling rainfall
  - Hourly observation counts

The models retain city and weather date fields so that the results can be filtered and analyzed by location and time.

## 5. Data Quality Testing

dbt schema tests were added to verify that important fields are not null. A custom test named `no_duplicate_weather_keys.sql` was also added to check for duplicate city and date combinations.

The dbt test execution completed successfully, as shown in:

`screenshots/dbt/dbt_dbt_test_success.png`

## 6. Snapshot

A dbt snapshot named `snapshot_raw_weather.sql` was created to preserve historical versions of the raw weather records. This supports tracking changes to weather data over time.

The successful snapshot execution is documented in:

`screenshots/dbt/dbt_dbt_snapshot_success.png`

## 7. Airflow and dbt Integration

The Airflow DAG `weather_dbt_dag.py` was added to trigger the dbt workflow after the weather data pipeline completes. This connects the raw-data loading stage with the downstream transformation process.

The Airflow DAG list and graph are documented in:

- `screenshots/dbt/airflow_dbt_airflow_dbt_dag_list.png`
- `screenshots/dbt/airflow_dbt_airflow_dbt_dag_graph.png`

## 8. Tableau Dashboard

A Tableau workbook named `weather_analytics_dashboard.twbx` was created in the `bi/` directory. The dashboard contains the following visualizations:

- Temperature Anomaly
- Rolling Rainfall
- Temperature Moving Average

The dashboard includes a city filter and a weather-date range filter. These controls allow users to examine the weather metrics for different cities and time periods.

The dashboard screenshots are:

- `screenshots/tableau_dashboard_all_cities.png`
- `screenshots/tableau_dashboard_seattle_filtered.png`

## 9. Results

The completed dbt models produced clean analytical data for the Tableau dashboard. The dashboard makes it possible to compare weather patterns, rainfall trends, temperature changes, and moving averages across locations and dates.

## 10. Conclusion

The downstream analytics layer successfully transformed raw Snowflake weather data into tested and reusable dbt models. Airflow was used to coordinate the workflow, while Tableau provided an interactive way to explore the final weather metrics.