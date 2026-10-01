{{ config(materialized='table') }}

WITH daily AS (
    SELECT *
    FROM {{ ref('fct_weather_daily') }}
),

metrics AS (
    SELECT
        daily.*,

        AVG(AVG_TEMPERATURE_C) OVER (
            PARTITION BY CITY
        ) AS CITY_BASELINE_TEMPERATURE_C,

        AVG(AVG_TEMPERATURE_C) OVER (
            PARTITION BY CITY
            ORDER BY WEATHER_DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS TEMPERATURE_7_DAY_MOVING_AVG_C,

        SUM(TOTAL_RAIN_MM) OVER (
            PARTITION BY CITY
            ORDER BY WEATHER_DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS RAINFALL_7_DAY_ROLLING_MM

    FROM daily
)

SELECT
    *,
    AVG_TEMPERATURE_C - CITY_BASELINE_TEMPERATURE_C
        AS TEMPERATURE_ANOMALY_C
FROM metrics