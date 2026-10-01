{% snapshot snapshot_raw_weather %}

{{
    config(
        target_schema='DBT_DEV',
        unique_key="CITY || '|' || TO_VARCHAR(WEATHER_TIME)",
        strategy='timestamp',
        updated_at='LOADED_AT',
        invalidate_hard_deletes=True
    )
}}

SELECT
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
FROM {{ source('raw', 'RAW_WEATHER') }}

{% endsnapshot %}