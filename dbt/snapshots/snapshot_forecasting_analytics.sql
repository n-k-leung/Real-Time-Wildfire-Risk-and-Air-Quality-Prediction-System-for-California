{% snapshot snapshot_forecasting_analytics %}
{{
    config(
        target_schema='snapshot',
        unique_key="city || '-' || CAST(date AS VARCHAR) || '-' || COALESCE(aqi_parameter, 'UNKNOWN')",
        strategy='timestamp',
        updated_at='updated_at_ts',
        invalidate_hard_deletes=True
    )
}}

SELECT
    *,
    CAST(date AS TIMESTAMP_NTZ) AS updated_at_ts
FROM {{ ref('forecast_analytics') }}

{% endsnapshot %}