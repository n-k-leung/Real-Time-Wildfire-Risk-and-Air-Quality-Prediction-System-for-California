{% snapshot snapshot_precipitation_analysis %}
{{
    config(
        target_schema='snapshot',
        unique_key="city || '-' || CAST(date AS VARCHAR)",
        strategy='timestamp',
        updated_at='date',
        invalidate_hard_deletes=True
    )
}}
-- did cast because warning for:  Data type of snapshot table timestamp columns (TIMESTAMP_NTZ) doesn't match derived column 'updated_at' (DATE). Please update snapshot config 'updated_at'.
SELECT
    *,
    CAST(date AS TIMESTAMP_NTZ) AS updated_at_ts
FROM {{ ref('precipitation_analysis') }}
{% endsnapshot %}
