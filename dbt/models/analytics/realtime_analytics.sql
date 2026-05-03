-- models/analytics/realtime_analytics.sql

WITH base AS (
    SELECT *
    FROM {{ ref('historical_transform') }}
),

latest_day AS (
    SELECT MAX(date) AS latest_date
    FROM base
    WHERE date <= CURRENT_DATE()
),

realtime AS (
    SELECT
        b.*,

        -- Simple AQI range
        CASE
            WHEN b.avg_aqi <= 50 THEN 'Good'
            WHEN b.avg_aqi <= 150 THEN 'Bad'
            WHEN b.avg_aqi > 150 THEN 'Terrible'
            ELSE 'Unknown'
        END AS aqi_range,

        -- Wildfire risk category
        CASE
            WHEN COALESCE(b.wildfire_risk_score, 0) < 30 THEN 'Low'
            WHEN COALESCE(b.wildfire_risk_score, 0) < 70 THEN 'Medium'
            ELSE 'High'
        END AS wildfire_risk,

        -- Weather type range
        CASE
            WHEN b.precipitation_sum >= 10 THEN 'Rainy'
            WHEN b.precipitation_sum > 0 THEN 'Rainy'
            WHEN b.uv_index_clear_sky_max >= 7 THEN 'Sunny'
            ELSE 'Clear Skies'
        END AS weather_type_range

    FROM base b
    JOIN latest_day l
        ON b.date = l.latest_date
)

SELECT *
FROM realtime
WHERE date = CURRENT_DATE();