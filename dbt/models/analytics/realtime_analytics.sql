-- models/analytics/realtime_analytics.sql

WITH base AS (
    SELECT *
    FROM {{ ref('historical_transform') }}
),

latest_day AS (
    SELECT MAX(date) AS latest_date
    FROM base
    WHERE date <= CURRENT_DATE()
)

SELECT b.*
FROM base b
JOIN latest_day l
    ON b.date = l.latest_date;
