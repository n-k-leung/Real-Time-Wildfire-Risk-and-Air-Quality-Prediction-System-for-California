WITH fire_forecast AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY city, date
                ORDER BY forecast_generated_at DESC
            ) AS fire_rn
        FROM {{ source('analytics', 'nifc_fire_forecast_updated') }}
    )
    WHERE fire_rn = 1
),

aqi_forecast AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY city, date, parameter
                ORDER BY forecast_generated_at DESC
            ) AS aqi_rn
        FROM {{ source('analytics', 'aqi_forecast_with_parameter') }}
    )
    WHERE aqi_rn = 1
),

joined AS (
    SELECT
        f.city,
        f.date,
        f.incident_count,
        f.total_acres,
        f.avg_acres,
        f.max_acres,
        f.most_common_incident,
        f.most_common_agency,
        f.most_common_source,
        f.forecast_generated_at AS wildfire_forecast_generated_at,

        a.aqi,
        a.parameter AS aqi_parameter,
        a.is_forecast AS aqi_is_forecast,
        a.forecast_generated_at AS aqi_forecast_generated_at,

        CASE
            WHEN MONTH(f.date) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(f.date) IN (3, 4, 5) THEN 'Spring'
            WHEN MONTH(f.date) IN (6, 7, 8) THEN 'Summer'
            ELSE 'Fall'
        END AS season,

        LEAST(
            100,
            COALESCE(f.incident_count, 0) * 2
            + CASE WHEN COALESCE(f.total_acres, 0) > 50 THEN 20 ELSE 0 END
            + CASE WHEN COALESCE(f.max_acres, 0) > 20 THEN 10 ELSE 0 END
        ) AS wildfire_risk_score,

        CASE
            WHEN a.aqi <= 50 THEN 'Good'
            WHEN a.aqi <= 100 THEN 'Bad'
            ELSE 'Terrible'
        END AS aqi_range

    FROM fire_forecast f
    LEFT JOIN aqi_forecast a
        ON f.city = a.city
       AND f.date = a.date
)

SELECT *
FROM joined