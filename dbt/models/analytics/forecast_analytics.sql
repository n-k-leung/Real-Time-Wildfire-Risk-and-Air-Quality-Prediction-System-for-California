WITH base AS (
    SELECT *
    FROM {{ ref('forecast_transform') }}
),

enriched AS (
    SELECT
        b.*,

        -- Wildfire signals
        CASE WHEN COALESCE(b.incident_count, 0) >= 5 THEN 1 ELSE 0 END AS high_incident_day,
        CASE WHEN COALESCE(b.total_acres, 0) >= 100 THEN 1 ELSE 0 END AS high_wildfire_area_day,

        -- AQI signals
        CASE WHEN COALESCE(b.aqi, 0) > 100 THEN 1 ELSE 0 END AS high_aqi_day,

        -- Combined risk
        CASE
            WHEN COALESCE(b.wildfire_risk_score, 0) >= 75
             AND COALESCE(b.aqi, 0) > 150
            THEN 1 ELSE 0
        END AS extreme_risk_day,

        -- Impact classification
        CASE
            WHEN COALESCE(b.incident_count, 0) >= 5
              OR COALESCE(b.total_acres, 0) >= 100
              OR COALESCE(b.wildfire_risk_score, 0) >= 75
            THEN 'High'
            WHEN COALESCE(b.incident_count, 0) >= 2
              OR COALESCE(b.total_acres, 0) >= 50
              OR COALESCE(b.wildfire_risk_score, 0) >= 50
            THEN 'Medium'
            ELSE 'Low'
        END AS forecast_impact_tier

    FROM base b
),

-- KEY: parameter-aware aggregation
city_rollup AS (
    SELECT
        city,

        COUNT(*) AS total_rows,

        COUNT(DISTINCT date) AS forecast_days,

        -- wildfire
        SUM(high_incident_day) AS high_incident_days,
        SUM(high_wildfire_area_day) AS high_wildfire_area_days,

        -- AQI
        SUM(high_aqi_day) AS high_aqi_days,

        -- parameter insight
        COUNT(DISTINCT aqi_parameter) AS aqi_parameter_types,
        COUNT(*) AS days_in_aqi_parameter_types,

        -- wildfire source
        MAX(most_common_incident) AS wildfire_incident_source,

        -- impact classification
        CASE
            WHEN SUM(high_incident_day) >= 5
              OR SUM(high_wildfire_area_day) >= 5
              OR MAX(wildfire_risk_score) >= 75
            THEN 'High Impact Area'

            WHEN SUM(high_incident_day) >= 2
              OR SUM(high_aqi_day) >= 3
              OR MAX(wildfire_risk_score) >= 50
            THEN 'Medium Impact Area'

            ELSE 'Lower Impact Area'
        END AS area_impact_level

    FROM enriched
    GROUP BY city
)

SELECT
    e.city,
    e.date,
    e.season,

    -- wildfire
    e.incident_count,
    e.total_acres,
    e.max_acres,
    e.most_common_incident,
    e.most_common_agency,
    e.most_common_source,
    e.wildfire_risk_score,

    -- AQI
    e.aqi,
    e.aqi_parameter,
    e.aqi_range,

    -- flags
    e.high_incident_day,
    e.high_wildfire_area_day,
    e.high_aqi_day,
    e.extreme_risk_day,

    e.forecast_impact_tier,

    -- rollups
    c.forecast_days,
    c.high_incident_days,
    c.high_wildfire_area_days,
    c.high_aqi_days,
    c.aqi_parameter_types,
    c.days_in_aqi_parameter_types,
    c.wildfire_incident_source,
    c.area_impact_level

FROM enriched e
LEFT JOIN city_rollup c
    ON e.city = c.city