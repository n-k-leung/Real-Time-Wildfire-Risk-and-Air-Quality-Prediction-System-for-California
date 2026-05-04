-- models/analytics/historical_analytics.sql
-- One row per city + date for time-series exploration (season, weather, AQI, wildfire).

WITH base AS (
    SELECT *
    FROM {{ ref('historical_transform') }}
),

enriched AS (
    SELECT
        b.*,

        CASE WHEN b.uv_index_max >= 6 THEN 1 ELSE 0 END AS abnormal_day_uv,
        CASE WHEN b.temp_max >= 32 THEN 1 ELSE 0 END AS abnormal_day_temp_max,
        CASE WHEN b.wind_speed_10m_max >= 30 THEN 1 ELSE 0 END AS abnormal_day_wind,

        CASE WHEN b.temp_max >= 35 THEN 1 ELSE 0 END AS heat_wave_day,
        CASE WHEN b.wind_speed_10m_max >= 40 THEN 1 ELSE 0 END AS high_wind_day,
        CASE WHEN b.uv_index_max >= 8 THEN 1 ELSE 0 END AS high_uv_day,
        CASE WHEN COALESCE(b.incident_count, 0) >= 5 THEN 1 ELSE 0 END AS high_incident_day,
        CASE WHEN COALESCE(b.total_acres, 0) >= 100 THEN 1 ELSE 0 END AS high_wildfire_area_day,

        SUM(COALESCE(b.incident_count, 0)) OVER (
            PARTITION BY b.city
            ORDER BY b.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_incidents,

        SUM(COALESCE(b.total_acres, 0)) OVER (
            PARTITION BY b.city
            ORDER BY b.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_wildfire_area,

        CASE
            WHEN COALESCE(b.incident_count, 0) >= 5 OR COALESCE(b.total_acres, 0) >= 100 THEN 'High'
            WHEN COALESCE(b.incident_count, 0) >= 2 OR COALESCE(b.total_acres, 0) >= 50 THEN 'Medium'
            ELSE 'Lower'
        END AS daily_impact_tier
    FROM base b
)

SELECT
    e.city,
    e.date,
    e.year,
    e.month,
    e.season,

    e.temp_max,
    e.temp_mean,
    e.temp_min,
    e.wind_speed_10m_max,
    e.uv_index_max,
    e.precipitation_sum,
    e.weather_type,
    e.weather_code,
    e.comfort_score,

    e.avg_aqi,
    e.parameter AS aqi_parameter,
    e.aqi_label,
    e.site_name,

    e.incident_count,
    e.total_acres,
    e.avg_acres,
    e.max_acres,
    e.most_common_incident,
    e.most_common_agency,
    e.most_common_source,
    e.wildfire_risk_score,

    e.abnormal_day_uv,
    e.abnormal_day_temp_max,
    e.abnormal_day_wind,
    e.heat_wave_day,
    e.high_wind_day,
    e.high_uv_day,
    e.high_incident_day,
    e.high_wildfire_area_day,

    e.cumulative_incidents,
    e.cumulative_wildfire_area,
    e.daily_impact_tier,

    CASE
        WHEN e.cumulative_incidents >= 100 OR e.cumulative_wildfire_area >= 5000 THEN 'High Impact'
        WHEN e.cumulative_incidents >= 50 OR e.cumulative_wildfire_area >= 2000 THEN 'Medium Impact'
        ELSE 'Lower Impact'
    END AS cumulative_high_impact_area
FROM enriched e
ORDER BY e.city, e.date
