WITH weather_base AS (
    SELECT
        city,
        date,
        LATITUDE,
        LONGITUDE,
        YEAR(date) AS year,
        MONTH(date) AS month,
        DAY(date) AS day,
        temp_max,
        temp_mean,
        temp_min,
        apparent_temp_max,
        apparent_temp_mean,
        apparent_temp_min,
        precipitation_sum,
        rain_sum,
        precipitation_hours,
        precipitation_probability_max,
        precipitation_probability_mean,
        precipitation_probability_min,
        weather_code,
        uv_index_max,
        uv_index_clear_sky_max,
        wind_speed_10m_max,

        CASE
            WHEN MONTH(date) IN (12, 1, 2) THEN 'Winter'
            WHEN MONTH(date) IN (3, 4, 5)  THEN 'Spring'
            WHEN MONTH(date) IN (6, 7, 8)  THEN 'Summer'
            ELSE 'Fall'
        END AS season,

        CASE WHEN precipitation_sum > 0 THEN 1 ELSE 0 END AS is_rainy_day,
        CASE WHEN precipitation_sum >= 10 THEN 1 ELSE 0 END AS is_heavy_rain_day,

        -- Comfort score based on weather qualities that are used for wildfire risk
        GREATEST(0, LEAST(100,
            100
            - GREATEST(0, temp_mean - 25) * 3
            - GREATEST(0, 15 - temp_mean) * 3
            - wind_speed_10m_max * 0.5
            - CASE WHEN precipitation_sum > 0 THEN 10 ELSE 0 END
        )) AS comfort_score

    FROM {{ source('raw', 'weather_data_proj') }}
    WHERE temp_mean IS NOT NULL
        AND precipitation_sum IS NOT NULL
),

aqi_base AS (
    SELECT
        city,
        date,
        AVG(AQI) AS avg_aqi,
        site_name,
        parameter,
        CASE
            WHEN AVG(AQI) <= 50 THEN 'Good'
            WHEN AVG(AQI) <= 100 THEN 'Moderate'
            WHEN AVG(AQI) <= 150 THEN 'Unhealthy for Sensitive Groups'
            WHEN AVG(AQI) <= 200 THEN 'Unhealthy'
            WHEN AVG(AQI) <= 300 THEN 'Very Unhealthy'
            ELSE 'Hazardous'
        END AS aqi_label
    FROM {{ source('raw', 'aqi_data_proj') }}
    GROUP BY city, date, site_name, parameter
),

fire_base AS (
    SELECT
        city,
        date,
        incident_count,
        total_acres,
        avg_acres,
        max_acres,
        most_common_incident,
        most_common_agency,
        most_common_source,
        -- Wildfire risk score
        LEAST(100, 
            incident_count * 2
            + CASE WHEN total_acres > 50 THEN 20 ELSE 0 END
            + CASE WHEN max_acres > 20 THEN 10 ELSE 0 END
        ) AS wildfire_risk_score
    FROM {{ source('raw', 'nifc_fire_proj') }}
)

SELECT
    w.*,
    a.avg_aqi,
    a.site_name,
    a.parameter,
    a.aqi_label,
    f.incident_count,
    f.total_acres,
    f.avg_acres,
    f.max_acres,
    f.most_common_incident,
    f.most_common_agency,
    f.most_common_source,
    f.wildfire_risk_score,

    -- Weather type range based on precipitation and UV
    CASE
        WHEN w.is_heavy_rain_day = 1 THEN 'Heavy Rain'
        WHEN w.is_rainy_day = 1 THEN 'Rain'
        WHEN w.uv_index_clear_sky_max >= 7 THEN 'Sunny'
        ELSE 'Clear Skies'
    END AS weather_type

FROM weather_base w
LEFT JOIN aqi_base a
    ON w.city = a.city AND w.date = a.date
LEFT JOIN fire_base f
    ON w.city = f.city AND w.date = f.date