-- Historical: California Fire Perimeters
CREATE DATABASE IF NOT EXISTS USER_DB_CHEETAH;
CREATE SCHEMA IF NOT EXISTS USER_DB_CHEETAH.raw;
CREATE SCHEMA IF NOT EXISTS USER_DB_CHEETAH.staging;

CREATE OR REPLACE TABLE USER_DB_CHEETAH.raw.ca_fire_perimeters (
    objectid          NUMBER,
    year_             NUMBER,
    state             VARCHAR,
    agency            VARCHAR,
    unit_id           VARCHAR,
    fire_name         VARCHAR,
    inc_num           VARCHAR,
    alarm_date        TIMESTAMP_NTZ,
    cont_date         TIMESTAMP_NTZ,
    cause             NUMBER,
    c_method          NUMBER,
    objective         NUMBER,
    gis_acres         FLOAT,
    comments          VARCHAR,
    complex_name      VARCHAR,
    complex_id        VARCHAR,
    irwinid           VARCHAR,
    fire_num          VARCHAR,
    geom_wkt          VARCHAR,           -- geometry as WKT string
    loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Incremental: NASA FIRMS Active Fire Data
CREATE OR REPLACE TABLE USER_DB_CHEETAH.raw.nasa_firms_viirs (
    latitude          FLOAT,
    longitude         FLOAT,
    bright_ti4        FLOAT,
    scan              FLOAT,
    track             FLOAT,
    acq_date          DATE,
    acq_time          VARCHAR,
    satellite         VARCHAR,
    instrument        VARCHAR,
    confidence        VARCHAR,
    version           VARCHAR,
    bright_ti5        FLOAT,
    frp               FLOAT,
    daynight          VARCHAR,
    -- Surrogate key for deduplication
    record_id         VARCHAR AS (
                          MD5(latitude::VARCHAR || longitude::VARCHAR
                              || acq_date::VARCHAR || acq_time)
                      ),
    loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Staging / analytics-ready view
CREATE OR REPLACE VIEW USER_DB_CHEETAH.staging.ca_fire_perimeters_clean AS
SELECT
    objectid,
    year_                                  AS fire_year,
    UPPER(state)                           AS state,
    agency,
    fire_name,
    alarm_date::DATE                       AS alarm_date,
    cont_date::DATE                        AS containment_date,
    DATEDIFF('day', alarm_date, cont_date) AS duration_days,
    gis_acres,
    geom_wkt
FROM USER_DB_CHEETAH.raw.ca_fire_perimeters
WHERE year_ IS NOT NULL;
