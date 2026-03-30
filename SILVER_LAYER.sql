CREATE SCHEMA VALIDATION_SCHEMA;
USE SCHEMA VALIDATION_SCHEMA;

CREATE OR REPLACE TABLE VALIDATION_SCHEMA.STATIONS_VALID AS
WITH CLEANED AS (
    SELECT
        station_id,

        -- NULL handling
        TRIM(station_name) AS station_name,

        -- Fix latitude/longitude precision (6 decimal places)
        ROUND(latitude, 6) AS latitude,
        ROUND(longitude, 6) AS longitude,

        -- Capacity check (set invalid to NULL)
        CASE 
            WHEN capacity >= 0 THEN capacity
            ELSE NULL
        END AS capacity,

        neighborhood,

        -- Standardize city_zone (upper + trim)
        UPPER(TRIM(city_zone)) AS city_zone,

        install_date,

        -- Standardize status values
        CASE 
            WHEN LOWER(status) IN ('active', 'in_service') THEN 'ACTIVE'
            WHEN LOWER(status) IN ('inactive', 'out_of_service') THEN 'INACTIVE'
            ELSE 'UNKNOWN'
        END AS status,

        -- For deduplication
        ROW_NUMBER() OVER (
            PARTITION BY station_id 
            ORDER BY install_date DESC
        ) AS rn

    FROM RAW_SCHEMA.STATIONS
),

DEDUPED AS (
    SELECT *
    FROM CLEANED
    WHERE rn = 1
),

FINAL AS (
    SELECT
        station_id,
        station_name,
        latitude,
        longitude,
        capacity,
        neighborhood,
        city_zone,
        install_date,
        status,

        -- Data Quality Status
        CASE 
            WHEN station_id IS NULL THEN 'INVALID'
            WHEN station_name IS NULL THEN 'INVALID'
            WHEN latitude IS NULL OR longitude IS NULL THEN 'INVALID'
            WHEN capacity IS NULL THEN 'INVALID'
            ELSE 'VALID'
        END AS dq_status,

        -- Data Quality Reason
        CASE 
            WHEN station_id IS NULL THEN 'MISSING_ID'
            WHEN station_name IS NULL THEN 'MISSING_NAME'
            WHEN latitude IS NULL OR longitude IS NULL THEN 'INVALID_GEO'
            WHEN capacity IS NULL THEN 'INVALID_CAPACITY'
            ELSE 'OK'
        END AS dq_reason,

        CURRENT_TIMESTAMP() AS validated_at

    FROM DEDUPED
)

SELECT * FROM FINAL;



SELECT * FROM STATIONS_VALID;


CREATE OR REPLACE TABLE HACKATHON_BIKE_RIDES.VALIDATION_SCHEMA.BIKES_VALID AS

WITH CLEANED AS (
    SELECT
        bike_id,

        -- Standardize bike_type
        CASE 
            WHEN LOWER(TRIM(bike_type)) IN ('classic', 'regular') THEN 'CLASSIC'
            WHEN LOWER(TRIM(bike_type)) IN ('ebike', 'electric') THEN 'EBIKE'
            ELSE 'UNKNOWN'
        END AS bike_type,

        -- Standardize status
        CASE 
            WHEN LOWER(status) IN ('available', 'active', 'in_service') THEN 'ACTIVE'
            WHEN LOWER(status) IN ('inactive', 'out_of_service', 'repair') THEN 'INACTIVE'
            ELSE 'UNKNOWN'
        END AS status,

        purchase_date,
        last_service_date,

        -- Odometer validation
        CASE 
            WHEN odometer_km >= 0 THEN odometer_km
            ELSE NULL
        END AS odometer_km,

        -- Battery validation (only meaningful for ebikes)
        CASE 
            WHEN battery_level BETWEEN 0 AND 100 THEN battery_level
            ELSE NULL
        END AS battery_level,

        firmware_version,

        -- Deduplication logic
        ROW_NUMBER() OVER (
            PARTITION BY bike_id
            ORDER BY last_service_date DESC NULLS LAST
        ) AS rn

    FROM HACKATHON_BIKE_RIDES.RAW_SCHEMA.BIKES
),

DEDUPED AS (
    SELECT *
    FROM CLEANED
    WHERE rn = 1
),

FINAL AS (
    SELECT
        bike_id,
        bike_type,
        status,
        purchase_date,
        last_service_date,
        odometer_km,
        battery_level,
        firmware_version,

        -- =========================
        -- DATA QUALITY STATUS
        -- =========================
        CASE 
            WHEN bike_id IS NULL THEN 'INVALID'
            WHEN bike_type = 'UNKNOWN' THEN 'INVALID'
            WHEN odometer_km IS NULL THEN 'INVALID'
            WHEN bike_type = 'EBIKE' AND battery_level IS NULL THEN 'INVALID'
            WHEN last_service_date < purchase_date THEN 'INVALID'
            ELSE 'VALID'
        END AS dq_status,

        -- =========================
        -- DQ REASON
        -- =========================
        CASE 
            WHEN bike_id IS NULL THEN 'MISSING_ID'
            WHEN bike_type = 'UNKNOWN' THEN 'INVALID_TYPE'
            WHEN odometer_km IS NULL THEN 'INVALID_ODOMETER'
            WHEN bike_type = 'EBIKE' AND battery_level IS NULL THEN 'INVALID_BATTERY'
            WHEN last_service_date < purchase_date THEN 'INVALID_SERVICE_DATE'
            ELSE 'OK'
        END AS dq_reason,

        -- =========================
        -- ANOMALY FLAGS 
        -- =========================
        
        -- Low battery anomaly
        CASE 
            WHEN bike_type = 'EBIKE' AND battery_level < 15 THEN 1
            ELSE 0
        END AS low_battery_flag,

        -- Very high odometer (possible overuse)
        CASE 
            WHEN odometer_km > 50000 THEN 1
            ELSE 0
        END AS high_usage_flag,

        -- Service overdue (more than 6 months)
        CASE 
            WHEN last_service_date IS NOT NULL 
                 AND DATEDIFF('day', last_service_date, CURRENT_DATE()) > 180
            THEN 1
            ELSE 0
        END AS service_overdue_flag,

        -- Invalid firmware anomaly
        CASE 
            WHEN firmware_version IS NULL OR TRIM(firmware_version) = '' THEN 1
            ELSE 0
        END AS firmware_issue_flag,

        CURRENT_TIMESTAMP() AS validated_at

    FROM DEDUPED
)

SELECT * FROM FINAL;

SELECT * FROM BIKES_VALID;


