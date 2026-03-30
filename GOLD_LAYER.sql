CREATE OR REPLACE SCHEMA CURATED_SCHEMA;
USE SCHEMA CURATED_SCHEMA;
CREATE OR REPLACE TABLE RENTALS_FINAL (
    rental_id STRING,
    user_id STRING,
    bike_id STRING,
    start_station_id STRING,
    end_station_id STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_sec NUMBER,
    distance_km FLOAT,
    price FLOAT,
    channel STRING,
    speed_kmh FLOAT,
    ultra_short_trip_flag INT,
    unrealistic_speed_flag INT,
    dq_status STRING,
    ingestion_ts TIMESTAMP,
    is_flagged BOOLEAN
);
CREATE OR REPLACE TABLE DIM_USERS (
    user_id STRING,
    email STRING,
    city STRING,
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN
);
CREATE OR REPLACE TABLE DIM_BIKES (
    bike_id STRING,
    bike_type STRING,
    status STRING,
    battery_level NUMBER,
    is_active BOOLEAN
);
CREATE OR REPLACE TABLE DIM_STATIONS (
    station_id STRING,
    station_name STRING,
    city_zone STRING,
    capacity NUMBER,
    is_active BOOLEAN
);
CREATE OR REPLACE STREAM RENTALS_STREAM 
ON TABLE VALIDATION_SCHEMA.RENTALS_VALID;

CREATE OR REPLACE STREAM USERS_STREAM 
ON TABLE VALIDATION_SCHEMA.USERS_VALID;

CREATE OR REPLACE STREAM BIKES_STREAM 
ON TABLE VALIDATION_SCHEMA.BIKES_VALID;

CREATE OR REPLACE STREAM STATIONS_STREAM 
ON TABLE VALIDATION_SCHEMA.STATIONS_VALID;
MERGE INTO RENTALS_FINAL t
USING VALIDATION_SCHEMA.RENTALS_VALID s
ON t.rental_id = s.rental_id

WHEN MATCHED THEN UPDATE SET
    t.price = s.price,
    t.duration_sec = s.duration_sec,
    t.distance_km = s.distance_km,
    t.speed_kmh = s.speed_kmh,
    t.ultra_short_trip_flag = s.ultra_short_trip_flag,
    t.unrealistic_speed_flag = s.unrealistic_speed_flag,
    t.dq_status = s.dq_status,
    t.ingestion_ts = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT (
    rental_id, user_id, bike_id, start_station_id, end_station_id,
    start_time, end_time, duration_sec, distance_km, price, channel,
    speed_kmh, ultra_short_trip_flag, unrealistic_speed_flag,
    dq_status, ingestion_ts, is_flagged
)
VALUES (
    s.rental_id, s.user_id, s.bike_id, s.start_station_id, s.end_station_id,
    s.start_time, s.end_time, s.duration_sec, s.distance_km, s.price, s.channel,
    s.speed_kmh, s.ultra_short_trip_flag, s.unrealistic_speed_flag,
    s.dq_status, CURRENT_TIMESTAMP(), FALSE
);
MERGE INTO DIM_USERS t
USING VALIDATION_SCHEMA.USERS_VALID s
ON t.user_id = s.user_id AND t.is_current = TRUE

WHEN MATCHED AND (
    t.email <> s.email OR t.city <> s.city
)
THEN UPDATE SET
    t.is_current = FALSE,
    t.end_date = CURRENT_DATE()

WHEN NOT MATCHED THEN
INSERT (
    user_id, email, city, start_date, end_date, is_current
)
VALUES (
    s.user_id, s.email, s.city, CURRENT_DATE(), NULL, TRUE
);
CREATE OR REPLACE PROCEDURE APPLY_ANOMALY_RULES()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN

-- Ultra short
UPDATE RENTALS_FINAL
SET is_flagged = TRUE
WHERE duration_sec < 60;

-- Unrealistic speed
UPDATE RENTALS_FINAL
SET is_flagged = TRUE
WHERE speed_kmh > 80;

-- Zero movement
UPDATE RENTALS_FINAL
SET is_flagged = TRUE
WHERE distance_km = 0 AND duration_sec > 300;

RETURN 'Anomalies Applied';

END;
$$;
CREATE OR REPLACE TASK RENTALS_TASK
WAREHOUSE = COMPUTE_WH
SCHEDULE = '5 MINUTE'
AS

MERGE INTO RENTALS_FINAL t
USING RENTALS_STREAM s
ON t.rental_id = s.rental_id

WHEN MATCHED THEN UPDATE SET
    t.price = s.price,
    t.duration_sec = s.duration_sec

WHEN NOT MATCHED THEN
INSERT VALUES (
    s.rental_id, s.user_id, s.bike_id, s.start_station_id, s.end_station_id,
    s.start_time, s.end_time, s.duration_sec, s.distance_km,
    s.price, s.channel, s.speed_kmh,
    s.ultra_short_trip_flag, s.unrealistic_speed_flag,
    s.dq_status, CURRENT_TIMESTAMP(), FALSE
);
CREATE OR REPLACE TASK ANOMALY_TASK
WAREHOUSE = COMPUTE_WH
AFTER RENTALS_TASK
AS
CALL APPLY_ANOMALY_RULES();
ALTER TASK RENTALS_TASK RESUME;
ALTER TASK ANOMALY_TASK RESUME;
CREATE OR REPLACE MASKING POLICY MASK_EMAIL
AS (val STRING) RETURNS STRING ->
CASE 
    WHEN CURRENT_ROLE() IN ('ACCOUNTADMIN') THEN val
    ELSE '***MASKED***'
END;

ALTER TABLE DIM_USERS MODIFY COLUMN email SET MASKING POLICY MASK_EMAIL;
CREATE OR REPLACE ROW ACCESS POLICY REGION_POLICY
AS (region STRING) RETURNS BOOLEAN ->
CURRENT_ROLE() = region;

ALTER TABLE DIM_USERS
ADD ROW ACCESS POLICY REGION_POLICY ON (city);
CREATE OR REPLACE TABLE AUDIT_LOG (
    table_name STRING,
    load_time TIMESTAMP,
    row_count INT,
    status STRING
);
INSERT INTO AUDIT_LOG
SELECT
    'RENTALS_FINAL',
    CURRENT_TIMESTAMP(),
    COUNT(*),
    'SUCCESS'
FROM RENTALS_FINAL;
