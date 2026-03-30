CREATE OR REPLACE DATABASE HACKATHON_BIKE_RIDES;
CREATE SCHEMA RAW_SCHEMA;
USE HACKATHON_BIKE_RIDES;
USE SCHEMA RAW_SCHEMA;
--csv format
create or replace file format csv_format type=csv 
skip_header=1 
field_optionally_enclosed_by='"';
--json format
CREATE OR REPLACE FILE FORMAT JSON_FILE_FORMAT
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE;
--stations table 
CREATE OR REPLACE TABLE STATIONS (
    station_id        STRING PRIMARY KEY,
    station_name      STRING,
    latitude          STRING,
    longitude         STRING,
    capacity          STRING,
    neighborhood      STRING,
    city_zone         STRING,
    install_date      STRING,
    status            STRING
);
CREATE OR REPLACE TABLE BIKES (
    bike_id STRING PRIMARY KEY ,
    bike_type         STRING,
    status            STRING,
    purchase_date     STRING,
    last_service_date STRING,
    odometer_km       STRING,
    battery_level     STRING,
    firmware_version  STRING
);
CREATE OR REPLACE TABLE USERS_TABLE (
    user_id STRING PRIMARY KEY,
    customer_name STRING,
    dob STRING, 
    gender STRING,
    email STRING,
    phone STRING,
    address STRING,
    city STRING,
    state STRING,
    region STRING,
    kyc_status STRING,
    registration_date STRING,
    is_student STRING,
    corporate_id STRING
);
SHOW TABLES;
