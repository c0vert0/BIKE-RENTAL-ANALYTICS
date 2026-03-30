CREATE OR REPLACE DATABASE HACKATHON_BIKE_RIDES;
CREATE SCHEMA RAW_SCHEMA;
USE DATABASE HACKATHON_BIKE_RIDES;
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
    status            STRING,
    _metadata_filename STRING,
    _metadata_file_row_number NUMBER,
    _load_timestamp TIMESTAMP_LTZ
);
CREATE OR REPLACE TABLE BIKES (
    bike_id STRING,
    bike_type         STRING,
    status            STRING,
    purchase_date     STRING,
    last_service_date STRING,
    odometer_km       STRING,
    battery_level     STRING,
    firmware_version  STRING,
    _metadata_filename STRING,
    _metadata_file_row_number NUMBER,
    _load_timestamp TIMESTAMP_LTZ
);
CREATE OR REPLACE TABLE USERS_TABLE (
    user_id STRING,
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
    corporate_id STRING,
    _metadata_filename STRING,
    _metadata_file_row_number NUMBER,
    _load_timestamp TIMESTAMP_LTZ
);
CREATE OR REPLACE TABLE RAW_RENTALS (
    rental_id STRING,
    user_id STRING,
    bike_id STRING,
    start_station_id STRING,
    end_station_id STRING,
    start_time STRING,
    end_time STRING,
    duration_sec STRING,
    distance_km STRING,
    price STRING,
    plan_type STRING,
    channel STRING,
    device_info STRING,
    start_gps STRING,
    end_gps STRING,
    is_flagged STRING,
    _metadata_filename STRING,
    _metadata_file_row_number NUMBER,
    _load_timestamp TIMESTAMP_LTZ
);

create or replace storage integration int1
    type = external_stage
    storage_provider = s3
    storage_aws_role_arn = 'arn:aws:iam::150852244298:role/carpediemr1'
    enabled = true
    storage_allowed_locations=('s3://carpediem-m-h/D-day/');

desc integration int1;


create or replace stage s1
    storage_integration=int1
    url='s3://carpediem-m-h/D-day/';
    

ls@s1;

-- Pipe for STATIONS table
CREATE OR REPLACE PIPE STATIONS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO STATIONS
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @s1/Stations/
)
FILE_FORMAT = (FORMAT_NAME = 'HACKATHON_BIKE_RIDES.RAW_SCHEMA.CSV_FORMAT')
ON_ERROR = 'CONTINUE'
PATTERN = '.*\.csv';;

show pipes;

-- Pipe for BIKES table
CREATE OR REPLACE PIPE BIKES_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO HACKATHON_BIKE_RIDES.RAW_SCHEMA.BIKES
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8,
        METADATA$FILENAME AS _metadata_filename,
        METADATA$FILE_ROW_NUMBER AS         _metadata_file_row_number,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @HACKATHON_BIKE_RIDES.RAW_SCHEMA.S1/Bikes/
)
FILE_FORMAT = (FORMAT_NAME = 'HACKATHON_BIKE_RIDES.RAW_SCHEMA.CSV_FORMAT')
ON_ERROR = 'CONTINUE'
PATTERN = '.*\.csv';;

-- Pipe for USERS_TABLE
CREATE OR REPLACE PIPE HACKATHON_BIKE_RIDES.RAW_SCHEMA.USERS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO HACKATHON_BIKE_RIDES.RAW_SCHEMA.USERS_TABLE
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
        METADATA$FILENAME AS _metadata_filename,
        METADATA$FILE_ROW_NUMBER AS _metadata_file_row_number,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @s1/Users/
)
FILE_FORMAT = (FORMAT_NAME = 'HACKATHON_BIKE_RIDES.RAW_SCHEMA.CSV_FORMAT');
--Rentals PIPE
CREATE OR REPLACE PIPE RENTALS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO RAW_RENTALS
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8,
        $9, $10, $11, $12, $13, $14, $15, $16,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        CURRENT_TIMESTAMP()
    FROM @S1/Rentals/
)
FILE_FORMAT = (FORMAT_NAME = CSV_FORMAT)
ON_ERROR = 'CONTINUE'
PATTERN = '.*\.csv';

ALTER PIPE HACKATHON_BIKE_RIDES.RAW_SCHEMA.USERS_PIPE REFRESH;

select * from users_table;
SELECT SYSTEM$PIPE_STATUS('HACKATHON_BIKE_RIDES.RAW_SCHEMA.USERS_PIPE');

-- Pipe for RENTALS table (CSV)
CREATE OR REPLACE PIPE HACKATHON_BIKE_RIDES.RAW_SCHEMA.RENTALS_PIPE
AUTO_INGEST = TRUE
AS
COPY INTO HACKATHON_BIKE_RIDES.RAW_SCHEMA.RENTALS
FROM (
    SELECT 
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
        METADATA$FILENAME AS _metadata_filename,
        METADATA$FILE_ROW_NUMBER AS _metadata_file_row_number,
        CURRENT_TIMESTAMP() AS _load_timestamp
    FROM @s1/Rentals/
)
FILE_FORMAT = (FORMAT_NAME = 'CSV_FORMAT');

SHOW PIPES;

SHOW FILE FORMATS;

SELECT * FROM RENTALS;
