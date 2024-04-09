
-- *************************************************************************************** --
-- 1. Create roles as per the mentioned hierarchy.

USE ROLE ACCOUNTADMIN;
CREATE ROLE ADMIN;
GRANT ROLE ADMIN TO ROLE ACCOUNTADMIN;
CREATE ROLE DEVELOPER;
GRANT ROLE DEVELOPER TO ROLE ADMIN;
CREATE ROLE PII;
GRANT ROLE PII TO ROLE ACCOUNTADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE admin;
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE admin;
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE PII;
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE DEVELOPER;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE DEVELOPER;
GRANT USAGE ON SCHEMA assignment_db.my_schema TO ROLE PII;
GRANT USAGE ON SCHEMA assignment_db.my_schema TO ROLE DEVELOPER;
GRANT SELECT ON ALL TABLES IN SCHEMA assignment_db.my_schema TO ROLE PII;
GRANT SELECT ON ALL TABLES IN SCHEMA assignment_db.my_schema TO ROLE DEVELOPER;

-- *************************************************************************************** --
-- 2.Create an M-sized warehouse using the accountadmin role, name = 'assignment_wh'.

CREATE OR REPLACE WAREHOUSE assignment_wh WAREHOUSE_SIZE = 'Medium' WAREHOUSE_TYPE = 'STANDARD';
USE WAREHOUSE assignment_wh;

-- *************************************************************************************** --
-- 3. Switch to role admin.

USE ROLE ADMIN;

-- *************************************************************************************** --
-- 4. Create a database assignment_db.
-- 5. Create a schema my_schema.

CREATE DATABASE IF NOT EXISTS assignment_db;
CREATE SCHEMA IF NOT EXISTS assignment_db.my_schema;
USE schema my_schema;

-- *************************************************************************************** --
-- 6. Create a table using a sample csv for internal staging

CREATE OR REPLACE TABLE assignment_db.my_schema.employee_data_internal(
        ELT_TS TIMESTAMP_NTZ,
        ELT_BY VARCHAR DEFAULT 'SnowSQL CLI',
        FILE_NAME VARCHAR,
        EMPLOYEE_ID INT,
        FIRST_NAME VARCHAR,
        LAST_NAME VARCHAR,
        EMAIL VARCHAR,
        PHONE_NUMBER VARCHAR,
        HIRE_DATE DATE,
        JOB_ID VARCHAR,
        SALARY FLOAT,
        COMMISION_PCT VARCHAR,
        MANAGER_ID INT,
        DEPARTMENT_ID INT
    );

-- *************************************************************************************** --
-- 7. Also, create a variant version of this dataset.

CREATE TABLE assignment_db.my_schema.employee_variant(employee_data VARIANT);
INSERT INTO assignment_db.my_schema.employee_variant (employee_data)
SELECT OBJECT_CONSTRUCT(
        'ELT_TS',
        ELT_TS,
        'ELT_BY',
        ELT_BY,
        'FILE_NAME',
        FILE_NAME,
        'EMPLOYEE_ID',
        EMPLOYEE_ID,
        'FIRST_NAME',
        FIRST_NAME,
        'LAST_NAME',
        LAST_NAME,
        'EMAIL',
        EMAIL,
        'PHONE_NUMBER',
        PHONE_NUMBER,
        'HIRE_DATE',
        HIRE_DATE,
        'JOB_ID',
        JOB_ID,
        'SALARY',
        SALARY,
        'MANAGER_ID',
        MANAGER_ID,
        'DEPARTMENT_ID',
        DEPARTMENT_ID
    ) AS employee_data
FROM assignment_db.my_schema.employee_data_internal;
SELECT *
FROM EMPLOYEE_VARIANT;

-- *************************************************************************************** --
-- 8. Load the file into an internal stage
-- 9. Load data into the tables using copy into statements. Load from the internal stage

--STEPS PERFORMED ON CLI FOR INTERNAL STAGING OF THE SAMPLE CSV FILE.
-- CREATE STAGE MYSTAGE;
-- create or replace file format myformat
-- type = csv
-- field_delimiter = ','
-- skip_header = 1
-- null_if = ('NULL', 'null')
-- empty_field_as_null = true
-- error_on_column_count_mismatch=false;
-- PUT file://Desktop/employees.csv @MYSTAGE;
-- COPY INTO EMPLOYEE_DATA_INTERNAL (
--     ELT_TS,
--     FILE_NAME,
--     EMPLOYEE_ID,
--     FIRST_NAME,
--     LAST_NAME,
--     EMAIL,
--     PHONE_NUMBER,
--     HIRE_DATE,
--     JOB_ID,
--     SALARY,
--     MANAGER_ID,
--     DEPARTMENT_ID
-- )
-- FROM (
--         SELECT METADATA $FILE_LAST_MODIFIED AS ELT_TS,
--             METADATA $FILENAME AS FILE_NAME,
--             t.$1 AS EMPLOYEE_ID,
--             t.$2 AS FIRST_NAME,
--             t.$3 AS LAST_NAME,
--             t.$4 AS EMAIL,
--             t.$5 AS PHONE_NUMBER,
--             t.$6 AS HIRE_DATE,
--             t.$7 AS JOB_ID,
--             t.$8 AS SALARY,
--             t.$10 AS MANAGER_ID,
--             t.$11 AS DEPARTMENT_ID
--         FROM @mystage / employees.csv.gz (file_format => myformat) t
--     );
-- SELECT * FROM EMPLOYEE_DATA_INTERNAL;

-- *************************************************************************************** --
-- 6. Create a table using a sample csv for external staging

CREATE OR REPLACE TABLE assignment_db.my_schema.employee_data_external(
        ELT_TS TIMESTAMP_NTZ,
        ELT_BY VARCHAR DEFAULT 'SnowSQL CLI',
        FILE_NAME VARCHAR,
;
EMPLOYEE_ID INT,
FIRST_NAME VARCHAR,
LAST_NAME VARCHAR,
EMAIL VARCHAR,
PHONE_NUMBER VARCHAR,
HIRE_DATE DATE,
JOB_ID VARCHAR,
SALARY FLOAT,
MANAGER_ID INT,
DEPARTMENT_ID INT
);

-- *************************************************************************************** --
-- 8. Load the file into an external stage
-- 9. Load data into the tables using copy into statements. Load from the external stage

-- CREATE OR REPLACE STORAGE INTEGRATION S3_INT
-- TYPE = EXTERNAL_STAGE
-- STORAGE_PROVIDER = S3
-- ENABLED = TRUE
-- STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::949770910753:role/sflrole'
-- STORAGE_ALLOWED_LOCATIONS = ('s3://sflbkt');
-- CREATE STAGE mystage1 
-- STORAGE_INTEGRATION = S3_INT
-- URL = 's3://sflbkt/employees.csv'
-- file_format = myformat;
-- desc Integration S3_INT;
-- COPY INTO EMPLOYEE_DATA_EXTERNAL (
--     ELT_TS,
--     FILE_NAME,
--     EMPLOYEE_ID,
--     FIRST_NAME,
--     LAST_NAME,
--     EMAIL,
--     PHONE_NUMBER,
--     HIRE_DATE,
--     JOB_ID,
--     SALARY,
--     MANAGER_ID,
--     DEPARTMENT_ID
-- )
-- FROM (
--         SELECT METADATA $FILE_LAST_MODIFIED AS ELT_TS,
--             METADATA $FILENAME AS FILE_NAME,
--             t.$1 AS EMPLOYEE_ID,
--             t.$2 AS FIRST_NAME,
--             t.$3 AS LAST_NAME,
--             t.$4 AS EMAIL,
--             t.$5 AS PHONE_NUMBER,
--             t.$6 AS HIRE_DATE,
--             t.$7 AS JOB_ID,
--             t.$8 AS SALARY,
--             t.$10 AS MANAGER_ID,
--             t.$11 AS DEPARTMENT_ID
--         FROM @mystage1 t
--     );

-- select * from employee_data_external;

-- *************************************************************************************** --
-- 10. Upload any parquet file to the stage location and infer the schema of the file.

CREATE OR REPLACE TABLE CITIES (
        CONTINENT VARCHAR DEFAULT NULL,
        COUNTRY VARCHAR DEFAULT NULL,
        CITY VARIANT DEFAULT NULL
    );
CREATE OR REPLACE FILE FORMAT parquetFileFormat TYPE = parquet;
CREATE OR REPLACE STAGE parquetFileStage FILE_FORMAT = parquetFileFormat;
-- Command on CLI

-- PUT  file://Desktop/CITIES.PARQUET @parquetFileStage;
-- list @parquetFileStage;

-- Infering schema of the file and quering data.

SELECT *
FROM TABLE(
        INFER_SCHEMA(
            LOCATION => '@parquetfilestage/CITIES.PARQUET',
            FILE_FORMAT => 'parquetFileFormat'
        )
    );

-- *************************************************************************************** --
-- 11. Run a select query on the staged parquet file without loading it to a snowflake table

SELECT $1
FROM @parquetfilestage / CITIES.PARQUET;
SELECT $1 :continent::varchar,
    $1 :country :name::varchar,
    $1 :country :city::variant
FROM @parquetfilestage / CITIES.PARQUET;

-- *************************************************************************************** --
-- 12. Add masking policy to the PII columns such that fields 
-- like email, phone number, etc. show as **masked** to a user with the developer role. 
-- If the role is PII the value of these columns should be visible
-- Create or replace the masking policy for email

CREATE OR REPLACE MASKING POLICY email_mask AS (val STRING) RETURNS STRING->CASE
        WHEN CURRENT_ROLE() = 'DEVELOPER' THEN '**MASKED**'
        ELSE VAL
    END;
    
ALTER TABLE employee_data_external
MODIFY COLUMN EMAIL
SET MASKING POLICY email_mask;

-- Create or replace the masking policy for phone number
CREATE OR REPLACE MASKING POLICY phone_number_mask AS (val STRING) RETURNS STRING->CASE
        WHEN CURRENT_ROLE() = 'DEVELOPER' THEN '**MASKED**'
        ELSE VAL
    END;

ALTER TABLE employee_data_external
MODIFY COLUMN PHONE_NUMBER
SET MASKING POLICY phone_number_mask;

-- check for the masked values
USE ROLE DEVELOPER;
SELECT * FROM ASSIGNMENT_DB.MY_SCHEMA.EMPLOYEE_DATA_EXTERNAL;

USE ROLE PII;
SELECT * FROM ASSIGNMENT_DB.MY_SCHEMA.EMPLOYEE_DATA_EXTERNAL;
