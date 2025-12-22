USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS ICEBERG_TEST_DB;

/*
Create Snowflake Storage integration
  https://docs.snowflake.com/en/sql-reference/sql/create-storage-integration
*/
CREATE OR REPLACE STORAGE INTEGRATION ICEBERG_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = ''
  STORAGE_ALLOWED_LOCATIONS = ('*');

/*
Create Snowflake external volume
  https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume
Configure Snowflake external volume
  https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-s3
*/

CREATE OR REPLACE EXTERNAL VOLUME SF_EXTERNAL_VOLUME
  STORAGE_LOCATIONS =
      (
        (
            NAME = 'my-eu-west-1'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://'
            STORAGE_AWS_ROLE_ARN = ''
        )
      )
  ALLOW_WRITES = TRUE;

SET EXTERNAL_VOLUME = 'SF_EXTERNAL_VOLUME';
ALTER DATABASE ICEBERG_TEST_DB SET external_volume = $external_volume;

CREATE SCHEMA DEMO;

CREATE OR REPLACE ICEBERG TABLE ICEBERG_TEST_DB.DEMO.TEST_TABLE_EXTENDED (
  col_int INT comment 'int column', 
  col_string STRING comment 'string column',
  col_timestamp_ntz timestamp_ntz(6) comment 'timestamp_ntz column'
  )
  CATALOG='SNOWFLAKE' 
  EXTERNAL_VOLUME=$external_volume 
  BASE_LOCATION='test_table_extended';

insert into ICEBERG_TEST_DB.DEMO.TEST_TABLE_EXTENDED (col_int, col_string, col_timestamp_ntz)
values (1, 'test', '2025-01-01 01:00:00'), 
(2, 'test2', '2025-01-02 02:00:00'),
(3, 'test3', '2025-01-03 03:00:00');


-- Setup service account role and grant access to an iceberg table
CREATE OR REPLACE ROLE ICEBERG_DATA_ENGINEER;

--Database level permissions
GRANT USAGE ON DATABASE IDENTIFIER('"ICEBERG_TEST_DB"') TO ROLE IDENTIFIER('"ICEBERG_DATA_ENGINEER"');

--Schema level permissions
GRANT USAGE ON SCHEMA IDENTIFIER('"ICEBERG_TEST_DB"."DEMO"') TO ROLE IDENTIFIER('"ICEBERG_DATA_ENGINEER"');

--Table level permissions
GRANT SELECT ON TABLE IDENTIFIER('"TEST_TABLE_EXTENDED"') TO ROLE IDENTIFIER('"ICEBERG_DATA_ENGINEER"');
GRANT INSERT ON TABLE IDENTIFIER('"TEST_TABLE_EXTENDED"') TO ROLE IDENTIFIER('"ICEBERG_DATA_ENGINEER"');
GRANT UPDATE ON TABLE IDENTIFIER('"TEST_TABLE_EXTENDED"') TO ROLE IDENTIFIER('"ICEBERG_DATA_ENGINEER"');

--CREATE SERCIE USER
CREATE OR REPLACE USER SPARK_USER TYPE=SERVICE DEFAULT_ROLE=ICEBERG_DATA_ENGINEER;
GRANT ROLE ICEBERG_DATA_ENGINEER TO USER SPARK_USER;

CREATE OR REPLACE USER SPARK_USER TYPE=SERVICE DEFAULT_ROLE=ICEBERG_DATA_ENGINEER;

ALTER USER IF EXISTS SPARK_USER
ADD PAT HORIZON_REST_SRV_ACCOUNT_USER_PAT
  DAYS_TO_EXPIRY = 7
  ROLE_RESTRICTION = 'ICEBERG_DATA_ENGINEER'
  COMMENT = 'HORIZON REST API PAT FOR SERVICE ACCOUNT';

--Save the PAT and COPY it to the spark_clean.ipynb file
