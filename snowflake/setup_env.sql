--Snowflake:

use role accountadmin;

create role uber_data_ro;
create role uber_data_rw;

create database uberdb;
create schema uber;

grant usage on database uberdb to role uber_data_ro;
grant usage on database uberdb to role uber_data_rw;

grant usage on schema uber to role uber_data_ro;
grant usage on schema uber to role uber_data_rw;

CREATE OR REPLACE STAGE uber_ext_stage
URL='s3://mer-dlake-stg/data/sf/'
CREDENTIALS=(AWS_KEY_ID='AKIAXXXXXX' AWS_SECRET_KEY='xDakOLxNTMXXXXXX');



create warehouse uber_etl_wh with WAREHOUSE_SIZE = 'SMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 600 AUTO_RESUME = TRUE;
create warehouse uber_read_wh with WAREHOUSE_SIZE = 'SMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 600 AUTO_RESUME = TRUE;


grant usage on warehouse uber_etl_wh to role uber_data_rw;
grant usage on warehouse uber_read_wh to role uber_data_ro;

grant usage, operate on warehouse uber_etl_wh to role uber_data_rw;