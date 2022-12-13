USE DATABASE uberdb;
USE SCHEMA uber;

insert into bd_calendar select d.d_date from 
(
select distinct d_date
from SNOWFLAKE_SAMPLE_DATA.TPCDS_SF100TCL.DATE_DIM ) d
JOIN 
(
   select max(end_dt) as mx_date, min(start_dt) as mn_date FROM UBER_STG
) s
where d.d_date >= mn_date and d.d_date <= mx_date;