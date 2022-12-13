USE DATABASE uberdb;
USE SCHEMA uber;

CREATE OR REPLACE TEMPORARY TABLE uniq_travel as 
( select r.*, concat(r.origin_movement_id,'t',r.destination_movement_id) as travel_id from (
    select row_number() OVER ( partition by origin_movement_id, destination_movement_id order by sf_ts desc) as rank, * from uber_stg
    ) r where r.rank=1
);

CREATE OR REPLACE TEMPORARY TABLE uniq_travel_lk as 
select 
c.business_dt, 
o.origin_key, d.destination_key, s.* 
FROM uniq_travel s
JOIN bd_origin o on s.origin_movement_id = o.origin_movement_id and o.current_version = 'Y'
JOIN bd_destination d on s.destination_movement_id = d.destination_movement_id and d.current_version = 'Y'
JOIN bd_calendar c WHERE c.business_dt >= s.start_dt and c.business_dt <= s.end_dt
;


CREATE OR REPLACE TEMPORARY TABLE uniq_travel_src as 
( select *, 'Y' as current_version  from uniq_travel_lk UNION ALL 
  select *, 'N' as current_version from uniq_travel_lk
);

MERGE INTO bf_travel as t
USING uniq_travel_src
 as s
ON t.travel_id = s.travel_id
AND t.business_dt = s.business_dt
AND t.current_version = 'Y'
AND S.current_version ='Y'
WHEN MATCHED THEN UPDATE
SET to_date = s.sf_ts,
current_version ='N'
WHEN NOT MATCHED AND s.current_version = 'N' THEN INSERT 
VALUES (
uuid_string(),              
s.travel_id,
s.city,
s.business_dt,
s.origin_key,
s.destination_key,
s.mean_travel_time_seconds,
s.upper_bound_travel_time_seconds,
s.lower_bound_travel_time_seconds,   
s.sf_ts,             
'9999-01-01 00:00:00',                 
'Y' );
