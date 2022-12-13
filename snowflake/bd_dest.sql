--TRUNCATE TABLE BD_ORIGIN;
USE DATABASE uberdb;
USE SCHEMA uber;


CREATE OR REPLACE TEMPORARY TABLE uniq_dest as 
( select r.* from (
    select row_number() OVER ( partition by destination_movement_id order by sf_ts desc) as rank, * from uber_stg
    ) r where r.rank=1
);

CREATE OR REPLACE TEMPORARY TABLE uniq_dest_src as 
( select *, 'Y' as current_version from uniq_dest UNION ALL 
  select *, 'N' as current_version from uniq_dest
);

MERGE INTO bd_destination as t
USING uniq_dest_src
 as s
ON t.destination_movement_id = s.destination_movement_id 
AND t.current_version = 'Y'
AND S.current_version ='Y'
WHEN MATCHED THEN UPDATE
SET to_date = s.sf_ts,
current_version ='N'
WHEN NOT MATCHED AND s.current_version = 'N' THEN INSERT 
VALUES (
uuid_string(),              
s.destination_movement_id,
s.destination_display_name,    
s.destination_geometry,    
s.sf_ts,             
'9999-01-01 00:00:00',                 
'Y' );

