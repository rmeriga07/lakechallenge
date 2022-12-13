CREATE OR REPLACE TEMPORARY TABLE uniq_origin as 
( select r.* from (
    select row_number() OVER ( partition by origin_movement_id order by sf_ts desc) as rank, * from uber_stg
    ) r where r.rank=1
);

CREATE OR REPLACE TEMPORARY TABLE uniq_origin_src as 
( select *, 'Y' as current_version from uniq_origin UNION ALL 
  select *, 'N' as current_version from uniq_origin
);

MERGE INTO bd_origin as t
USING uniq_origin_src
 as s
ON t.origin_movement_id = s.origin_movement_id 
AND t.current_version = 'Y'
AND S.current_version ='Y'
WHEN MATCHED THEN UPDATE
SET to_date = s.sf_ts,
current_version ='N'
WHEN NOT MATCHED AND s.current_version = 'N' THEN INSERT 
VALUES (
uuid_string(),              
s.origin_movement_id,
s.origin_display_name,    
s.origin_geometry,    
s.sf_ts,             
'9999-01-01 00:00:00',                 
'Y' );