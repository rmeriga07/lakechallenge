USE DATABASE uberdb;
USE SCHEMA uber;

TRUNCATE TABLE bc_travel;
INSERT INTO bc_travel
SELECT 
travel_key,
travel_id,
city,
business_dt,
mean_travel_time,
upper_bound_travel_time,
lower_bound_travel_time,
(upper_bound_travel_time-mean_travel_time) as upper_bound_mean_diff,
(lower_bound_travel_time-mean_travel_time) as lower_bound_mean_diff
from bf_travel 
WHERE CURRENT_VERSION = 'Y';



