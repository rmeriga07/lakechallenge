
#Query to find the distribution of the city travel
select distinct city, 
IFF( (upper_bound_mean_diff >= lower_bound_mean_diff),'lower boundary distribution','upper boundary distribution' ) as distribution_status
from bc_travel;

#Query to find the order of most profitable cities
select city, MIN(lower_bound_mean_diff) as lbd
from bc_travel
GROUP BY city order by lbd asc;