/******************************
Date written: 2022-02-11
Description: WORK IN PROGRESS
******************************/ 

/* DELPHI */
-- list of known oracle pools
-- need associated p2pk address with each oracle hash
select *
from delphi.pools
;



/* POOL SPECIFIC DATA */

-- number of datapoints posted to date
select pool_id, count(*) as n 
from delphi.datapoints 
group by 1
order by 1 ;

-- timeseries of datapoints
select to_timestamp(timestamp / 1000)::date as date, pool_id, count(*) as n 
from delphi.datapoints 
group by 1,2 
order by 2, 1
limit 5;




/* ORACLE SPECIFIC DATA */
-- number of datapoints submitted to date
select pool_id, oracle_id, count(*) as n 
from delphi.datapoints 
group by 1, 2 
order by 1 ;


-- timeseries of datapoints
select to_timestamp(timestamp / 1000)::date as date, pool_id, oracle_id, count(*) as n 
from delphi.datapoints 
group by 1,2,3
order by 2, 1
limit 5;



