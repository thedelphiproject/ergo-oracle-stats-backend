/******************************
Date written: 2022-02-11
Last updated: 2022-02-12
Description: WORK IN PROGRESS
******************************/ 

/* DELPHI */
-- list of known oracle pools
select *
from delphi.pools
;



/* POOL SPECIFIC DATA */

-- number of datapoints posted to date
select dp.pool_id, p.name as pool_name, count(*) as total_posted 
from delphi.datapoints dp 
join delphi.pools p on p.id = dp.pool_id
group by 1, 2
order by 1 ;

-- timeseries of datapoints
select dp.pool_id, p.name as pool_name, to_timestamp(dp.timestamp / 1000)::date as date, count(*) as total_posted
from delphi.datapoints dp
join delphi.pools p on p.id = dp.pool_id 
group by 1, 2, 3
order by 1, 3
limit 1000;

-- timeseries of active oracles (oracles submitting a datapoint)
select a.pool_id, a.pool_name, a.date, count(*) as active_oracles
from (
select distinct dp.pool_id, p.name as pool_name, to_timestamp(dp.timestamp / 1000)::date as date, oracle_id 
from delphi.datapoints dp
join delphi.pools p on p.id = dp.pool_id 
) a 
group by 1, 2, 3 
order by 1, 3
limit 1000;




/* ORACLE SPECIFIC DATA */
-- number of datapoints submitted to date
select dp.oracle_id, dp.pool_id, p.name as pool_name, count(*) as total_posted
from delphi.datapoints dp 
join delphi.pools p on p.id = dp.pool_id
group by 1, 2, 3
order by 2, 1;


-- timeseries of datapoints
select dp.oracle_id, o.address, dp.pool_id, p.name as pool_name, to_timestamp(dp.timestamp / 1000)::date as date, count(*) as total_posted
from delphi.datapoints dp
join delphi.pools p on p.id = dp.pool_id
join delphi.oracles o on o.oracle_id = dp.oracle_id 
group by 1, 2, 3, 4, 5
order by 1
limit 1000;



-- first and last posting date
with postings as (
select dp.pool_id, dp.oracle_id, 'first' as posting_horizon, min(to_timestamp(dp.timestamp / 1000)) as timestamp
from delphi.datapoints dp
group by 1, 2

union 

select dp.pool_id, dp.oracle_id, 'last' as posting_horizon, max(to_timestamp(dp.timestamp / 1000)) as timestamp
from delphi.datapoints dp
group by 1, 2
order by 2 )

select ps.oracle_id, o.address, ps.pool_id, p.name as pool_name,  ps.posting_horizon, ps.timestamp 
from postings ps
join delphi.pools p on p.id = ps.pool_id 
join delphi.oracles o on o.oracle_id = ps.oracle_id 
order by 1 
;










