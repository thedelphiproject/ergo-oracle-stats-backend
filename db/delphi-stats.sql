/******************************
Date written: 2022-02-11
Last updated: 2022-02-13
Description: SQL queries to compute Delphi statitiscs for visualization

Future work
- Identify rejected datapoints and success rate
- Total oracle tokens and activation rate
******************************/ 

/******************************
****** ORACLE POOL DATA *******
******************************/

-- total datapoints posted to date
select 
    dp.pool_id, 
    p.name as pool_name, 
    count(*) as total_posted 
from delphi.datapoints dp 
join delphi.pools p on p.id = dp.pool_id
group by 1, 2
order by 1 
;


-- timeseries of datapoints posted to date
select 
    dp.pool_id, 
    p.name as pool_name, 
    to_timestamp(dp.timestamp / 1000)::date as date, 
    count(*) as total_posted
from delphi.datapoints dp
join delphi.pools p on p.id = dp.pool_id 
group by 1, 2, 3
order by 1, 3
;

-- timeseries of active oracles (oracles submitting a datapoint)
select 
    a.pool_id, 
    a.pool_name, 
    a.date, 
    count(*) as active_oracles
from (
    select 
        distinct dp.pool_id, 
        p.name as pool_name, 
        to_timestamp(dp.timestamp / 1000)::date as date, 
        oracle_id 
    from delphi.datapoints dp
    join delphi.pools p on p.id = dp.pool_id 
    ) a 
group by 1, 2, 3 
order by 1, 3
;



/******************************
***** SPECIFIC ORACLE DATA *****
******************************/

-- total datapoints submitted to date per oracle
select 
    dp.oracle_id, 
    dp.pool_id, 
    p.name as pool_name, 
    count(*) as total_posted
from delphi.datapoints dp 
join delphi.pools p on p.id = dp.pool_id
group by 1, 2, 3
order by 2, 1
;


-- timeseries of datapoints
select 
    dp.oracle_id, 
    dp.pool_id, 
    p.name as pool_name, 
    to_timestamp(dp.timestamp / 1000)::date as date, 
    count(*) as total_posted
from delphi.datapoints dp
join delphi.pools p on p.id = dp.pool_id
group by 1, 2, 3, 4
order by 1
;



-- first and last posting date
with postings as (
select 
    dp.pool_id, 
    dp.oracle_id, 
    'first' as posting_segment, 
    min(to_timestamp(dp.timestamp / 1000)) as timestamp
from delphi.datapoints dp
group by 1, 2
union 
select 
    dp.pool_id, 
    dp.oracle_id, 
    'last' as posting_segment, 
    max(to_timestamp(dp.timestamp / 1000)) as timestamp
from delphi.datapoints dp
group by 1, 2
order by 2 )

select 
    ps.oracle_id, 
    ps.pool_id, 
    p.name as pool_name,  
    ps.posting_segment, ps.timestamp 
from postings ps
join delphi.pools p on p.id = ps.pool_id 
order by 2, 1
;



-- WIP: costs and rewards to date
-- is pd.value == $erg ?
select 
    pd.box_id, 
    pd.pool_id, 
    p.name as pool_name, 
    o.oracle_id, 
    to_timestamp(pd.timestamp / 1000 ) as timestamp, 
    oa.address,
    sum(pd.value)/10^9 as ergo
from delphi.pool_deposits pd
left join delphi.pools p on p.id = pd.pool_id 
left join delphi.oracles o on o.pool_id = pd.pool_id 
left join delphi.oracle_addresses oa on oa.oracle_id = o.oracle_id 
group by 1, 2, 3, 4, 5, 6
limit 1000
;





