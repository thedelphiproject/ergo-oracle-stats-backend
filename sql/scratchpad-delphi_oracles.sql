
-- query for insert into delphi.oracles
-- retrieve address for each oracle

select distinct p.id as pool_id,
dp.datapoint_address, 
nio.address,
nos.additional_registers #>> '{R4,renderedValue}' as oracle_address,
dp.token_id, 
dp.token_name,
dp.token_description,
dp.pool_token_id
from node_outputs nos
join node_transactions txs on txs.id = nos.tx_id
join node_inputs nis on nis.tx_id = txs.id
join node_outputs nio on nio.box_id = nis.box_id
join delphi.discovery_datapoints dp on dp.datapoint_address = nos.address 
inner join delphi.pools p on p.pool_nft_id = dp.pool_token_id
where exists (select distinct datapoint_address from delphi.discovery_datapoints x where x.datapoint_address = nos.address)
and p.id = 2
;



