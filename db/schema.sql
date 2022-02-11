--
-- Name: delphi; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA delphi;


--
-- Name: insert_datapoints(); Type: PROCEDURE; Schema: delphi; Owner: -
--

CREATE PROCEDURE delphi.insert_datapoints()
    LANGUAGE sql
    AS $$
truncate table delphi.datapoints;    
with prep as (
    select 
        o.box_id
        ,os.pool_id
        ,os.oracle_id
        ,max(o.timestamp) as timestamp
        ,o.additional_registers->'R4'->>'renderedValue' as r4
        ,o.additional_registers->'R5'->>'renderedValue' as r5
        ,o.additional_registers->'R6'->>'renderedValue' as r6        
    from public.node_outputs o 
    inner join delphi.oracles os on os.address_hash = o.additional_registers->'R4'->>'renderedValue'
    inner join delphi.pools p on os.pool_id = p.id and os.participation_token_id = p.participant_token_id
    inner join public.node_assets a on o.additional_registers->'R5'->>'renderedValue' = a.box_id and a.token_id = p.pool_nft_id
    group by
        o.box_id
        ,os.pool_id
        ,os.oracle_id
        ,o.additional_registers->'R4'->>'renderedValue'
        ,o.additional_registers->'R5'->>'renderedValue'
        ,o.additional_registers->'R6'->>'renderedValue'
)
insert into delphi.datapoints
select p.box_id,p.pool_id,p.oracle_id,p.timestamp,p.r4,p.r5,p.r6 from prep p
left outer join delphi.datapoints dp on p.box_id = dp.box_id and p.pool_id = dp.pool_id and p.oracle_id = dp.oracle_id
$$;


--
-- Name: insert_discovery_datapoint(); Type: PROCEDURE; Schema: delphi; Owner: -
--

CREATE PROCEDURE delphi.insert_discovery_datapoint()
    LANGUAGE sql
    AS $$
truncate table delphi.discovery_datapoints;
with prep as (
    select
        o.address as datapoint_address
        ,o.additional_registers->'R4'->>'renderedValue' as oracle_address
        ,t.token_id
        ,t.name as token_name
        ,t.description as token_description
        ,o.additional_registers->'R4'->>'renderedValue' as r4
        ,lag(o.additional_registers->'R4'->>'renderedValue') over (order by o.address,o.additional_registers->'R4'->>'renderedValue',o.timestamp desc) as r4_lag
        ,p_t.token_id AS pool_token_id
    from public.node_outputs o
    left join public.node_assets a on o.box_id = a.box_id
    left join public.tokens t on a.token_id = t.token_id
    left join public.node_assets p_a on o.additional_registers->'R5'->>'renderedValue' = p_a.box_id
    left join public.tokens p_t on p_a.token_id = p_t.token_id
    where o.additional_registers->'R4'->>'sigmaType' = 'SGroupElement' and o.additional_registers->'R5'->>'sigmaType' = 'Coll[SByte]' and o.additional_registers->'R6'->>'sigmaType' = 'SLong' and a.value = 1 and p_a.value = 1
    order by o.address,o.additional_registers->'R4'->>'renderedValue',o.timestamp desc
)
insert into delphi.discovery_datapoints
select datapoint_address,oracle_address,token_id,token_name,token_description,pool_token_id
from prep 
where r4 = r4_lag 
group by datapoint_address,oracle_address,token_id,token_name,token_description,pool_token_id
having count(datapoint_address) > 10
$$;


--
-- Name: insert_discovery_epoch_prep(); Type: PROCEDURE; Schema: delphi; Owner: -
--

CREATE PROCEDURE delphi.insert_discovery_epoch_prep()
    LANGUAGE sql
    AS $$
truncate table delphi.discovery_epoch_prep;
with prep as (
    select 
        address
        ,o.additional_registers->'R5'->>'renderedValue' as r5
        ,lag(o.additional_registers->'R5'->>'renderedValue') over (order by o.address,o.additional_registers->'R5'->>'renderedValue',o.timestamp desc) as r5_lag
        ,a.token_id
        ,t.name as token_name
        ,t.description as token_description
    from public.node_outputs o
    left join public.node_assets a on o.box_id = a.box_id
    left join public.tokens t on a.token_id = t.token_id
    --where address = 'EfS5abyDe4vKFrJ48K5HnwTqa1ksn238bWFPe84bzVvCGvK1h2B7sgWLETtQuWwzVdBaoRZ1HcyzddrxLcsoM5YEy4UnqcLqMU1MDca1kLw9xbazAM6Awo9y6UVWTkQcS97mYkhkmx2Tewg3JntMgzfLWz5mACiEJEv7potayvk6awmLWS36sJMfXWgnEfNiqTyXNiPzt466cgot3GLcEsYXxKzLXyJ9EfvXpjzC2abTMzVSf1e17BHre4zZvDoAeTqr4igV3ubv2PtJjntvF2ibrDLmwwAyANEhw1yt8C8fCidkf3MAoPE6T53hX3Eb2mp3Xofmtrn4qVgmhNonnV8ekWZWvBTxYiNP8Vu5nc6RMDBv7P1c5rRc3tnDMRh2dUcDD7USyoB9YcvioMfAZGMNfLjWqgYu9Ygw2FokGBPThyWrKQ5nkLJvief1eQJg4wZXKdXWAR7VxwNftdZjPCHcmwn6ByRHZo9kb4Emv3rjfZE'
    where additional_registers->'R4'->>'sigmaType' = 'SLong' and additional_registers->'R5'->>'sigmaType' = 'SInt' and a.value = 1 and COALESCE(additional_registers->'R6'->>'sigmaType','') = ''
    order by o.address,o.additional_registers->'R5'->>'renderedValue',o.timestamp desc
)
,prep2 AS (
    select
        address
        ,token_id
        ,token_name
        ,token_description
    from prep
    group by address, token_id, token_name, token_description, (CAST(r5 as int)-CAST(COALESCE(r5_lag,'0') as int))
    having count(address) > 10
)
insert into delphi.discovery_epoch_prep
select address,token_id,token_name,token_description from prep2 group by address,token_id,token_name,token_description --having count(token_id) = 1
$$;


--
-- Name: insert_discovery_live_epoch(); Type: PROCEDURE; Schema: delphi; Owner: -
--

CREATE PROCEDURE delphi.insert_discovery_live_epoch()
    LANGUAGE sql
    AS $$
truncate table delphi.discovery_live_epoch;
with prep as (
    select 
        address
        ,o.additional_registers->'R5'->>'renderedValue' as r5
        ,lag(o.additional_registers->'R5'->>'renderedValue') over (order by o.address,o.additional_registers->'R5'->>'renderedValue',o.timestamp desc) as r5_lag
        ,a.token_id
        ,t.name as token_name
        ,t.description as token_description
    from public.node_outputs o
    left join public.node_assets a on o.box_id = a.box_id
    left join public.tokens t on a.token_id = t.token_id
    --where address = 'EfS5abyDe4vKFrJ48K5HnwTqa1ksn238bWFPe84bzVvCGvK1h2B7sgWLETtQuWwzVdBaoRZ1HcyzddrxLcsoM5YEy4UnqcLqMU1MDca1kLw9xbazAM6Awo9y6UVWTkQcS97mYkhkmx2Tewg3JntMgzfLWz5mACiEJEv7potayvk6awmLWS36sJMfXWgnEfNiqTyXNiPzt466cgot3GLcEsYXxKzLXyJ9EfvXpjzC2abTMzVSf1e17BHre4zZvDoAeTqr4igV3ubv2PtJjntvF2ibrDLmwwAyANEhw1yt8C8fCidkf3MAoPE6T53hX3Eb2mp3Xofmtrn4qVgmhNonnV8ekWZWvBTxYiNP8Vu5nc6RMDBv7P1c5rRc3tnDMRh2dUcDD7USyoB9YcvioMfAZGMNfLjWqgYu9Ygw2FokGBPThyWrKQ5nkLJvief1eQJg4wZXKdXWAR7VxwNftdZjPCHcmwn6ByRHZo9kb4Emv3rjfZE'
    where additional_registers->'R4'->>'sigmaType' = 'SLong' and additional_registers->'R5'->>'sigmaType' = 'SInt' and a.value = 1 and additional_registers->'R6'->>'sigmaType' = 'Coll[SByte]'
    order by o.address,o.additional_registers->'R5'->>'renderedValue',o.timestamp desc
)
,prep2 AS (
    select
        address
        ,token_id
        ,token_name
        ,token_description
    from prep
    group by address, token_id, token_name, token_description, (CAST(r5 as int)-CAST(COALESCE(r5_lag,'0') as int))
    having count(address) > 10
)
insert into delphi.discovery_live_epoch
select address,token_id,token_name,token_description from prep2 group by address,token_id,token_name,token_description --having count(token_id) = 1
$$;


--
-- Name: insert_oracles(); Type: PROCEDURE; Schema: delphi; Owner: -
--

CREATE PROCEDURE delphi.insert_oracles()
    LANGUAGE sql
    AS $$
truncate table delphi.oracles;    
with prep as (
    select 
        p.id as pool_id
        ,dp.token_id as participation_token_id
        ,oracle_address
    from delphi.discovery_datapoints dp
    inner join delphi.pools p on dp.pool_token_id = p.pool_nft_id 
    group by p.id, dp.token_id, oracle_address
)
insert into delphi.oracles (pool_id,participation_token_id,address_hash)
select p.pool_id, p.participation_token_id, oracle_address
from prep p
left outer join delphi.oracles o on o.participation_token_id = p.participation_token_id and o.pool_id = p.pool_id
where o.oracle_id is null
$$;


--
-- Name: insert_pools(); Type: PROCEDURE; Schema: delphi; Owner: -
--

CREATE PROCEDURE delphi.insert_pools()
    LANGUAGE sql
    AS $$
truncate table delphi.pools;    
with discovery_epoch_prep as (
    select  epoch_prep_address
    ,token_id
    ,token_name
    ,token_description
    ,row_number() over (partition by token_id order by epoch_prep_address) as rn
    from delphi.discovery_epoch_prep
)
,discovery_live_epoch as (
    select  live_epoch_address
    ,token_id
    ,token_name
    ,token_description
    ,row_number() over (partition by token_id order by live_epoch_address) as rn
    from delphi.discovery_live_epoch
)
,discovery_datapoints as (
    select  datapoint_address
    ,token_id as participant_token_id
    ,pool_token_id
    ,row_number() over (partition by pool_token_id order by datapoint_address) as rn
    from delphi.discovery_datapoints
)
,pools AS (
    select dep.token_id,dep.token_description,live_epoch_address,epoch_prep_address,datapoint_address,participant_token_id
    from discovery_epoch_prep dep
    left join discovery_live_epoch dle on dep.token_id = dle.token_id
    left join discovery_datapoints dd on dep.token_id = dd.pool_token_id
    where dep.rn = 1 and dle.rn = 1 and dd.rn = 1
)
insert into delphi.pools (pool_nft_id,name,live_epoch_address,epoch_prep_address,datapoint_address,participant_token_id)
select s.* from pools s
left outer join delphi.pools t on s.token_id = t.pool_nft_id
where t.pool_nft_id is null
$$;


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: datapoints; Type: TABLE; Schema: delphi; Owner: -
--

CREATE TABLE delphi.datapoints (
    box_id text NOT NULL,
    pool_id integer NOT NULL,
    oracle_id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    r4 text NOT NULL,
    r5 text NOT NULL,
    r6 text NOT NULL
);


--
-- Name: discovery_datapoints; Type: TABLE; Schema: delphi; Owner: -
--

CREATE TABLE delphi.discovery_datapoints (
    datapoint_address text NOT NULL,
    oracle_address text NOT NULL,
    token_id text NOT NULL,
    token_name text,
    token_description text,
    pool_token_id text
);


--
-- Name: discovery_epoch_prep; Type: TABLE; Schema: delphi; Owner: -
--

CREATE TABLE delphi.discovery_epoch_prep (
    epoch_prep_address text NOT NULL,
    token_id text NOT NULL,
    token_name text,
    token_description text
);


--
-- Name: discovery_live_epoch; Type: TABLE; Schema: delphi; Owner: -
--

CREATE TABLE delphi.discovery_live_epoch (
    live_epoch_address text NOT NULL,
    token_id text NOT NULL,
    token_name text,
    token_description text
);


--
-- Name: epoch_preps; Type: TABLE; Schema: delphi; Owner: -
--

CREATE TABLE delphi.epoch_preps (
    box_id text NOT NULL,
    pool_id integer NOT NULL,
    oracle_id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    r4 text NOT NULL,
    r5 text NOT NULL
);


--
-- Name: live_epochs; Type: TABLE; Schema: delphi; Owner: -
--

CREATE TABLE delphi.live_epochs (
    box_id text NOT NULL,
    pool_id integer NOT NULL,
    oracle_id integer NOT NULL,
    "timestamp" bigint NOT NULL,
    r4 text NOT NULL,
    r5 text NOT NULL,
    r6 text NOT NULL
);


--
-- Name: oracle_id_seq; Type: SEQUENCE; Schema: delphi; Owner: -
--

CREATE SEQUENCE delphi.oracle_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: oracles; Type: TABLE; Schema: delphi; Owner: -
--

CREATE TABLE delphi.oracles (
    pool_id integer NOT NULL,
    oracle_id integer DEFAULT nextval('delphi.oracle_id_seq'::regclass) NOT NULL,
    participation_token_id text NOT NULL,
    address text,
    address_hash text NOT NULL
);


--
-- Name: pools_id_seq; Type: SEQUENCE; Schema: delphi; Owner: -
--

CREATE SEQUENCE delphi.pools_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pools; Type: TABLE; Schema: delphi; Owner: -
--

CREATE TABLE delphi.pools (
    id integer DEFAULT nextval('delphi.pools_id_seq'::regclass) NOT NULL,
    name text,
    datapoint_address text NOT NULL,
    epoch_prep_address text NOT NULL,
    live_epoch_address text NOT NULL,
    deposits_address text,
    pool_nft_id text NOT NULL,
    participant_token_id text NOT NULL,
    deviation_range integer,
    consensus_number integer
);


--
-- Name: oracle_id_seq; Type: SEQUENCE SET; Schema: delphi; Owner: -
--

SELECT pg_catalog.setval('delphi.oracle_id_seq', 1, true);


--
-- Name: pools_id_seq; Type: SEQUENCE SET; Schema: delphi; Owner: -
--

SELECT pg_catalog.setval('delphi.pools_id_seq', 1, true);


--
-- Name: datapoints datapoints_box_id_pool_id_oracle_id; Type: CONSTRAINT; Schema: delphi; Owner: -
--

ALTER TABLE ONLY delphi.datapoints
    ADD CONSTRAINT datapoints_box_id_pool_id_oracle_id PRIMARY KEY (box_id, pool_id, oracle_id);


--
-- Name: epoch_preps epoch_preps_box_id_pool_id_oracle_id; Type: CONSTRAINT; Schema: delphi; Owner: -
--

ALTER TABLE ONLY delphi.epoch_preps
    ADD CONSTRAINT epoch_preps_box_id_pool_id_oracle_id PRIMARY KEY (box_id, pool_id, oracle_id);


--
-- Name: live_epochs live_epochs_box_id_pool_id_oracle_id; Type: CONSTRAINT; Schema: delphi; Owner: -
--

ALTER TABLE ONLY delphi.live_epochs
    ADD CONSTRAINT live_epochs_box_id_pool_id_oracle_id PRIMARY KEY (box_id, pool_id, oracle_id);


--
-- Name: oracles oracles_pkey; Type: CONSTRAINT; Schema: delphi; Owner: -
--

ALTER TABLE ONLY delphi.oracles
    ADD CONSTRAINT oracles_pkey PRIMARY KEY (pool_id, oracle_id);


--
-- Name: pools pools_pkey; Type: CONSTRAINT; Schema: delphi; Owner: -
--

ALTER TABLE ONLY delphi.pools
    ADD CONSTRAINT pools_pkey PRIMARY KEY (id);


--
-- Name: oracles oracles_pool_id_fkey; Type: FK CONSTRAINT; Schema: delphi; Owner: -
--

ALTER TABLE ONLY delphi.oracles
    ADD CONSTRAINT oracles_pool_id_fkey FOREIGN KEY (pool_id) REFERENCES delphi.pools(id);
