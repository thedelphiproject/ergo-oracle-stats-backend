#region imports
from optparse import Option
from fastapi import APIRouter, Request, Query, Path
from typing import Optional
#endregion

#region main
pools_router = r = APIRouter()

@r.get(
    "/"
)
async def get_pools(request: Request):
    """
    Get available pools
    """
    query = """
        select  id
            ,name
            ,datapoint_address
            ,epoch_prep_address
            ,live_epoch_address
            ,deposits_address
            ,pool_nft_id
            ,participant_token_id
            ,total_participant_tokens
        from delphi.pools
    """
    async with request.app.state.db.acquire() as conn:
        res = await conn.fetch(query)
    return res

@r.get(
     "/{pool_id}"
)
async def get_pool_id(
        request: Request,
        pool_id: int = Path(..., title='Id of oracle pool.',ge=1),
    ):
    """
    Get details for pool
    """
    query = f"""
        select  
            id
            ,name
            ,datapoint_address
            ,epoch_prep_address
            ,live_epoch_address
            ,deposits_address
            ,pool_nft_id
            ,participant_token_id
            ,total_participant_tokens
            ,count(oracle_id) as oracles
        from delphi.pools p
        left join delphi.oracles o on p.id = o.pool_id
        group by 
            id
            ,name
            ,datapoint_address
            ,epoch_prep_address
            ,live_epoch_address
            ,deposits_address
            ,pool_nft_id
            ,participant_token_id
            ,total_participant_tokens
    """
    async with request.app.state.db.acquire() as conn:
        res = await conn.fetch(query)
    return res

#endregion