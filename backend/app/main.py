#region imports
import asyncpg
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


from api.v1.routes.pools import pools_router
#endregion

#region main
app = FastAPI(
    title="delphiproject"
)

origins = [
    "https://*.delphiproject.org/"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    # allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    dsn = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_SERVER')}/{os.getenv('POSTGRES_DB')}"
    app.state.db = await asyncpg.create_pool(dsn)

app.include_router(pools_router,      prefix="/api/v1/pools",      tags=["pools"])

@app.get("/")
async def root():
    return {"message": "Hello World"}
#endregion