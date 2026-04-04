import logging
import os
import uuid
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from db_repository import StockRepository


DB_ERROR_STR = "DB error"
service_name = "stock"


def _conninfo() -> str:
    return (
        f"host={os.environ['POSTGRES_HOST']} "
        f"port={os.environ['POSTGRES_PORT']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']} "
        f"dbname={os.environ['POSTGRES_DB']}"
    )


async def init_db() -> None:
    async with AsyncConnectionPool(conninfo=_conninfo(), min_size=1, max_size=2) as tmp_pool:
        async with tmp_pool.connection() as conn:
            try:
                async with conn.cursor() as cur:
                    repo = StockRepository(cur)
                    await repo.create_tables()
                await conn.commit()
            except psycopg.errors.UniqueViolation:
                await conn.rollback()
                logging.getLogger(__name__).warning("Tables already created by another worker, skipping")


@asynccontextmanager
async def lifespan(app: FastAPI):
    if os.getenv("INIT_DB", "true").lower() == "true":
        await init_db()
    async with AsyncConnectionPool(conninfo=_conninfo(), min_size=1, max_size=10) as pool:
        app.state.pool = pool
        yield


app = FastAPI(title="stock-service", lifespan=lifespan)


@app.post("/item/create/{price}")
async def create_item(price: int, request: Request):
    key = str(uuid.uuid4())
    pool: AsyncConnectionPool = request.app.state.pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                repo = StockRepository(cur)
                await repo.insert_inventory_item(key, 0, int(price), 1)
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"item_id": key}


@app.get("/items")
async def get_items(request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            repo = StockRepository(cur)
            items = await repo.list_inventory()
            return items


@app.post("/batch_init/{n}/{starting_stock}/{item_price}")
async def batch_init_users(n: int, starting_stock: int, item_price: int, request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                repo = StockRepository(cur)
                for i in range(int(n)):
                    await repo.insert_inventory_item(str(i), int(starting_stock), int(item_price), 1)
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"msg": "Batch init for stock successful"}


@app.get("/find/{item_id}")
async def find_item(item_id: str, request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                repo = StockRepository(cur)
                row = await repo.get_inventory_item(item_id)
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    if row is None:
        raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!")
    return {"stock": row["stock"], "price": row["price"]}


@app.post("/add/{item_id}/{amount}")
async def add_stock(item_id: str, amount: int, request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    for _ in range(3):
        try:
            async with pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    repo = StockRepository(cur)
                    row = await repo.get_inventory_item(item_id)
                    if row is None:
                        raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!")

                    current_version = int(row["version"])
                    new_stock = int(row["stock"]) + int(amount)
                    new_version = current_version + 1

                    if not await repo.update_inventory_item_versioned(item_id, new_stock, new_version, current_version):
                        continue

                    return PlainTextResponse(f"Item: {item_id} stock updated to: {new_stock}")

        except HTTPException:
            raise
        except psycopg.Error:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    raise HTTPException(status_code=409, detail="Too many concurrent updates, please retry")


@app.post("/subtract/{item_id}/{amount}")
async def remove_stock(item_id: str, amount: int, request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    for _ in range(3):
        try:
            async with pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    repo = StockRepository(cur)
                    row = await repo.get_inventory_item(item_id)
                    if row is None:
                        raise HTTPException(status_code=400, detail=f"Item: {item_id} not found!")

                    current_stock = int(row["stock"])
                    current_version = int(row["version"])
                    new_stock = current_stock - int(amount)

                    if new_stock < 0:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Item: {item_id} stock cannot get reduced below zero!",
                        )

                    new_version = current_version + 1
                    if not await repo.update_inventory_item_versioned(item_id, new_stock, new_version, current_version):
                        continue

                    return PlainTextResponse(f"Item: {item_id} stock updated to: {new_stock}")

        except HTTPException:
            raise
        except psycopg.Error:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    raise HTTPException(status_code=409, detail="Too many concurrent updates, please retry")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    logging.getLogger().handlers = gunicorn_logger.handlers
    logging.getLogger().setLevel(gunicorn_logger.level)
