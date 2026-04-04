import logging
import os
import uuid
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from db_repository import PaymentRepository


DB_ERROR_STR = "DB error"
service_name = "payment"


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
                    repo = PaymentRepository(cur)
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


app = FastAPI(title="payment-service", lifespan=lifespan)


@app.post("/create_user")
async def create_user(request: Request):
    key = str(uuid.uuid4())
    pool: AsyncConnectionPool = request.app.state.pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                repo = PaymentRepository(cur)
                await repo.insert_account(key, 0)
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"user_id": key}


@app.post("/batch_init/{n}/{starting_money}")
async def batch_init_users(n: int, starting_money: int, request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                repo = PaymentRepository(cur)
                for i in range(int(n)):
                    await repo.upsert_account(str(i), int(starting_money), 1)
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"msg": "Batch init for users successful"}


@app.get("/find_user/{user_id}")
async def find_user(user_id: str, request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                repo = PaymentRepository(cur)
                row = await repo.get_account(user_id)
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    if row is None:
        raise HTTPException(status_code=400, detail=f"User: {user_id} not found!")
    return {"user_id": user_id, "credit": row["credit"]}


@app.post("/pay/{user_id}/{amount}")
async def http_remove_credit(user_id: str, amount: int, request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    for _ in range(3):
        try:
            async with pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    repo = PaymentRepository(cur)
                    row = await repo.get_account(user_id)
                    if row is None:
                        raise HTTPException(status_code=400, detail=f"User: {user_id} not found!")

                    current_credit = int(row["credit"])
                    current_version = int(row["version"])
                    if current_credit - int(amount) < 0:
                        raise HTTPException(status_code=400, detail="Insufficient credit")

                    new_credit = current_credit - int(amount)
                    new_version = current_version + 1
                    if await repo.update_account_versioned(user_id, new_credit, new_version, current_version):
                        return PlainTextResponse(f"User: {user_id} credit updated to: {new_credit}")

        except HTTPException:
            raise
        except psycopg.Error:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    raise HTTPException(status_code=409, detail="Too many concurrent updates, please retry")


@app.post("/add_funds/{user_id}/{amount}")
async def add_credit(user_id: str, amount: int, request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    for _ in range(3):
        try:
            async with pool.connection() as conn:
                async with conn.cursor(row_factory=dict_row) as cur:
                    repo = PaymentRepository(cur)
                    row = await repo.get_account(user_id)
                    if row is None:
                        raise HTTPException(status_code=400, detail=f"User: {user_id} not found!")

                    new_credit = int(row["credit"]) + int(amount)
                    current_version = int(row["version"])
                    new_version = current_version + 1

                    if not await repo.update_account_versioned(user_id, new_credit, new_version, current_version):
                        continue

                    return PlainTextResponse(f"User: {user_id} credit updated to: {new_credit}")

        except HTTPException:
            raise
        except psycopg.Error:
            raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    raise HTTPException(status_code=409, detail="Too many concurrent updates, please retry")


@app.get("/users")
async def get_users(request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                repo = PaymentRepository(cur)
                rows = await repo.list_accounts()
                return rows
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    logging.getLogger().handlers = gunicorn_logger.handlers
    logging.getLogger().setLevel(gunicorn_logger.level)
