import logging
import os
import random
import uuid
from contextlib import asynccontextmanager

import psycopg
from fastapi import FastAPI, HTTPException, Request
from msgspec import json
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from saga_workflow import FINAL_STATUSES, start_checkout


DB_ERROR_STR = "DB error"
service_name = "order"


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
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS orders (
                        order_id TEXT PRIMARY KEY,
                        paid BOOLEAN NOT NULL,
                        items JSONB NOT NULL,
                        user_id TEXT NOT NULL,
                        total_cost INTEGER NOT NULL
                    )
                    """
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sagas (
                        order_id TEXT NOT NULL,
                        id TEXT PRIMARY KEY,
                        status TEXT NOT NULL,
                        step TEXT NOT NULL,
                        results JSONB,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        updated_at TIMESTAMPTZ DEFAULT now()
                    )
                    """
                )
                await cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_sagas_order_status ON sagas(order_id, status)"
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS outbox (
                        id TEXT PRIMARY KEY,
                        event_id TEXT NOT NULL UNIQUE,
                        topic TEXT NOT NULL,
                        message_key TEXT NOT NULL,
                        correlation_id TEXT NOT NULL,
                        payload JSONB NOT NULL,
                        headers JSONB,
                        status TEXT NOT NULL DEFAULT 'PENDING',
                        publish_attempts INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMPTZ DEFAULT now(),
                        published_at TIMESTAMPTZ,
                        last_error TEXT
                    )
                    """
                )
                await cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS event_id TEXT")
                await cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS message_key TEXT")
                await cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS correlation_id TEXT")
                await cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS headers JSONB")
                await cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING'")
                await cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS publish_attempts INTEGER DEFAULT 0")
                await cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ")
                await cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS last_error TEXT")
                await cur.execute("UPDATE outbox SET status='PENDING' WHERE status IS NULL")
                await cur.execute("UPDATE outbox SET event_id = id WHERE event_id IS NULL")
                await cur.execute("UPDATE outbox SET correlation_id = COALESCE(correlation_id, '')")
                await cur.execute("UPDATE outbox SET message_key = COALESCE(message_key, correlation_id, '')")
                await cur.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_order_outbox_event_id ON outbox(event_id)"
                )
                await cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_outbox_pending_created ON outbox(status, created_at)"
                )
                await cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS inbox (
                        id TEXT PRIMARY KEY,
                        event_id TEXT NOT NULL UNIQUE,
                        topic TEXT NOT NULL,
                        partition INTEGER,
                        kafka_offset BIGINT,
                        correlation_id TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'RECEIVED',
                        payload_hash TEXT,
                        payload JSONB,
                        error TEXT,
                        received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                        processed_at TIMESTAMPTZ
                    )
                    """
                )
                await cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_inbox_correlation_id ON inbox(correlation_id)"
                )
                await cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_inbox_status_received ON inbox(status, received_at)"
                )
            await conn.commit()


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with AsyncConnectionPool(conninfo=_conninfo(), min_size=1, max_size=10) as pool:
        app.state.pool = pool
        yield


app = FastAPI(title="order-service", lifespan=lifespan)


@app.post("/create/{user_id}")
async def create_order(user_id: str, request: Request):
    key = str(uuid.uuid4())
    pool: AsyncConnectionPool = request.app.state.pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO orders (order_id, paid, items, user_id, total_cost) VALUES (%s, %s, %s::jsonb, %s, %s)",
                    (key, False, "[]", user_id, 0),
                )
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"order_id": key}


@app.post("/batch_init/{n}/{n_items}/{n_users}/{item_price}")
async def batch_init_orders(n: int, n_items: int, n_users: int, item_price: int, request: Request):
    n, n_items, n_users, item_price = int(n), int(n_items), int(n_users), int(item_price)
    pool: AsyncConnectionPool = request.app.state.pool

    def generate_entry(order_id: int):
        user_id = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        items = [
            {"item_id": f"{item1_id}", "quantity": 1},
            {"item_id": f"{item2_id}", "quantity": 1},
        ]
        return (f"{order_id}", False, json.encode(items).decode(), f"{user_id}", 2 * item_price)

    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                values = [generate_entry(i) for i in range(n)]
                await cur.executemany(
                    "INSERT INTO orders (order_id, paid, items, user_id, total_cost) VALUES (%s, %s, %s::jsonb, %s, %s)",
                    values,
                )
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)
    return {"msg": "Batch init for orders successful"}


@app.get("/find/{order_id}")
async def find_order(order_id: str, request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT paid, items, user_id, total_cost FROM orders WHERE order_id = %s",
                    (order_id,),
                )
                row = await cur.fetchone()
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    if row is None:
        raise HTTPException(status_code=400, detail=f"Order: {order_id} not found!")

    items = [(item["item_id"], item["quantity"]) for item in row["items"]]
    return {
        "order_id": order_id,
        "paid": row["paid"],
        "items": items,
        "user_id": row["user_id"],
        "total_cost": row["total_cost"],
    }


@app.get("/routes")
async def list_routes(request: Request):
    return {"routes": [str(route.path) for route in request.app.routes]}


@app.post("/addItem/{order_id}/{item_id}/{quantity}")
async def add_item(order_id: str, item_id: str, quantity: int, request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT items, total_cost FROM orders WHERE order_id = %s",
                    (order_id,),
                )
                row = await cur.fetchone()
                if row is None:
                    raise HTTPException(status_code=400, detail=f"Order: {order_id} not found!")

                items = list(row["items"])
                items.append({"item_id": item_id, "quantity": quantity})
                await cur.execute(
                    "UPDATE orders SET items = %s::jsonb WHERE order_id = %s",
                    (json.encode(items).decode(), order_id),
                )
    except HTTPException:
        raise
    except psycopg.Error as e:
        raise HTTPException(status_code=400, detail=f"Database error: {str(e)}")

    return {"order_id": order_id, "items": items, "item_id": item_id}


@app.post("/checkout/start/{order_id}")
async def checkout_start(order_id: str, request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT user_id, items FROM orders WHERE order_id = %s",
                    (order_id,),
                )
                row = await cur.fetchone()
                if row is None:
                    raise HTTPException(status_code=400, detail=f"Order: {order_id} not found!")

                items_list = [(item["item_id"], int(item["quantity"])) for item in (row["items"] or [])]
                if not items_list:
                    raise HTTPException(status_code=400, detail="Order has no items.")

                correlation_id, created = await start_checkout(
                    cur,
                    order_id=order_id,
                    user_id=row["user_id"],
                    items=items_list,
                )
                await conn.commit()
    except HTTPException:
        raise
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    return {
        "status": "running",
        "order_id": order_id,
        "correlation_id": correlation_id,
        "created": created,
    }


@app.get("/checkout/status/{correlation_id}")
async def checkout_status(correlation_id: str, request: Request):
    pool: AsyncConnectionPool = request.app.state.pool
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT id, order_id, status, step, results FROM sagas WHERE id = %s",
                    (correlation_id,),
                )
                row = await cur.fetchone()
    except psycopg.Error:
        raise HTTPException(status_code=400, detail=DB_ERROR_STR)

    if row is None:
        return {"status": "not_found", "correlation_id": correlation_id}

    saga_status = row["status"]
    http_status = 200 if saga_status in FINAL_STATUSES else 202
    from fastapi.responses import JSONResponse
    return JSONResponse(
        status_code=http_status,
        content={
            "status": saga_status,
            "order_id": row["order_id"],
            "step": row["step"],
            "correlation_id": row["id"],
            "results": row.get("results") or {},
        },
    )


@app.post("/checkout/{order_id}")
async def checkout(order_id: str, request: Request):
    return await checkout_start(order_id, request)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    logging.getLogger().handlers = gunicorn_logger.handlers
    logging.getLogger().setLevel(gunicorn_logger.level)
