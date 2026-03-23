import atexit
import logging
import os
import random
import uuid

import psycopg  # type: ignore
from flask import Flask, abort, jsonify
from msgspec import Struct, json
from psycopg.rows import dict_row  # type: ignore
from psycopg_pool import ConnectionPool  # type: ignore

from producer import OutboxRelay
from saga_workflow import FINAL_STATUSES, start_checkout

DB_ERROR_STR = "DB error"

app = Flask("order-service")

service_name = "order"
kafka_producer = None
outbox_relay: OutboxRelay | None = None

conn_params = {
    "host": os.environ["POSTGRES_HOST"],
    "port": int(os.environ["POSTGRES_PORT"]),
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
    "dbname": os.environ["POSTGRES_DB"],
}

db_pool: ConnectionPool = None


def init_db_pool():
    return ConnectionPool(
        conninfo=(
            f"host={conn_params['host']} port={conn_params['port']} "
            f"user={conn_params['user']} password={conn_params['password']} "
            f"dbname={conn_params['dbname']}"
        ),
        min_size=1,
        max_size=10,
        reconnect_timeout=30,
        kwargs={"connect_timeout": 10},
    )


def init_db():
    tmp_pool = init_db_pool()
    with tmp_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
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
            cur.execute(
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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sagas_order_status ON sagas(order_id, status)")
            cur.execute(
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
            cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS event_id TEXT")
            cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS message_key TEXT")
            cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS correlation_id TEXT")
            cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS headers JSONB")
            cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING'")
            cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS publish_attempts INTEGER DEFAULT 0")
            cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ")
            cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS last_error TEXT")
            cur.execute("UPDATE outbox SET status='PENDING' WHERE status IS NULL")
            cur.execute("UPDATE outbox SET event_id = id WHERE event_id IS NULL")
            cur.execute("UPDATE outbox SET correlation_id = COALESCE(correlation_id, '')")
            cur.execute("UPDATE outbox SET message_key = COALESCE(message_key, correlation_id, '')")
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_order_outbox_event_id ON outbox(event_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_outbox_pending_created ON outbox(status, created_at)")
            cur.execute(
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
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inbox_correlation_id ON inbox(correlation_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_inbox_status_received ON inbox(status, received_at)")
    tmp_pool.close()


def start_outbox_relay():
    global outbox_relay
    if outbox_relay is not None:
        return
    if db_pool is None or kafka_producer is None:
        app.logger.warning("Outbox relay not started: db_pool or kafka_producer not initialized")
        return
    outbox_relay = OutboxRelay(db_pool=db_pool, kafka_producer=kafka_producer)
    outbox_relay.start()


def stop_outbox_relay():
    global outbox_relay
    if outbox_relay is None:
        return
    outbox_relay.stop()
    outbox_relay = None


def close_db_connection():
    stop_outbox_relay()
    if db_pool is not None:
        db_pool.close()


init_db()
atexit.register(close_db_connection)


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue | None:
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT paid, items, user_id, total_cost FROM orders WHERE order_id = %s",
                    (order_id,),
                )
                row = cur.fetchone()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)

    if row is None:
        abort(400, f"Order: {order_id} not found!")

    items = [(item["item_id"], item["quantity"]) for item in row["items"]]
    return OrderValue(
        paid=row["paid"],
        items=items,
        user_id=row["user_id"],
        total_cost=row["total_cost"],
    )


@app.post("/create/<user_id>")
def create_order(user_id: str):
    key = str(uuid.uuid4())
    empty_items = []
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO orders (order_id, paid, items, user_id, total_cost) VALUES (%s, %s, %s::jsonb, %s, %s)",
                    (key, False, empty_items, user_id, 0),
                )
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({"order_id": key})


@app.post("/batch_init/<n>/<n_items>/<n_users>/<item_price>")
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n = int(n)
    n_items = int(n_items)
    n_users = int(n_users)
    item_price = int(item_price)

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
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                values = [generate_entry(i) for i in range(n)]
                cur.executemany(
                    "INSERT INTO orders (order_id, paid, items, user_id, total_cost) VALUES (%s, %s, %s::jsonb, %s, %s)",
                    values,
                )
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get("/find/<order_id>")
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)  # type: ignore
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost,
        }
    )


@app.get("/routes")
def list_routes():
    return {"routes": [str(rule) for rule in app.url_map.iter_rules()]}


@app.get("/test/outbox")
def test_outbox():
    with db_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT id, event_id, topic, message_key, correlation_id, status, publish_attempts
                FROM outbox
                ORDER BY created_at ASC
                LIMIT 10
                """
            )
            rows = cur.fetchall()
            return jsonify(rows)


@app.post("/addItem/<order_id>/<item_id>/<int:quantity>")
def add_item(order_id: str, item_id: str, quantity: int):
    app.logger.info("item=%s quantity=%s order=%s", item_id, quantity, order_id)
    order_entry: OrderValue = get_order_from_db(order_id)  # type: ignore
    items_json = [{"item_id": item[0], "quantity": item[1]} for item in order_entry.items]
    items_json.append({"item_id": item_id, "quantity": quantity})
    serialized_items = json.encode(items_json).decode()
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "UPDATE orders SET items = %s::jsonb, total_cost = %s WHERE order_id = %s",
                    (serialized_items, order_entry.total_cost, order_id),
                )
    except psycopg.Error as e:
        app.logger.exception("Database error while updating order %s", order_id)
        return abort(400, f"Database error: {str(e)}")

    items_as_dicts = [{"item_id": item[0], "quantity": item[1]} for item in order_entry.items]
    return jsonify({"order_id": order_id, "items": items_as_dicts, "user_id": order_entry.user_id}), 200


@app.post("/checkout/start/<order_id>")
def checkout_start(order_id: str):
    order_value: OrderValue = get_order_from_db(order_id)  # type: ignore
    items_list = [(item_id, qty) for item_id, qty in order_value.items]
    if not items_list:
        return {"status": "failed", "message": "Order has no items."}, 400

    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                correlation_id, created = start_checkout(
                    cur,
                    order_id=order_id,
                    user_id=order_value.user_id,
                    items=items_list,
                )
                conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)

    app.logger.info(
        "Checkout start order_id=%s correlation_id=%s created=%s",
        order_id,
        correlation_id,
        created,
    )
    return {
        "status": "running",
        "order_id": order_id,
        "correlation_id": correlation_id,
        "created": created,
    }, 202


@app.get("/checkout/status/<correlation_id>")
def checkout_status(correlation_id: str):
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT id, order_id, status, step, results
                    FROM sagas
                    WHERE id = %s
                    """,
                    (correlation_id,),
                )
                row = cur.fetchone()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)

    if row is None:
        return {"status": "not_found", "correlation_id": correlation_id}, 404

    saga_status = row["status"]
    http_status = 200 if saga_status in FINAL_STATUSES else 202
    return {
        "status": saga_status,
        "order_id": row["order_id"],
        "step": row["step"],
        "correlation_id": row["id"],
        "results": row.get("results") or {},
    }, http_status


@app.post("/checkout/<order_id>")
def checkout(order_id: str):
    return checkout_start(order_id)


if __name__ == "__main__":
    start_outbox_relay()
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger("gunicorn.error")
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
