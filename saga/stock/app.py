import logging
import os
import atexit
import uuid

import psycopg # type: ignore
from psycopg_pool import ConnectionPool # type: ignore
from psycopg.rows import dict_row # type: ignore
from collections import defaultdict

from msgspec import Struct, json
from flask import Flask, jsonify, abort, Response
from services import utils
from db_repository import StockRepository


DB_ERROR_STR = "DB error"

app = Flask("payment-service")
service_name = "stock"

kafka_producer = None
kafka_consumer = None


conn_params = {
    'host': os.environ['POSTGRES_HOST'],
    'port': int(os.environ['POSTGRES_PORT']),
    'user': os.environ['POSTGRES_USER'],
    'password': os.environ['POSTGRES_PASSWORD'],
    'dbname': os.environ['POSTGRES_DB']
}

db_pool: ConnectionPool = None


def init_db_pool():
        
    return ConnectionPool(
        conninfo=f"host={conn_params['host']} port={conn_params['port']} "
                f"user={conn_params['user']} password={conn_params['password']} "
                f"dbname={conn_params['dbname']}",
        min_size=1,
        max_size=10,
        reconnect_timeout=30,
        kwargs={"connect_timeout": 10}
    )

def init_db():
    """Initialize stock persistence tables."""
    tmp_pool = init_db_pool()
    with tmp_pool.connection() as conn:
        with conn.cursor() as cur:
            repo = StockRepository(cur)
            repo.create_tables()
    tmp_pool.close()

def close_db_connection():
    db_pool.close()


# Initialize database on startup
if os.getenv("INIT_DB", "true").lower() == "true":
    init_db()
atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int


def _cleanup_inbox() -> None:
    """Delete old processed inbox rows to prevent table bloat."""
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM inbox
                    WHERE status = 'PROCESSED'
                    AND processed_at < now() - interval '10 minutes'
                    """
                )
            conn.commit()
            app.logger.debug("Stock inbox cleanup completed")
    except Exception as exc:
        app.logger.warning("Stock inbox cleanup failed: %s", exc)


def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                repo = StockRepository(cur)
                row = repo.get_inventory_item(item_id)

                if row is not None:
                    return StockValue(stock=row['stock'], price=row['price'])
                abort(400, f"Item: {item_id} not found!")

    except psycopg.Error:
        return abort(400, DB_ERROR_STR)


def consume_messages(consumer):
    """Process Kafka messages with one DB transaction per event."""
    app.logger.info("Stock consumer started")
    message_count = 0
    for message in consumer:
        message_count += 1
        if message_count % 500 == 0:
            _cleanup_inbox()

        result = utils.decode_and_type_event(message)
        if isinstance(result, utils.Failure):
            app.logger.error("Stock decode failed: %s", result.error)
            continue

        event = result.value
        correlation_id = utils.event_correlation_id(event)
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                repo = StockRepository(cur)
                if repo.inbox_event_exists(event.id):
                    inbox_result = repo.get_inbox_event_result(event.id) or {
                        "status": "failure",
                        "reason": "duplicate_event",
                    }
                    _emit_stock_result(repo, event, correlation_id, inbox_result)
                    conn.commit()
                    continue

                raw_payload = message.value.decode() if isinstance(message.value, bytes) else str(message.value)
                repo.insert_inbox_event(
                    event_id=event.id,
                    topic=message.topic,
                    partition=message.partition,
                    kafka_offset=message.offset,
                    correlation_id=correlation_id,
                    payload=event,
                    payload_hash=utils.payload_hash(raw_payload),
                )

                if event.event_type == utils.Commands.RESERVE_STOCK:
                    checkout_result = _reserve_stock(repo, event)
                elif event.event_type == utils.Commands.FREE_STOCK:
                    checkout_result = _free_stock(repo, event)
                else:
                    checkout_result = {"status": "failure", "reason": "unsupported_event_type"}

                repo.set_inbox_event_result(
                    event_id=event.id,
                    status="PROCESSED",
                    result=checkout_result,
                    error=checkout_result.get("reason") if checkout_result.get("status") == "failure" else None,
                )
                _emit_stock_result(repo, event, correlation_id, checkout_result)
                conn.commit()


def _reserve_stock(repo: StockRepository, event: utils.BaseEvent) -> dict[str, int | str]:
    items = sorted(event.payload.items, key=lambda x: x[0])
    if not items:
        return {"status": "failure", "reason": "no_items"}

    max_retries = 3
    for _ in range(max_retries):
        item_ids = [item_id for item_id, _ in items]
        rows = repo.get_inventory_items_for_ids(item_ids, include_price=True)

        missing = [item_id for item_id, _ in items if item_id not in rows]
        if missing:
            return {"status": "failure", "reason": "items_not_found"}

        unavailable = [item_id for item_id, qty in items if int(rows[item_id]["stock"]) < int(qty)]
        if unavailable:
            return {"status": "failure", "reason": "insufficient_stock"}

        total_cost = 0
        conflict = False
        for item_id, qty in items:
            row = rows[item_id]
            current_version = int(row["version"])
            new_stock = int(row["stock"]) - int(qty)
            new_version = current_version + 1
            if not repo.update_inventory_item_versioned(item_id, new_stock, new_version, current_version):
                conflict = True
                break
            total_cost += int(row["price"]) * int(qty)

        if not conflict:
            return {"status": "success", "amount": total_cost}

    return {"status": "failure", "reason": "version_conflict"}


def _free_stock(repo: StockRepository, event: utils.BaseEvent) -> dict[str, str]:
    qty_by_item: dict[str, int] = defaultdict(int)
    for item_id, qty in event.payload.items:
        qty_by_item[item_id] += int(qty)
    items = sorted(qty_by_item.items(), key=lambda x: x[0])

    max_retries = 3
    for _ in range(max_retries):
        item_ids = [item_id for item_id, _ in items]
        rows = repo.get_inventory_items_for_ids(item_ids)

        conflict = False
        for item_id, qty in items:
            row = rows.get(item_id)
            if row is None:
                continue
            current_version = int(row["version"])
            new_stock = int(row["stock"]) + int(qty)
            new_version = current_version + 1
            if not repo.update_inventory_item_versioned(item_id, new_stock, new_version, current_version):
                conflict = True
                break

        if not conflict:
            return {"status": "success"}

    return {"status": "failure", "reason": "version_conflict"}


def _emit_stock_result(
    repo: StockRepository,
    event: utils.BaseEvent,
    correlation_id: str,
    result: dict[str, int | str],
) -> None:
    if event.event_type == utils.Commands.RESERVE_STOCK:
        if result.get("status") == "success":
            response_event = utils.build_stock_allocated_event(
                correlation_id=correlation_id,
                order_id=event.payload.order_id,
                amount=int(result.get("amount", 0)),
            )
        else:
            response_event = utils.build_stock_unavailable_event(
                correlation_id=correlation_id,
                order_id=event.payload.order_id,
            )
        response_event.id = f"stock-result:{event.id}"
        repo.insert_outbox_message("order.request", response_event)

    if event.event_type == utils.Commands.FREE_STOCK and result.get("status") == "success":
        response_event = utils.build_stock_freed_event(
            correlation_id=correlation_id,
            order_id=event.payload.order_id,
        )
        response_event.id = f"stock-compensation:{event.id}"
        repo.insert_outbox_message("order.request", response_event)


@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                repo = StockRepository(cur)
                repo.insert_inventory_item(key, 0, int(price), 1)
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.get('/items')
def get_items():
    with db_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            repo = StockRepository(cur)
            items = repo.list_inventory()
            return jsonify(items)


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                repo = StockRepository(cur)
                for i in range(n):
                    item_id = str(i)
                    repo.insert_inventory_item(item_id, starting_stock, item_price, 1)
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id) # type: ignore
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )

@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    repo = StockRepository(cur)
                    row = repo.get_inventory_item(item_id)
                    if row is None:
                        return abort(400, f"Item: {item_id} not found!")

                    current_version = int(row['version'])
                    new_stock = int(row['stock']) + int(amount)
                    new_version = current_version + 1

                    if not repo.update_inventory_item_versioned(item_id, new_stock, new_version, current_version):
                        conn.rollback()
                        app.logger.warning(f"Version conflict for item {item_id}, retry {attempt + 1}/{MAX_RETRIES}")
                        continue

                    return Response(f"Item: {item_id} stock updated to: {new_stock}", status=200)

        except psycopg.Error:
            return abort(400, DB_ERROR_STR)

    return abort(409, "Too many concurrent updates, please retry")


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    repo = StockRepository(cur)
                    row = repo.get_inventory_item(item_id)
                    if row is None:
                        return abort(400, f"Item: {item_id} not found!")

                    current_stock = int(row['stock'])
                    current_version = int(row['version'])
                    new_stock = current_stock - int(amount)

                    if new_stock < 0:
                        return abort(400, f"Item: {item_id} stock cannot get reduced below zero!")

                    new_version = current_version + 1

                    if not repo.update_inventory_item_versioned(item_id, new_stock, new_version, current_version):
                        conn.rollback()
                        app.logger.warning(f"Version conflict for item {item_id}, retry {attempt + 1}/{MAX_RETRIES}")
                        continue

                    app.logger.debug(f"Item: {item_id} stock updated to: {new_stock}")
                    return Response(f"Item: {item_id} stock updated to: {new_stock}", status=200)

        except psycopg.Error:
            return abort(400, DB_ERROR_STR)

    return abort(409, "Too many concurrent updates, please retry")


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
