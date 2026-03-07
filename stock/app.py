import logging
import os
import atexit
import threading
import time
import uuid
import json

import psycopg
import requests
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
from msgspec import Struct
from flask import Flask, jsonify, abort, Response, request

from kafka_service import kafka_client
from kafka_service.kafka_event import BaseEvent, StockPayload
from kafka_service.consumer_handler import EventConsumer

DB_ERROR_STR = "DB error"

app = Flask("stock-service")
service_name = "stock"

order_kafka = kafka_client.Client(
    service_name,
    ['order.events', 'stock.events', 'payment.events']
)
kafka_producer, kafka_consumer = order_kafka.producer, order_kafka.consumer


# DB pool

db_pool = ConnectionPool(
    conninfo=(
        f"host={os.environ['POSTGRES_HOST']} "
        f"port={os.environ['POSTGRES_PORT']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']} "
        f"dbname={os.environ['POSTGRES_DB']}"
    ),
    min_size=1,
    max_size=10,
    reconnect_timeout=30,
    kwargs={"connect_timeout": 10}
)

event_consumer = EventConsumer(kafka_consumer, db_pool, service_name)

# DB initalization


def init_db():
    with db_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS items (
                    item_id TEXT    PRIMARY KEY,
                    stock   INTEGER NOT NULL,
                    price   INTEGER NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_transactions (
                    transaction_id TEXT  PRIMARY KEY,
                    order_id       TEXT  NOT NULL,
                    status         TEXT  NOT NULL DEFAULT 'PREPARED',
                    items_reserved JSONB NOT NULL,
                    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_reservations (
                    reservation_id TEXT    PRIMARY KEY,
                    transaction_id TEXT    NOT NULL,
                    item_id        TEXT    NOT NULL,
                    quantity       INTEGER NOT NULL,
                    FOREIGN KEY (transaction_id)
                        REFERENCES stock_transactions(transaction_id) ON DELETE CASCADE
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS event_log (
                    correlation_id TEXT   PRIMARY KEY,
                    event_type     TEXT   NOT NULL,
                    service        TEXT   NOT NULL,
                    topic          TEXT   NOT NULL,
                    kafka_offset   BIGINT NOT NULL,
                    payload        JSONB  NOT NULL,
                    received_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()


def close_db_connection():
    db_pool.close()


init_db()
atexit.register(close_db_connection)
def consume_messages():
    event_consumer._run()




class StockValue(Struct):
    stock: int
    price: int


def get_item_from_db(item_id: str) -> StockValue:
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT stock, price FROM items WHERE item_id = %s", (item_id,))
                row = cur.fetchone()
    except psycopg.Error:
        abort(400, DB_ERROR_STR)
    if row is None:
        abort(400, f"Item: {item_id} not found!")
    return StockValue(stock=row['stock'], price=row['price'])




@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO items (item_id, stock, price) VALUES (%s, %s, %s)",
                    (key, 0, int(price))
                )
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)

    kafka_producer.send(topic="stock.events", value=BaseEvent.create(
        event_type="CREATE_ITEM",
        payload=StockPayload(order_id="", item_id=key),
    ))
    return jsonify({'item_id': key})


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n, starting_stock, item_price = int(n), int(starting_stock), int(item_price)
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO items (item_id, stock, price) VALUES (%s, %s, %s)",
                    [(f"{i}", starting_stock, item_price) for i in range(n)]
                )
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item = get_item_from_db(item_id)
    return jsonify({"stock": item.stock, "price": item.price})


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry = get_item_from_db(item_id)
    item_entry.stock += int(amount)
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE items SET stock = %s WHERE item_id = %s",
                            (item_entry.stock, item_id))
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)

    kafka_producer.send(topic="stock.events", value=BaseEvent.create(
        event_type="RELEASE_STOCK",
        payload=StockPayload(order_id="", item_id=item_id),
    ))
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry = get_item_from_db(item_id)
    item_entry.stock -= int(amount)
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE items SET stock = %s WHERE item_id = %s",
                            (item_entry.stock, item_id))
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)

    kafka_producer.send(topic="stock.events", value=BaseEvent.create(
        event_type="STOCK_PROCESSED",
        payload=StockPayload(order_id="", item_id=item_id),
    ))
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


#2PC Endpoints

@app.post('/prepare/<transaction_id>')
def prepare_stock(transaction_id: str):
    try:
        data     = request.get_json() or {}
        order_id = data.get('order_id')
        items    = data.get('items', [])
        if not order_id or not items:
            abort(400, "Missing order_id or items in request")

        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                                # ── Idempotent: if already prepared/committed, return early ──
                cur.execute(
                    "SELECT status FROM stock_transactions WHERE transaction_id = %s",
                    (transaction_id,),
                )
                existing = cur.fetchone()
                if existing:
                    if existing['status'] in ('PREPARED', 'COMMITTED'):
                        return jsonify({
                            'status': existing['status'],
                            'transaction_id': transaction_id,
                        }), 200
                    abort(400, f"Transaction {transaction_id} already aborted")

                for item in items:
                    cur.execute("SELECT stock FROM items WHERE item_id = %s FOR UPDATE", (item['item_id'],))
                    row = cur.fetchone()
                    if row is None:
                        abort(400, f"Item {item['item_id']} not found")
                    stock = row['stock']

                    cur.execute("""
                        SELECT COALESCE(SUM(sr.quantity), 0) as reserved
                        FROM stock_reservations sr
                        JOIN stock_transactions st ON sr.transaction_id = st.transaction_id
                        WHERE sr.item_id = %s AND st.status = 'PREPARED'
                    """, (item['item_id'],))
                    reserved = cur.fetchone()['reserved']

                    if stock - reserved < int(item['quantity']):
                        abort(400, f"Insufficient stock for {item['item_id']}")
                cur.execute(
                    "INSERT INTO stock_transactions (transaction_id, order_id, status, items_reserved) VALUES (%s, %s, %s, %s)",
                    (transaction_id, order_id, 'PREPARED', json.dumps(items))
                )
                for item in items:
                    cur.execute(
                        "INSERT INTO stock_reservations (reservation_id, transaction_id, item_id, quantity) VALUES (%s, %s, %s, %s)",
                        (f"{transaction_id}_{item['item_id']}", transaction_id,
                         item['item_id'], int(item['quantity']))
                    )
                conn.commit()

        return jsonify({'status': 'PREPARED', 'transaction_id': transaction_id}), 200
    except psycopg.Error as e:
        abort(400, f"DB error in prepare: {str(e)}")


@app.post('/commit/<transaction_id>')
def commit_stock(transaction_id: str):
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM stock_transactions WHERE transaction_id = %s",
                            (transaction_id,))
                tx = cur.fetchone()
                if tx is None:
                    abort(400, f"Transaction {transaction_id} not found")
                #Idempotency Check
                if tx['status'] == 'COMMITTED':
                    return jsonify({'status': 'COMMITTED', 'transaction_id': transaction_id}), 200
                
                cur.execute("SELECT * FROM stock_reservations WHERE transaction_id = %s",
                            (transaction_id,))
                for res in cur.fetchall():
                    cur.execute("UPDATE items SET stock = stock - %s WHERE item_id = %s",
                                (res['quantity'], res['item_id']))               
                cur.execute("DELETE FROM stock_reservations WHERE transaction_id = %s", (transaction_id,))
                cur.execute("UPDATE stock_transactions SET status = 'COMMITTED' WHERE transaction_id = %s",
                            (transaction_id,))
                conn.commit()
        return jsonify({'status': 'COMMITTED', 'transaction_id': transaction_id}), 200
    except psycopg.Error as e:
        abort(400, f"DB error in commit: {str(e)}")


@app.post('/abort/<transaction_id>')
def abort_stock(transaction_id: str):
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM stock_reservations WHERE transaction_id = %s",
                            (transaction_id,))
                cur.execute("UPDATE stock_transactions SET status = 'ABORTED' WHERE transaction_id = %s",
                            (transaction_id,))
                conn.commit()
        return jsonify({'status': 'ABORTED', 'transaction_id': transaction_id}), 200
    except psycopg.Error as e:
        abort(400, f"DB error in abort: {str(e)}")


# ── Cooperative Termination: expose local tx status ──

@app.get('/transaction/<transaction_id>/status')
def transaction_status(transaction_id: str):
    """Other participants can query this to resolve uncertain transactions."""
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT status FROM stock_transactions WHERE transaction_id = %s",
                            (transaction_id,))
                row = cur.fetchone()
                if row:
                    return jsonify({'transaction_id': transaction_id, 'status': row['status']}), 200
                return jsonify({'transaction_id': transaction_id, 'status': 'UNKNOWN'}), 200
    except psycopg.Error:
        return jsonify({'transaction_id': transaction_id, 'status': 'UNKNOWN'}), 200


# ── Stale Transaction Recovery Sweep ──

def _recovery_commit(transaction_id: str) -> None:
    """Apply a commit for a stale PREPARED transaction."""
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT status FROM stock_transactions WHERE transaction_id = %s",
                            (transaction_id,))
                tx = cur.fetchone()
                if tx is None or tx['status'] != 'PREPARED':
                    return
                cur.execute("SELECT item_id, quantity FROM stock_reservations WHERE transaction_id = %s",
                            (transaction_id,))
                for res in cur.fetchall():
                    cur.execute("UPDATE items SET stock = stock - %s WHERE item_id = %s",
                                (res['quantity'], res['item_id']))
                cur.execute("DELETE FROM stock_reservations WHERE transaction_id = %s", (transaction_id,))
                cur.execute("UPDATE stock_transactions SET status = 'COMMITTED' WHERE transaction_id = %s",
                            (transaction_id,))
                conn.commit()
        logging.info("RECOVERY committed  tx=%s", transaction_id)
    except psycopg.Error as exc:
        logging.error("RECOVERY commit error  tx=%s  exc=%s", transaction_id, exc)


def _recovery_abort(transaction_id: str) -> None:
    """Abort a stale PREPARED transaction."""
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM stock_reservations WHERE transaction_id = %s",
                            (transaction_id,))
                cur.execute("UPDATE stock_transactions SET status = 'ABORTED' WHERE transaction_id = %s",
                            (transaction_id,))
                conn.commit()
        logging.info("RECOVERY aborted  tx=%s", transaction_id)
    except psycopg.Error as exc:
        logging.error("RECOVERY abort error  tx=%s  exc=%s", transaction_id, exc)


def _resolve_via_peers(transaction_id: str) -> str:
    """Cooperative termination: ask coordinator first, then sibling participant."""
    for url in [
        f"{GATEWAY_URL}/orders/transaction/{transaction_id}/status",
        f"{GATEWAY_URL}/payment/transaction/{transaction_id}/status",
    ]:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                status = r.json().get('status', 'UNKNOWN')
                if status in ('COMMITTED', 'ABORTED'):
                    return status
        except requests.exceptions.RequestException:
            continue
    return 'UNKNOWN'


def _recovery_sweep() -> None:
    """Background thread: resolve stale PREPARED transactions."""
    while True:
        time.sleep(RECOVERY_INTERVAL_SECS)
        if not GATEWAY_URL:
            continue
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute("""
                        SELECT transaction_id
                        FROM stock_transactions
                        WHERE status = 'PREPARED'
                          AND created_at < NOW() - %s * INTERVAL '1 second'
                    """, (STALE_THRESHOLD_SECS,))
                    stale = cur.fetchall()
            for tx in stale:
                tid = tx['transaction_id']
                outcome = _resolve_via_peers(tid)
                if outcome == 'COMMITTED':
                    _recovery_commit(tid)
                elif outcome == 'ABORTED':
                    _recovery_abort(tid)
        except Exception as exc:
            logging.error("Stock recovery sweep error: %s", exc)


if GATEWAY_URL:
    threading.Thread(target=_recovery_sweep, daemon=True, name="stock-recovery").start()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    logging.root.handlers = gunicorn_logger.handlers
    logging.root.setLevel(gunicorn_logger.level)
