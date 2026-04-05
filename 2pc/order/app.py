import logging
import os
import atexit
import random
import threading
import time
import uuid
from collections import defaultdict

import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
import requests
import json
from msgspec import Struct
from flask import Flask, jsonify, abort, Response, request

from kafka_service import kafka_client
from kafka_service.kafka_event import BaseEvent, OrderPayload, CheckoutPayload
from kafka_service.consumer_handler import EventConsumer
from coordinator import Orchestrator, Participant, create_tables
from coordinator.app import RECONCILE_INTERVAL

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")
service_name = "order"

order_kafka = kafka_client.Client(
    service_name,
    ['order.events', 'stock.events', 'payment.events']
)
kafka_producer, kafka_consumer = order_kafka.producer, order_kafka.consumer

#Database Pool

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


def _mark_order_paid(cur, order_id: str) -> None:
    cur.execute("UPDATE orders SET paid = TRUE WHERE order_id = %s", (order_id,))


orchestrator = Orchestrator(db_pool, timeout=10, on_commit_decided=_mark_order_paid)


# ---------------------------------------------------------------------------
# DB init
# ---------------------------------------------------------------------------


def init_db():
    with db_pool.connection() as conn:
        create_tables(conn)  # ← orchestrator handles its own schema
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id   TEXT    PRIMARY KEY,
                    paid       BOOLEAN NOT NULL,
                    items      JSONB   NOT NULL,
                    user_id    TEXT    NOT NULL,
                    total_cost INTEGER NOT NULL
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
            # rest of your app tables
            conn.commit()


def close_db_connection():
    db_pool.close()


def _build_participant(transaction_id: str, name: str) -> Participant:
    """Reconstruct a Participant from its persisted name + runtime GATEWAY_URL."""
    if name == "stock":
        return Participant(
            name="stock",
            prepare_url=f"{GATEWAY_URL}/stock/prepare/{{transaction_id}}",
            commit_url=f"{GATEWAY_URL}/stock/commit/{{transaction_id}}",
            abort_url=f"{GATEWAY_URL}/stock/abort/{{transaction_id}}",
        )
    if name == "payment":
        return Participant(
            name="payment",
            prepare_url=f"{GATEWAY_URL}/payment/prepare/{{transaction_id}}",
            commit_url=f"{GATEWAY_URL}/payment/commit/{{transaction_id}}",
            abort_url=f"{GATEWAY_URL}/payment/abort/{{transaction_id}}",
        )
    raise ValueError(f"Unknown participant: {name}")


def _reconciler_loop():
    """Background thread: keep retrying incomplete txns until all ACK'd."""
    while True:
        time.sleep(RECONCILE_INTERVAL)
        try:
            remaining = orchestrator.reconcile(_build_participant)
            if remaining:
                logging.info("Reconciler: %d txns still incomplete", remaining)
        except Exception as exc:
            logging.error("Reconciler error: %s", exc)


def start_reconciler():
    """Start the background reconciler and run one immediate sweep."""
    # Immediate recovery sweep on startup
    try:
        remaining = orchestrator.reconcile(_build_participant)
        if remaining:
            logging.info("Startup recovery: %d txns still incomplete", remaining)
    except Exception as exc:
        logging.error("Startup recovery error: %s", exc)
    threading.Thread(target=_reconciler_loop, daemon=True, name="order-reconciler").start()


init_db()
atexit.register(close_db_connection)
def consume_messages():
    event_consumer._run()


class OrderValue(Struct):
    paid: bool
    items: list[tuple[str, int]]
    user_id: str
    total_cost: int


def get_order_from_db(order_id: str) -> OrderValue:
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT paid, items, user_id, total_cost FROM orders WHERE order_id = %s",
                    (order_id,)
                )
                row = cur.fetchone()
    except psycopg.Error:
        abort(400, DB_ERROR_STR)
    if row is None:
        abort(400, f"Order: {order_id} not found!")
    items = [(i['item_id'], i['quantity']) for i in row['items']]
    return OrderValue(paid=row['paid'], items=items,
                      user_id=row['user_id'], total_cost=row['total_cost'])



@app.post('/create/<user_id>')
def create_order(user_id: str):
    key = str(uuid.uuid4())
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO orders (order_id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s)",
                    (key, False, json.dumps([]), user_id, 0)
                )
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)

    kafka_producer.send(topic="order.events", value=BaseEvent.create(
        event_type="ORDER_CREATED",
        payload=OrderPayload(order_id=key, item_id="", quantity=0),
    ))
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
def batch_init_users(n: int, n_items: int, n_users: int, item_price: int):
    n, n_items, n_users, item_price = int(n), int(n_items), int(n_users), int(item_price)

    def generate_entry(order_id: int):
        user_id  = random.randint(0, n_users - 1)
        item1_id = random.randint(0, n_items - 1)
        item2_id = random.randint(0, n_items - 1)
        items    = [{'item_id': f"{item1_id}", 'quantity': 1},
                    {'item_id': f"{item2_id}", 'quantity': 1}]
        return (f"{order_id}", False, json.dumps(items), f"{user_id}", 2 * item_price)

    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO orders (order_id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s)",
                    [generate_entry(i) for i in range(n)]
                )
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    o = get_order_from_db(order_id)
    return jsonify({"order_id": order_id, "paid": o.paid, "items": o.items,
                    "user_id": o.user_id, "total_cost": o.total_cost})


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    order_entry = get_order_from_db(order_id)
    try:
        item_reply = requests.get(f"{GATEWAY_URL}/stock/find/{item_id}")
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    if item_reply.status_code != 200:
        abort(400, f"Item: {item_id} does not exist!")

    item_json = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    items_json = [{'item_id': i[0], 'quantity': i[1]} for i in order_entry.items]

    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE orders SET items = %s, total_cost = %s WHERE order_id = %s",
                    (json.dumps(items_json), order_entry.total_cost, order_id)
                )
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)

    kafka_producer.send(topic="order.events", value=BaseEvent.create(
        event_type="ADD_ITEM",
        payload=OrderPayload(order_id=order_id, item_id=item_id, quantity=int(quantity)),
    ))
    return Response(
        f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
        status=200
    )


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    app.logger.debug(f"Checking out {order_id}")
    order_entry = get_order_from_db(order_id)
    corr_id = str(uuid.uuid4())

    items_quantities: dict[str, int] = defaultdict(int)
    for item_id, quantity in order_entry.items:
        items_quantities[item_id] += quantity
    items_list = [{'item_id': k, 'quantity': v} for k, v in items_quantities.items()]

    stock_participant = Participant(
        name="stock",
        prepare_url=f"{GATEWAY_URL}/stock/prepare/{{transaction_id}}",
        commit_url=f"{GATEWAY_URL}/stock/commit/{{transaction_id}}",
        abort_url=f"{GATEWAY_URL}/stock/abort/{{transaction_id}}",
        prepare_body={'order_id': order_id, 'items': items_list},
    )
    payment_participant = Participant(
        name="payment",
        prepare_url=f"{GATEWAY_URL}/payment/prepare/{{transaction_id}}",
        commit_url=f"{GATEWAY_URL}/payment/commit/{{transaction_id}}",
        abort_url=f"{GATEWAY_URL}/payment/abort/{{transaction_id}}",
        prepare_body={'order_id': order_id, 'user_id': order_entry.user_id,
                      'amount': order_entry.total_cost},
    )

    result = orchestrator.run(business_id=order_id,
                              participants=[stock_participant, payment_participant])

    if not result.success:
        kafka_producer.send(topic="order.events", value=BaseEvent.create(
            event_type="ORDER_FAILED",
            payload=OrderPayload(order_id=order_id, item_id="", quantity=0),
            corr_id=corr_id,
        ))
        abort(400, result.error)

    kafka_producer.send(topic="order.events", value=BaseEvent.create(
        event_type="CHECKOUT_SUCCESS",
        payload=CheckoutPayload(order_id=order_id, user_id=order_entry.user_id),
        corr_id=corr_id,
    ))

    return jsonify({'status': 'success', 'order_id': order_id,
                    'transaction_id': result.transaction_id,
                    'correlation_id': corr_id}), 200


@app.get('/transaction/<transaction_id>/status')
def transaction_status(transaction_id: str):
    """Participant inquiry protocol — participants poll to resolve uncertain txs."""
    status = orchestrator.get_transaction_status(transaction_id)
    return jsonify({'transaction_id': transaction_id, 'status': status}), 200

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    logging.root.handlers = gunicorn_logger.handlers
    logging.root.setLevel(gunicorn_logger.level)
