import logging
import os
import atexit
import uuid
import json as std_json

import psycopg # type: ignore
from psycopg_pool import ConnectionPool # type: ignore
from psycopg.rows import dict_row # type: ignore

from msgspec import Struct
from flask import Flask, jsonify, abort, Response

from flask import Flask, jsonify, abort, Response
from services import utils

DB_ERROR_STR = "DB error"

app = Flask("payment-service")
service_name = "stock"

kafka_producer = None
kafka_consumer = None


# Create connection pool
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
        # Reconnection policy
        reconnect_timeout=30,
        kwargs={"connect_timeout": 10}
    )

def init_db():
    """Initialize event-store and snapshot tables"""
    tmp_pool = init_db_pool()
    with tmp_pool.connection() as conn:
        with conn.cursor() as cur:
            # Append-only event log
            cur.execute("""
                CREATE TABLE IF NOT EXISTS log (
                    id TEXT PRIMARY KEY,
                    item_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    version INTEGER NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now()
                )
            """)
            # Mutable snapshot/projection for fast reads
            cur.execute("""
                CREATE TABLE IF NOT EXISTS item_snapshots (
                    item_id TEXT PRIMARY KEY,
                    stock INTEGER NOT NULL,
                    price INTEGER NOT NULL,
                    version INTEGER NOT NULL
                )
            """)

            # Implement a received table for idempotency if needed in the future
            cur.execute("""
                CREATE TABLE IF NOT EXISTS received_events (
                    event_id TEXT PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT now()
                )
            """)

            # Implement a table for outbox messages if needed in the future
            cur.execute("""
                CREATE TABLE IF NOT EXISTS outbox (
                    id TEXT PRIMARY KEY,
                    topic TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    sent BOOLEAN DEFAULT FALSE
                )
            """)

            
    tmp_pool.close()

def close_db_connection():
    db_pool.close()


# Initialize database on startup
init_db()
atexit.register(close_db_connection)


class StockValue(Struct):
    stock: int
    price: int



def get_item_from_db(item_id: str) -> StockValue | None:
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT stock, price FROM item_snapshots WHERE item_id = %s",
                    (item_id,)
                )
                row = cur.fetchone()

                if row is not None:
                    return StockValue(stock=row['stock'], price=row['price'])

                # Fallback: replay events to rebuild state
                cur.execute(
                    "SELECT event_type, payload FROM log WHERE item_id = %s ORDER BY version",
                    (item_id,)
                )
                event_rows = cur.fetchall()
                if not event_rows:
                    abort(400, f"Item: {item_id} not found!")

                stock, price = 0, 0
                for event in event_rows:
                    et = event['event_type']
                    p = event['payload']
                    if et == 'ITEM_CREATED':
                        stock = p.get('stock', 0)
                        price = p.get('price', 0)
                    elif et == 'STOCK_ADDED':
                        stock += int(p.get('amount', 0))
                    elif et == 'STOCK_SUBTRACTED':
                        stock -= int(p.get('amount', 0))

                return StockValue(stock=stock, price=price)

    except psycopg.Error:
        return abort(400, DB_ERROR_STR)



def consume_messages(consumer):
    """Background task to process Kafka messages."""
    print("Kafka consumer started...")
    for message in consumer:
        result = utils.decode_and_type_event(message)
        if isinstance(result, utils.Failure):
            print(f"Failed to decode message: {result.error}")
            # TODO Handle validation error response in a decent way
            continue
            
        event = result.value
        print(f"Received message on topic {message.topic}: {event.event_type}.")

        if isinstance(is_already_processed(event), utils.Success):
            print(f"Event {event.saga_id} already processed, skipping.")
            
            continue

        dispatch_event(event)
            
        
def is_already_processed(event: utils.BaseEvent):
    with db_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # Check if event was already processed
            cur.execute(
                "SELECT event_id FROM received_events WHERE event_id = %s",
                (event.saga_id,)
            )
            if cur.fetchone() is not None:
                return utils.Success(0)  # Already processed, skip
            
            # Mark event as processed
            cur.execute(
                "INSERT INTO received_events (event_id) VALUES (%s) ON CONFLICT DO NOTHING",
                (event.saga_id,)
            )


def dispatch_event(event: utils.BaseEvent):

    if event.event_type == utils.Commands.RESERVE_STOCK:
        handle_stock_reservation(event)
    elif event.event_type == utils.Commands.FREE_STOCK:
        handle_rollback(event)




def handle_stock_reservation(event: utils.BaseEvent):
   
    payload = event.payload
    print(f"Handling stock reservation for order: {payload.order_id} with items: {payload.items}")

    result = subtract_stock_batch(payload.order_id, payload.items, event.correlation_id)
    
    if isinstance(result, utils.Success):
        print(f"Stock successfully reserved for order: {payload.order_id}")
        event = utils.BaseEvent(
            utils.StockIntegrationEvent.STOCK_ALLOCATED,
            correlation_id=event.correlation_id,
            saga_id=event.saga_id,
            payload=utils.StockReservedPayload(
                order_id=payload.order_id,
                amount=result.value
            )
        )

        kafka_producer.send( # type: ignore
            topic='order.request',
            value=event)

    else:
        print(f"Failed to reserve stock for order: {payload.order_id}. Reason: {result.error}")
        event = utils.BaseEvent(
            utils.StockIntegrationEvent.STOCK_UNAVAILABLE,
            correlation_id=event.correlation_id,
            saga_id=event.saga_id,
            # TODO Add more info in the payload about the failure (e.g. which items were unavailable)
            payload=utils.StockUnavailablePayload(
                order_id=payload.order_id
            )
        )

        kafka_producer.send( # type: ignore
            topic='order.request',
            value=event
        )
    

def handle_rollback(event: utils.BaseEvent):

    order_id = event.payload.order_id
    items = sorted(event.payload.items, key=lambda x: x[0])  # ← add this
    print(f"Handling stock rollback for order: {order_id}")
    MAX_RETRIES = 3

    for attempt in range(MAX_RETRIES):
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    item_ids = [item_id for item_id, _ in items]
                    placeholders = ','.join(['%s'] * len(item_ids))
                    cur.execute(
                        f"SELECT item_id, stock, version FROM item_snapshots WHERE item_id IN ({placeholders})",
                        item_ids
                    )
                    rows = {row['item_id']: row for row in cur.fetchall()}

                    conflict = False
                    for item_id, qty in items:
                        if item_id not in rows:
                            app.logger.warning(f"Rollback requested for unknown item {item_id} in order {order_id}")
                            continue
                        row = rows[item_id]
                        current_version = int(row['version'])
                        new_stock = int(row['stock']) + int(qty)
                        new_version = current_version + 1

                        ev_id = str(uuid.uuid4())
                        payload_str = std_json.dumps({"order_id": order_id, "amount": int(qty)})
                        cur.execute(
                            "INSERT INTO log (id, item_id, event_type, payload, version) VALUES (%s, %s, %s, %s, %s)",
                            (ev_id, item_id, 'STOCK_ADDED', payload_str, new_version)
                        )

                        cur.execute(
                            """UPDATE item_snapshots
                               SET stock = %s, version = %s
                               WHERE item_id = %s AND version = %s""",
                            (new_stock, new_version, item_id, current_version)
                        )

                        if cur.rowcount == 0:          # ← check HERE, immediately after the version guard
                            conflict = True
                            break

                    if conflict:
                        conn.rollback()
                        app.logger.warning(f"Version conflict in rollback for order {order_id}, retry {attempt + 1}/{MAX_RETRIES}")
                        continue

                    print(f"Stock rollback completed for order: {order_id}")
                    rollback_event = utils.BaseEvent(
                        utils.StockIntegrationEvent.STOCK_FREED,
                        correlation_id=event.correlation_id,
                        saga_id=event.saga_id,
                        payload=utils.StockFreedPayload(order_id=order_id)
                    )

                    kafka_producer.send(topic='order.request', value=rollback_event) # type: ignore
                    return

        except psycopg.Error as e:
            app.logger.error(f"Stock rollback DB error: {e}")
            return

    app.logger.error(f"Stock rollback failed after {MAX_RETRIES} retries for order: {order_id}")










def subtract_stock_batch(order_id: str, items: list[tuple[str, int]], event_id: str):
    if not items:
        return utils.Failure("No items to reserve")

    items = sorted(items, key=lambda x: x[0])  # ← add this
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    # Read all items WITHOUT lock
                    item_ids = [item_id for item_id, _ in items]
                    placeholders = ','.join(['%s'] * len(item_ids))
                    cur.execute(
                        f"SELECT item_id, stock, price, version FROM item_snapshots WHERE item_id IN ({placeholders})",
                        item_ids
                    )
                    rows = {row['item_id']: row for row in cur.fetchall()}

                    # Check that all items exist
                    missing = [iid for iid, _ in items if iid not in rows]
                    if missing:
                        return utils.Failure(f"Items not found: {missing}")

                    # Check availability before writing anything
                    unavailable = [
                        iid for iid, qty in items
                        if int(rows[iid]['stock']) < int(qty)
                    ]
                    if unavailable:
                        return utils.Failure(f"Insufficient stock for items: {unavailable}")

                    # Write phase: version-guarded update for every item
                    conflict = False
                    total_cost = 0
                    for item_id, qty in items:
                        row = rows[item_id]
                        current_version = int(row['version'])
                        new_stock = int(row['stock']) - int(qty)
                        new_version = current_version + 1
                        price = int(row['price'])

                        ev_id = str(uuid.uuid4())
                        payload_str = std_json.dumps({"order_id": order_id, "amount": int(qty)})
                        cur.execute(
                            "INSERT INTO log (id, item_id, event_type, payload, version) VALUES (%s, %s, %s, %s, %s)",
                            (ev_id, item_id, 'STOCK_SUBTRACTED', payload_str, new_version)
                        )

                        # Version guard: only succeeds if no one else wrote first
                        cur.execute(
                            """UPDATE item_snapshots
                               SET stock = %s, version = %s
                               WHERE item_id = %s AND version = %s""",
                            (new_stock, new_version, item_id, current_version)
                        )

                        if cur.rowcount == 0:
                            conflict = True
                            break
                        
                        

                        total_cost += price * int(qty)

                    if conflict:
                        conn.rollback()
                        app.logger.warning(f"Version conflict in batch stock update, retry {attempt + 1}/{MAX_RETRIES}")
                        continue

                    return utils.Success(total_cost)

        except psycopg.Error as e:
            app.logger.error(f"Stock subtraction failed: {e}")
            return utils.Failure("Database error during stock subtraction")

    return utils.Failure("Too many concurrent updates, please retry")

@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                event_id = str(uuid.uuid4())
                payload_str = std_json.dumps({"price": int(price), "stock": 0})
                cur.execute(
                    "INSERT INTO log (id, item_id, event_type, payload, version) VALUES (%s, %s, %s, %s, %s)",
                    (event_id, key, 'ITEM_CREATED', payload_str, 1)
                )
                cur.execute(
                    "INSERT INTO item_snapshots (item_id, stock, price, version) VALUES (%s, %s, %s, %s)",
                    (key, 0, int(price), 1)
                )
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.get('/items')
def get_items():
    with db_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SELECT item_id, stock, price FROM item_snapshots")
            items = cur.fetchall()
            return jsonify(items)
    


@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                for i in range(n):
                    item_id = str(i)
                    event_id = str(uuid.uuid4())
                    payload_str = std_json.dumps({"stock": starting_stock, "price": item_price})
                    cur.execute(
                        "INSERT INTO " \
                        "log (id, item_id, event_type, payload, version) VALUES (%s, %s, %s, %s, %s)",
                        (event_id, item_id, 'ITEM_CREATED', payload_str, 1)
                    )
                    cur.execute(
                        "INSERT INTO item_snapshots (item_id, stock, price, version) VALUES (%s, %s, %s, %s)",
                        (item_id, starting_stock, item_price, 1)
                    )
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
                    # Read WITHOUT lock
                    cur.execute(
                        "SELECT stock, version FROM item_snapshots WHERE item_id = %s",
                        (item_id,)
                    )
                    row = cur.fetchone()
                    if row is None:
                        return abort(400, f"Item: {item_id} not found!")

                    current_version = int(row['version'])
                    new_stock = int(row['stock']) + int(amount)
                    new_version = current_version + 1

                    event_id = str(uuid.uuid4())
                    payload_str = std_json.dumps({"amount": int(amount)})
                    cur.execute(
                        "INSERT INTO log (id, item_id, event_type, payload, version) VALUES (%s, %s, %s, %s, %s)",
                        (event_id, item_id, 'STOCK_ADDED', payload_str, new_version)
                    )

                    # Version guard
                    cur.execute(
                        """UPDATE item_snapshots
                           SET stock = %s, version = %s
                           WHERE item_id = %s AND version = %s""",
                        (new_stock, new_version, item_id, current_version)
                    )

                    if cur.rowcount == 0:
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
                    # Read WITHOUT lock
                    cur.execute(
                        "SELECT stock, version FROM item_snapshots WHERE item_id = %s",
                        (item_id,)
                    )
                    row = cur.fetchone()
                    if row is None:
                        return abort(400, f"Item: {item_id} not found!")

                    current_stock = int(row['stock'])
                    current_version = int(row['version'])
                    new_stock = current_stock - int(amount)

                    if new_stock < 0:
                        return abort(400, f"Item: {item_id} stock cannot get reduced below zero!")

                    new_version = current_version + 1

                    event_id = str(uuid.uuid4())
                    payload_str = std_json.dumps({"amount": int(amount)})
                    cur.execute(
                        "INSERT INTO log (id, item_id, event_type, payload, version) VALUES (%s, %s, %s, %s, %s)",
                        (event_id, item_id, 'STOCK_SUBTRACTED', payload_str, new_version)
                    )

                    # Version guard
                    cur.execute(
                        """UPDATE item_snapshots
                           SET stock = %s, version = %s
                           WHERE item_id = %s AND version = %s""",
                        (new_stock, new_version, item_id, current_version)
                    )

                    if cur.rowcount == 0:
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
