import logging
import os
import atexit
import uuid

import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

from msgspec import Struct, ValidationError, convert, json
from flask import Flask, jsonify, abort, Response

import threading
from flask import Flask, jsonify, abort, Response
from services import utils

DB_ERROR_STR = "DB error"

app = Flask("payment-service")
service_name = "stock"

kafka_producer = None
kafka_consumer = None


def consume_messages(consumer):
    """Background task to process Kafka messages."""
    print("Kafka consumer started...")
    for message in consumer:
        result = utils.decode_and_type_event(message)
        if isinstance(result, utils.Failure):
            print(f"Failed to decode message: {result.error}")
            # TODO Handle validation error response in a decent way
            # event = utils.BaseEvent(
            #     event_type=utils.StockIntegrationEvent.STOCK_FAILED,
            #     correlation_id=None,
            #     payload=None
            # )
            # kafka_producer.send(
            #     topic='order.request',
            #     value=event
            # ) 
            continue
            
        event = result.value
        print(f"Received message on topic {message.topic}: {event.event_type}.") 
        dispatch_event(event)
            
        


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
            payload=utils.StockReservedPayload(
                order_id=payload.order_id,
                amount=result.value
            )
        )

        kafka_producer.send(
            topic='order.request',
            value=event)

    else:
        print(f"Failed to reserve stock for order: {payload.order_id}. Reason: {result.error}")
        event = utils.BaseEvent(
            utils.StockIntegrationEvent.STOCK_UNAVAILABLE,
            correlation_id=event.correlation_id,
            # TODO Add more info in the payload about the failure (e.g. which items were unavailable)
            payload=utils.StockUnavailablePayload(
                order_id=payload.order_id
            )
        )

        kafka_producer.send(
            topic='order.request',
            value=event
        )
    

def handle_rollback(event: utils.BaseEvent):
    
    order_id = event.payload.order_id
    print(f"Handling stock rollback for order: {order_id}")
    items = event.payload.items
    with db_pool.connection() as conn:
        with conn.cursor() as cur:
            for item_id, qty in items:
                cur.execute(
                    "UPDATE items SET stock = stock + %s WHERE item_id = %s",
                    (qty, item_id)
                )
        conn.commit()
    print(f"Stock rollback completed for order: {order_id}")

    event = utils.BaseEvent(
        utils.StockIntegrationEvent.STOCK_FREED,
        correlation_id=event.correlation_id,
        payload=utils.StockFreedPayload(
            order_id=order_id
        )
    )

    kafka_producer.send(
        topic='order.request',
        value=event
    )




def checkout():
    # Query for all the needed items
    # If not available, rollback
    pass

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
    """Initialize database table"""
    tmp_pool = init_db_pool()
    with tmp_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS items (
                    item_id TEXT PRIMARY KEY,
                    stock INTEGER NOT NULL,
                    price INTEGER NOT NULL
                )
            """)
            # conn.commit()
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
                    "SELECT stock, price FROM items WHERE item_id = %s",
                    (item_id,)
                )
                row = cur.fetchone()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    
    if row is None:
        abort(400, f"Item: {item_id} not found!")
    
    return StockValue(stock=row['stock'], price=row['price'])

def subtract_stock_batch(order_id: str, items: list[tuple[str, int]], event_id: str):
    # items looks like: [(<item_id1>,<qty1>), (<item_id2>,<qty2>)]

    if not items:
        return utils.Failure("No items to reserve")

    item_ids, quantities = zip(*items)
    item_ids, quantities = list(item_ids), list(quantities)

    # TODO might be required to add missing items check
    query = """
    WITH
    items_to_update AS (
        -- 1. Create a virtual table of the requested IDs and quantities
        SELECT unnest(%s::text[]) as id, unnest(%s::int[]) as req_qty
    ),
    availability_check AS (
        -- 2. Lock and verify stock — tag each row as available or not
        SELECT
            i.item_id,
            i.price,
            u.req_qty,
            i.stock >= u.req_qty AS is_available
        FROM items i
        JOIN items_to_update u ON i.item_id = u.id
        FOR UPDATE
    ),
    do_update AS (
        -- 3. Perform subtraction ONLY if every item is available
        UPDATE items i
        SET stock = i.stock - a.req_qty
        FROM availability_check a
        WHERE i.item_id = a.item_id
        AND (SELECT COUNT(*) FROM availability_check WHERE NOT is_available) = 0
        RETURNING i.item_id, a.price, a.req_qty
    )
    -- 4. Return two result sets in one query:
    --    - updated rows with their cost (do_update)
    --    - unavailable item_ids for failure reporting (availability_check)
    SELECT
        'updated'       AS result_type,
        item_id,
        price,
        req_qty,
        NULL            AS unavailable_item_id
    FROM do_update

    UNION ALL

    SELECT
        'unavailable'   AS result_type,
        NULL,
        NULL,
        NULL,
        item_id         AS unavailable_item_id
    FROM availability_check
    WHERE NOT is_available;
    """

    params = (item_ids, quantities)

    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

                updated_rows = [(r[1], r[2], r[3]) for r in rows if r[0] == 'updated']
                unavailable_ids = [r[4] for r in rows if r[0] == 'unavailable']

                if unavailable_ids:
                    # At least one item lacked stock — nothing was updated (guard in do_update)
                    conn.rollback()
                    return utils.Failure(f"Insufficient stock for items: {unavailable_ids}")

                if not updated_rows:
                    # No unavailable items but also nothing updated — unexpected state
                    conn.rollback()
                    return utils.Failure("No items were updated")

                total_cost = sum(price * qty for _, price, qty in updated_rows)
                conn.commit()
                return utils.Success(total_cost)

    except psycopg.Error as e:
        app.logger.error(f"Stock subtraction failed: {e}")
        return utils.Failure("Database error during stock subtraction")

@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO items (item_id, stock, price) VALUES (%s, %s, %s)",
                    (key, 0, int(price))
                )
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.get('/items')
def get_items():
    with db_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                    SELECT item_id, stock, price FROM items
                """)
            items = cur.fetchall()
            return jsonify(items)
    

@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    
    values = [(f"{i}", starting_stock, item_price) for i in range(n)]
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO items (item_id, stock, price) VALUES (%s, %s, %s)",
                    values
                )
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id)
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )


@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock
    item_entry.stock += int(amount)
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE items SET stock = %s WHERE item_id = %s",
                    (item_entry.stock, item_id)
                )
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    item_entry: StockValue = get_item_from_db(item_id)
    # update stock
    item_entry.stock -= int(amount)
    app.logger.debug(f"Item: {item_id} stock updated to: {item_entry.stock}")
    if item_entry.stock < 0:
        abort(400, f"Item: {item_id} stock cannot get reduced below zero!")
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE items SET stock = %s WHERE item_id = %s",
                    (item_entry.stock, item_id)
                )
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} stock updated to: {item_entry.stock}", status=200)




if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
