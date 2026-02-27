import logging
import os
import atexit
import uuid

import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

from msgspec import Struct, json
from flask import Flask, jsonify, abort, Response

import threading
from flask import Flask, jsonify, abort, Response
from services import kafka_client, utils

DB_ERROR_STR = "DB error"

app = Flask("payment-service")
service_name = "stock"

kafka_producer = None
kafka_consumer = None

def consume_messages(consumer):
    """Background task to process Kafka messages."""
    print("Kafka consumer started...")
    for message in consumer:
        event = json.decode(
            message.value, 
            type=utils.BaseEvent
        )
        
        print(f"Received message on topic {event.event_type}: {event.payload}") 
        if event.event_type == 'CHECKOUT_INITIATED':
            payload = event.payload
            response = utils.BaseEvent(
                utils.StockIntegrationEvent.STOCK_ALLOCATED,
                correlation_id=event.correlation_id,
                payload=utils.StockUpdatePayload("1",10)
            )

            kafka_producer.send('order.request',response)
            # subtract_stock_batch(payload.order_id, payload.items, message.correlation_id)


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

db_pool = ConnectionPool(
    conninfo=f"host={conn_params['host']} port={conn_params['port']} "
             f"user={conn_params['user']} password={conn_params['password']} "
             f"dbname={conn_params['dbname']}",
    min_size=1,
    max_size=10,
    # Reconnection policy
    reconnect_timeout=30,
    kwargs={"connect_timeout": 10}
)


# TODO Abstract DB connection in an external class
def init_db():
    """Initialize database table"""
    with db_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS items (
                    item_id TEXT PRIMARY KEY,
                    stock INTEGER NOT NULL,
                    price INTEGER NOT NULL
                )
            """)
            # conn.commit()

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


def subtract_stock_batch(order_id: str, items: list[tuple[str,int]],event_id: str):
    # items looks like: [(<item_id1>,<qty1>),(<item_id1>,<qty1>)]
    
    # Extract lists for the query
    if not items:
        return False

    # zip(*items) turns [(a, b), (c, d)] into [(a, c), (b, d)]
    item_ids, quantities = zip(*items)
    item_ids, quantities = list(item_ids), list(quantities)

    # Common table expression to First prepare all the queries an then execute them at the same time.
    query = """
        -- 1. Create a temporary virtual table 
        WITH items_to_update AS (
            -- unnest turns the item_ids and quantities arrays into a table with two columns
            SELECT unnest(%s::text[]) as id, unnest(%s::int[]) as req_qty
        ),

        -- 2. Verify availability and lock the rows
        availability_check AS (
            SELECT i.item_id
            FROM items i
            JOIN items_to_update u ON i.item_id = u.id
            -- Only select rows where current stock is enough for the requested quantity
            WHERE i.stock >= u.req_qty
            -- FOR UPDATE locks these rows; other transactions must wait until we COMMIT or ROLLBACK
            FOR UPDATE 
        ),

        -- 3. Perform the subtraction only if the check passed for EVERY item
        do_update AS (
            UPDATE items i
            SET stock = i.stock - u.req_qty
            FROM items_to_update u
            WHERE i.item_id = u.id
            -- This subquery ensures the update runs ONLY if the number of available items
            -- found in Step 2 matches the total number of items the user requested
            AND (SELECT COUNT(*) FROM availability_check) = %s 
            -- RETURNING allows us to know exactly which rows were modified
            RETURNING i.item_id
        )

        -- 4. Log the event ID to ensure this order isn't processed twice (Idempotency)
        INSERT INTO processed_events (event_id, order_id)
        -- We select from a dummy query that only returns a row if the previous step succeeded
        SELECT %s, %s
        -- This check ensures that if 'do_update' failed (due to stock), no event log is created
        WHERE (SELECT COUNT(*) FROM do_update) = %s
        -- If this succeeds, the function returns a value; if it fails, it returns nothing
        RETURNING 1;
    """

    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (item_ids, quantities, len(item_ids), event_id, order_id, len(item_ids)))
                result = cur.fetchone() # used to verify the RETURN 1 condition
                
                if result:
                    print("QUERY COMPLETED.")
                    conn.commit()
                    return True  # Success: Stock subtracted and event recorded
                else:
                    print("QUERY FAILED.")
                    conn.rollback()
                    return False # Failure: Insufficient stock or already processed
    # TODO modify this
    except psycopg.Error:
        return abort(400, "Database error during stock subtraction")


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
    item_ids = []
    with db_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT * FROM items
                """)
            item_ids = cur.fetchall()
    
    return {"items":item_ids},200

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
