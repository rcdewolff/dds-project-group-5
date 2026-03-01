import logging
import os
import atexit
import uuid
import json

import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row

from msgspec import Struct
from flask import Flask, jsonify, abort, Response, request

import threading
from flask import Flask, jsonify, abort, Response
from kafka_service import kafka_client, kafka_event

DB_ERROR_STR = "DB error"

app = Flask("payment-service")
service_name = "stock"

order_kafka = kafka_client.Client(
    service_name, 
    [f'{service_name}.request']
)
kafka_producer, kafka_consumer = order_kafka.producer, order_kafka.consumer


def consume_messages():
    """Background task to process Kafka messages."""
    print("Kafka consumer started...")
    for message in kafka_consumer:
        print(f"Received message on topic {message.topic}: {message.value}") 



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
            # Create 2PC transaction table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_transactions (
                    transaction_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PREPARED',
                    items_reserved JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Create reserved stock table for holding stock during prepare phase
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_reservations (
                    reservation_id TEXT PRIMARY KEY,
                    transaction_id TEXT NOT NULL,
                    item_id TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    FOREIGN KEY (transaction_id) REFERENCES stock_transactions(transaction_id) ON DELETE CASCADE
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


# ============ 2-Phase Commit Endpoints ============

@app.post('/prepare/<transaction_id>')
def prepare_stock(transaction_id: str):
    """Prepare phase: Check if stock is available and reserve it"""
    try:
        data = request.get_json() or {}
        order_id = data.get('order_id')
        items = data.get('items', [])  # List of {'item_id': str, 'quantity': int}
        
        if not order_id or not items:
            abort(400, "Missing order_id or items in request")
        
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Check if all items have sufficient stock
                for item in items:
                    item_id = item['item_id']
                    quantity = int(item['quantity'])
                    cur.execute(
                        "SELECT stock FROM items WHERE item_id = %s",
                        (item_id,)
                    )
                    row = cur.fetchone()
                    if row is None:
                        abort(400, f"Item {item_id} not found")
                    if row['stock'] < quantity:
                        abort(400, f"Insufficient stock for item {item_id}. Available: {row['stock']}, Required: {quantity}")
                
                # All items available - create transaction record
                items_json = json.dumps(items)
                cur.execute(
                    "INSERT INTO stock_transactions (transaction_id, order_id, status, items_reserved) VALUES (%s, %s, %s, %s)",
                    (transaction_id, order_id, 'PREPARED', items_json)
                )
                
                # Create reservations (don't actually reduce stock yet)
                for item in items:
                    item_id = item['item_id']
                    quantity = int(item['quantity'])
                    res_id = f"{transaction_id}_{item_id}"
                    cur.execute(
                        "INSERT INTO stock_reservations (reservation_id, transaction_id, item_id, quantity) VALUES (%s, %s, %s, %s)",
                        (res_id, transaction_id, item_id, quantity)
                    )
                
                conn.commit()
        
        return jsonify({'status': 'PREPARED', 'transaction_id': transaction_id}), 200
    
    except psycopg.Error as e:
        abort(400, f"DB error in prepare: {str(e)}")


@app.post('/commit/<transaction_id>')
def commit_stock(transaction_id: str):
    """Commit phase: Actually deduct the reserved stock"""
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Get the transaction
                cur.execute(
                    "SELECT * FROM stock_transactions WHERE transaction_id = %s",
                    (transaction_id,)
                )
                trans = cur.fetchone()
                if trans is None:
                    abort(400, f"Transaction {transaction_id} not found")
                
                # Get all reservations
                cur.execute(
                    "SELECT * FROM stock_reservations WHERE transaction_id = %s",
                    (transaction_id,)
                )
                reservations = cur.fetchall()
                
                # Deduct stock for each reserved item
                for res in reservations:
                    item_id = res['item_id']
                    quantity = res['quantity']
                    cur.execute(
                        "UPDATE items SET stock = stock - %s WHERE item_id = %s",
                        (quantity, item_id)
                    )
                
                # Update transaction status
                cur.execute(
                    "UPDATE stock_transactions SET status = %s WHERE transaction_id = %s",
                    ('COMMITTED', transaction_id)
                )
                
                conn.commit()
        
        return jsonify({'status': 'COMMITTED', 'transaction_id': transaction_id}), 200
    
    except psycopg.Error as e:
        abort(400, f"DB error in commit: {str(e)}")


@app.post('/abort/<transaction_id>')
def abort_stock(transaction_id: str):
    """Abort phase: Release reserved stock without deducting"""
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                # Delete reservations
                cur.execute(
                    "DELETE FROM stock_reservations WHERE transaction_id = %s",
                    (transaction_id,)
                )
                
                # Update transaction status
                cur.execute(
                    "UPDATE stock_transactions SET status = %s WHERE transaction_id = %s",
                    ('ABORTED', transaction_id)
                )
                
                conn.commit()
        
        return jsonify({'status': 'ABORTED', 'transaction_id': transaction_id}), 200
    
    except psycopg.Error as e:
        abort(400, f"DB error in abort: {str(e)}")


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
