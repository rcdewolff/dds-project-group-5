import logging
import os
import atexit
import random
import uuid
from collections import defaultdict

import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
import requests
import threading
import json
from msgspec import Struct, msgpack
from flask import Flask, jsonify, abort, Response, request
from kafka_service import kafka_client, kafka_event

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")
service_name = "order"
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



def init_db():
    """Initialize database table"""
    with db_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id TEXT PRIMARY KEY,
                    paid BOOLEAN NOT NULL,
                    items JSONB NOT NULL,
                    user_id TEXT NOT NULL,
                    total_cost INTEGER NOT NULL
                )
            """)
            # Drop and recreate order_transactions to fix schema
            cur.execute("DROP TABLE IF EXISTS order_transactions CASCADE")
            # Create 2PC transaction table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS order_transactions (
                    transaction_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'INITIATED',
                    stock_transaction_id TEXT,
                    payment_transaction_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # conn.commit()



def close_db_connection():
    db_pool.close()


# Initialize database on startup
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
                    (order_id,)
                )
                row = cur.fetchone()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    
    if row is None:
        abort(400, f"Order: {order_id} not found!")
    
    # Convert items from JSON to list of tuples
    items = [(item['item_id'], item['quantity']) for item in row['items']]
    return OrderValue(
        paid=row['paid'],
        items=items,
        user_id=row['user_id'],
        total_cost=row['total_cost']
    )


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
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({'order_id': key})


@app.post('/batch_init/<n>/<n_items>/<n_users>/<item_price>')
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
            {'item_id': f"{item1_id}", 'quantity': 1},
            {'item_id': f"{item2_id}", 'quantity': 1}
        ]
        return (f"{order_id}", False, json.dumps(items), f"{user_id}", 2*item_price)

    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                values = [generate_entry(i) for i in range(n)]
                cur.executemany(
                    "INSERT INTO orders (order_id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s)",
                    values
                )
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for orders successful"})


@app.get('/find/<order_id>')
def find_order(order_id: str):
    order_entry: OrderValue = get_order_from_db(order_id)
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


@app.get("/test/<service>")
def test_kafka(service: str):
    test_msg = "TEST"
    print(f"Order service testing kafka on {service}...")
    kafka_producer.send(
        topic = f'{service}.request',
        value=test_msg
    )
    return jsonify({
        "message":test_msg
    })

def send_post_request(url: str):
    try:
        response = requests.post(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):

    order_entry: OrderValue = get_order_from_db(order_id)
    item_reply = send_get_request(f"{GATEWAY_URL}/stock/find/{item_id}")
    if item_reply.status_code != 200:
        # Request failed because item does not exist
        abort(400, f"Item: {item_id} does not exist!")
    item_json: dict = item_reply.json()
    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    
    # Convert items to JSON format for storage
    items_json = [{'item_id': item[0], 'quantity': item[1]} for item in order_entry.items]
    
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE orders SET items = %s, total_cost = %s WHERE order_id = %s",
                    (json.dumps(items_json), order_entry.total_cost, order_id)
                )
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)


@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item_kafka(order_id: str, item_id: str, quantity: int):
    
    order_entry: OrderValue = get_order_from_db(order_id)
    # TODO Check if there are enough elements in stock

    order_entry.items.append((item_id, int(quantity)))
    order_entry.total_cost += int(quantity) * item_json["price"]
    
    # Convert items to JSON format for storage
    items_json = [{'item_id': item[0], 'quantity': item[1]} for item in order_entry.items]
    
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE orders SET items = %s, total_cost = %s WHERE order_id = %s",
                    (json.dumps(items_json), order_entry.total_cost, order_id)
                )
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)



# ============ 2-Phase Commit Implementation ============

def prepare_stock(transaction_id: str, order_id: str, items: list[dict]) -> bool:
    """
    Send prepare request to stock service
    Returns True if successful, False otherwise
    """
    try:
        response = requests.post(
            f"{GATEWAY_URL}/stock/prepare/{transaction_id}",
            json={'order_id': order_id, 'items': items}
        )
        if response.status_code != 200:
            app.logger.warning(f"Stock prepare failed: {response.text}")
            return False
        return True
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Stock prepare request error: {str(e)}")
        return False


def prepare_payment(transaction_id: str, order_id: str, user_id: str, amount: int) -> bool:
    """
    Send prepare request to payment service
    Returns True if successful, False otherwise
    """
    try:
        response = requests.post(
            f"{GATEWAY_URL}/payment/prepare/{transaction_id}",
            json={'order_id': order_id, 'user_id': user_id, 'amount': amount}
        )
        if response.status_code != 200:
            app.logger.warning(f"Payment prepare failed: {response.text}")
            return False
        return True
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Payment prepare request error: {str(e)}")
        return False


def commit_stock(transaction_id: str) -> bool:
    """
    Send commit request to stock service
    Returns True if successful, False otherwise
    """
    try:
        response = requests.post(f"{GATEWAY_URL}/stock/commit/{transaction_id}")
        if response.status_code != 200:
            app.logger.error(f"Stock commit failed: {response.text}")
            return False
        return True
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Stock commit request error: {str(e)}")
        return False


def commit_payment(transaction_id: str) -> bool:
    """
    Send commit request to payment service
    Returns True if successful, False otherwise
    """
    try:
        response = requests.post(f"{GATEWAY_URL}/payment/commit/{transaction_id}")
        if response.status_code != 200:
            app.logger.error(f"Payment commit failed: {response.text}")
            return False
        return True
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Payment commit request error: {str(e)}")
        return False


def abort_stock(transaction_id: str):
    """Send abort request to stock service"""
    try:
        requests.post(f"{GATEWAY_URL}/stock/abort/{transaction_id}")
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Stock abort request error: {str(e)}")


def abort_payment(transaction_id: str):
    """Send abort request to payment service"""
    try:
        requests.post(f"{GATEWAY_URL}/payment/abort/{transaction_id}")
    except requests.exceptions.RequestException as e:
        app.logger.error(f"Payment abort request error: {str(e)}")


@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    """
    2-Phase Commit Protocol for checkout:
    Phase 1 (Prepare): Check if stock and payment are available
    Phase 2 (Commit/Abort): Commit if both agree, otherwise abort both
    """
    app.logger.debug(f"Checking out {order_id}")
    
    try:
        # Get order information
        order_entry: OrderValue = get_order_from_db(order_id)
        
        # Aggregate items by item_id to get total quantity per item
        items_quantities: dict[str, int] = defaultdict(int)
        for item_id, quantity in order_entry.items:
            items_quantities[item_id] += quantity
        
        # Convert to list of dicts for the prepare request
        items_list = [{'item_id': item_id, 'quantity': qty} for item_id, qty in items_quantities.items()]
        
        # Generate transaction ID for this 2PC transaction
        transaction_id = str(uuid.uuid4())
        
        # Create transaction record in order service
        try:
            with db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO order_transactions (transaction_id, order_id, status) VALUES (%s, %s, %s)",
                        (transaction_id, order_id, 'INITIATED')
                    )
                    conn.commit()
        except psycopg.Error as e:
            app.logger.error(f"Failed to create transaction record: {str(e)}")
            abort(400, DB_ERROR_STR)
        
        # ===== PHASE 1: PREPARE =====
        app.logger.debug(f"Phase 1 (Prepare): Starting prepare phase for transaction {transaction_id}")
        
        # Prepare stock
        stock_prepared = prepare_stock(transaction_id, order_id, items_list)
        if not stock_prepared:
            app.logger.warning(f"Stock prepare failed for transaction {transaction_id}")
            # Update transaction status to ABORTED
            try:
                with db_pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE order_transactions SET status = %s WHERE transaction_id = %s",
                            ('ABORTED', transaction_id)
                        )
                        conn.commit()
            except psycopg.Error:
                pass
            abort(400, "Failed to prepare stock")
        
        # Prepare payment
        payment_prepared = prepare_payment(transaction_id, order_id, order_entry.user_id, order_entry.total_cost)
        if not payment_prepared:
            app.logger.warning(f"Payment prepare failed for transaction {transaction_id}")
            # Abort stock since payment prepare failed
            abort_stock(transaction_id)
            # Update transaction status to ABORTED
            try:
                with db_pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE order_transactions SET status = %s WHERE transaction_id = %s",
                            ('ABORTED', transaction_id)
                        )
                        conn.commit()
            except psycopg.Error:
                pass
            abort(400, "User out of credit or payment service error")
        
        # ===== PHASE 2: COMMIT =====
        app.logger.debug(f"Phase 2 (Commit): Committing changes for transaction {transaction_id}")
        
        # Update transaction status to PREPARED
        try:
            with db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE order_transactions SET status = %s WHERE transaction_id = %s",
                        ('PREPARED', transaction_id)
                    )
                    conn.commit()
        except psycopg.Error:
            pass
        
        # Commit stock
        stock_committed = commit_stock(transaction_id)
        if not stock_committed:
            app.logger.error(f"Stock commit failed for transaction {transaction_id}, aborting payment")
            # Since stock commit failed, abort payment too
            abort_payment(transaction_id)
            # Update transaction status to ABORTED
            try:
                with db_pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE order_transactions SET status = %s WHERE transaction_id = %s",
                            ('ABORTED', transaction_id)
                        )
                        conn.commit()
            except psycopg.Error:
                pass
            abort(400, "Stock commit failed")
        
        # Commit payment
        payment_committed = commit_payment(transaction_id)
        if not payment_committed:
            app.logger.error(f"Payment commit failed for transaction {transaction_id}")
            # Both stock and payment should have been updated, so we can't rollback
            # This is a critical error
            try:
                with db_pool.connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "UPDATE order_transactions SET status = %s WHERE transaction_id = %s",
                            ('ABORTED', transaction_id)
                        )
                        conn.commit()
            except psycopg.Error:
                pass
            abort(400, "Payment commit failed")
        
        # ===== TRANSACTION COMMITTED =====
        # Update order status to paid
        order_entry.paid = True
        try:
            with db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE orders SET paid = %s WHERE order_id = %s",
                        (order_entry.paid, order_id)
                    )
                    cur.execute(
                        "UPDATE order_transactions SET status = %s WHERE transaction_id = %s",
                        ('COMMITTED', transaction_id)
                    )
                    conn.commit()
        except psycopg.Error:
            app.logger.error(f"Failed to update order {order_id} to paid status")
            abort(400, DB_ERROR_STR)
        
        app.logger.debug(f"Checkout successful for order {order_id} (transaction {transaction_id})")
        return jsonify({
            'status': 'success',
            'order_id': order_id,
            'transaction_id': transaction_id,
            'message': 'Checkout successful'
        }), 200
    
    except Exception as e:
        app.logger.error(f"Unexpected error during checkout: {str(e)}")
        abort(400, "Checkout failed")


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
