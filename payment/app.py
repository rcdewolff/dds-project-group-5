import logging
import os
import atexit
import uuid
import psycopg
import json
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
import threading
from msgspec import Struct
from flask import Flask, jsonify, abort, Response, request
from kafka_service import kafka_client, kafka_event
DB_ERROR_STR = "DB error"


app = Flask("payment-service")
service_name = "payment"
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
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    credit INTEGER NOT NULL
                )
            """)
            # Create 2PC transaction table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS payment_transactions (
                    transaction_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PREPARED',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            # Create hold table for holding credit during prepare phase
            cur.execute("""
                CREATE TABLE IF NOT EXISTS credit_holds (
                    hold_id TEXT PRIMARY KEY,
                    transaction_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    FOREIGN KEY (transaction_id) REFERENCES payment_transactions(transaction_id) ON DELETE CASCADE
                )
            """)
            # conn.commit()

def close_db_connection():
    db_pool.close()


# Initialize database on startup
init_db()
atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT credit FROM users WHERE user_id = %s",
                    (user_id,)
                )
                row = cur.fetchone()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    
    if row is None:
        abort(400, f"User: {user_id} not found!")
    
    return UserValue(credit=row['credit'])


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO users (user_id, credit) VALUES (%s, %s)",
                    (key, 0)
                )
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    
    values = [(f"{i}", starting_money) for i in range(n)]
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO users (user_id, credit) VALUES (%s, %s)",
                    values
                )
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id)
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit
    user_entry.credit += int(amount)
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET credit = %s WHERE user_id = %s",
                    (user_entry.credit, user_id)
                )
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    user_entry: UserValue = get_user_from_db(user_id)
    # update credit
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE users SET credit = %s WHERE user_id = %s",
                    (user_entry.credit, user_id)
                )
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


# ============ 2-Phase Commit Endpoints ============

@app.post('/prepare/<transaction_id>')
def prepare_payment(transaction_id: str):
    """Prepare phase: Check if user has sufficient credit and hold it"""
    try:
        data = request.get_json() or {}
        order_id = data.get('order_id')
        user_id = data.get('user_id')
        amount = int(data.get('amount', 0))
        
        if not order_id or not user_id or amount <= 0:
            abort(400, "Missing order_id, user_id or invalid amount in request")
        
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Check user's current credit
                cur.execute(
                    "SELECT credit FROM users WHERE user_id = %s",
                    (user_id,)
                )
                row = cur.fetchone()
                if row is None:
                    abort(400, f"User {user_id} not found")
                if row['credit'] < amount:
                    abort(400, f"Insufficient credit for user {user_id}. Available: {row['credit']}, Required: {amount}")
                
                # All checks passed - create transaction record
                cur.execute(
                    "INSERT INTO payment_transactions (transaction_id, order_id, user_id, amount, status) VALUES (%s, %s, %s, %s, %s)",
                    (transaction_id, order_id, user_id, amount, 'PREPARED')
                )
                
                # Create hold (don't actually deduct credit yet)
                hold_id = f"{transaction_id}_hold"
                cur.execute(
                    "INSERT INTO credit_holds (hold_id, transaction_id, user_id, amount) VALUES (%s, %s, %s, %s)",
                    (hold_id, transaction_id, user_id, amount)
                )
                
                conn.commit()
        
        return jsonify({'status': 'PREPARED', 'transaction_id': transaction_id}), 200
    
    except psycopg.Error as e:
        abort(400, f"DB error in prepare: {str(e)}")


@app.post('/commit/<transaction_id>')
def commit_payment(transaction_id: str):
    """Commit phase: Actually deduct the user's credit"""
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Get the transaction
                cur.execute(
                    "SELECT * FROM payment_transactions WHERE transaction_id = %s",
                    (transaction_id,)
                )
                trans = cur.fetchone()
                if trans is None:
                    abort(400, f"Transaction {transaction_id} not found")
                
                user_id = trans['user_id']
                amount = trans['amount']
                
                # Deduct credit
                cur.execute(
                    "UPDATE users SET credit = credit - %s WHERE user_id = %s",
                    (amount, user_id)
                )
                
                # Update transaction status
                cur.execute(
                    "UPDATE payment_transactions SET status = %s WHERE transaction_id = %s",
                    ('COMMITTED', transaction_id)
                )
                
                conn.commit()
        
        return jsonify({'status': 'COMMITTED', 'transaction_id': transaction_id}), 200
    
    except psycopg.Error as e:
        abort(400, f"DB error in commit: {str(e)}")


@app.post('/abort/<transaction_id>')
def abort_payment(transaction_id: str):
    """Abort phase: Release held credit without deducting"""
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                # Delete holds
                cur.execute(
                    "DELETE FROM credit_holds WHERE transaction_id = %s",
                    (transaction_id,)
                )
                
                # Update transaction status
                cur.execute(
                    "UPDATE payment_transactions SET status = %s WHERE transaction_id = %s",
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
