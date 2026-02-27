import logging
import os
import atexit
import uuid
import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
import threading
from msgspec import Struct, json
from flask import Flask, jsonify, abort, Response
from services import utils


DB_ERROR_STR = "DB error"


app = Flask("payment-service")
service_name = "payment"
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


def consume_messages(consumer):
    """Background task to process Kafka messages."""
    if consumer is None:
        print("Consumer is None, exiting thread.")
        return
    
    print("Kafka consumer started...")
    for message in consumer:
        
        event = json.decode(
            message.value, 
            type=utils.BaseEvent
        )

        print(f"Received message on topic {message.topic}: {event.event_type}") 
        handle_event(event)

def handle_event(event: utils.BaseEvent):
    pass


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

def remove_user_credit(user_id: str, amount: int):
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
                    """
                    UPDATE users 
                    SET credit = credit - %s 
                    WHERE user_id = %s 
                    AND credit - %s >= 0
                    RETURNING credit;",
                    """,
                    (user_entry.credit, user_id)
                )
                result = cur.fetchone()
                if result is None:
                    return False
                return True

        
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
