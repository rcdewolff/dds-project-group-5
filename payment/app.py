import logging
import os
import atexit
import uuid
import psycopg # type: ignore
from psycopg_pool import ConnectionPool # type: ignore
from psycopg.rows import dict_row # type: ignore
import threading
import json as std_json
from msgspec import Struct, json
from flask import Flask, jsonify, abort, Response
from services import utils


DB_ERROR_STR = "DB error"


app = Flask("payment-service")
service_name = "payment"
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
        # Reconnection policy
        reconnect_timeout=30,
        kwargs={"connect_timeout": 10}
    )



def consume_messages(consumer):
    """Background task to process Kafka messages."""
    print("Kafka consumer started...")
    for message in consumer:
        result = utils.decode_and_type_event(message)
        if isinstance(result, utils.Failure):
            print(f"Failed to decode message: {result.error}")
            # TODO Handle validation error response

            continue
            
        event = result.value
        print(f"Received message on topic {message.topic}: {event.event_type}.") 
        handle_checkout(event)


def dispatch_event(event: utils.BaseEvent):
    if event.event_type == utils.Commands.START_PAYMENT:
        handle_checkout(event)
    else:
        print(f"Unknown event type: {event.event_type}")


def handle_checkout(event: utils.BaseEvent):
    # Create Query
    result = remove_user_credit(event.payload.user_id, event.payload.amount)
    # print(f"Payment processing result for user: {event.payload.user_id}, order: {event.payload.order_id}: {result}")
    # If valid, emit valid event
    if isinstance(result, utils.Success):
        new_event = utils.BaseEvent(
            event_type=utils.PaymentIntegrationEvent.PAYMENT_SUCCEEDED,
            payload=utils.PaymentProcessedPayload(
                order_id=event.payload.order_id,
                user_id=event.payload.user_id,
                amount=event.payload.amount,
                remaining_credit= result.value
            ),
            correlation_id=event.correlation_id,
            saga_id=event.saga_id
        )

        kafka_producer.send( # type: ignore
            topic="order.request",
            value=new_event
        )
    
    else: 
        new_event = utils.BaseEvent(
            event_type=utils.PaymentIntegrationEvent.PAYMENT_FAILED,
            payload=utils.PaymentFailedPayload(
                order_id=event.payload.order_id,
                user_id=event.payload.user_id,
                amount=event.payload.amount,
                reason=result.error
            ),
            correlation_id=event.correlation_id,
            saga_id=event.saga_id
        )

        kafka_producer.send( # type: ignore
            topic="order.request",
            value=new_event
        )




def init_db():
    """Initialize event-store and projection tables"""
    tmp_pool = init_db_pool()
    with tmp_pool.connection() as conn:
        with conn.cursor() as cur:
            # Append-only event log
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    version INTEGER NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now()
                )
            """)

            # Lightweight projection (mutable) to make reads efficient while
            # keeping the events table append-only. 
            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_snapshots (
                    user_id TEXT PRIMARY KEY,
                    credit INTEGER NOT NULL,
                    version INTEGER NOT NULL
                )
            """)

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


# Initialize database on startup and create a shared pool
init_db()
# create a global pool for the app to use
db_pool = init_db_pool()
atexit.register(close_db_connection)


class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue | None:
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "SELECT credit, version FROM user_snapshots WHERE user_id = %s",
                    (user_id,)
                )
                row = cur.fetchone()
                if row is not None:                    
                    return UserValue(credit=row['credit'])
            
                # No snapshot: replay events to reconstruct
                cur.execute(
                    "SELECT event_type, payload FROM events WHERE user_id = %s ORDER BY id",
                    (user_id,)
                )
                
                event_rows = cur.fetchall()
                if not event_rows:
                    abort(400, f"User: {user_id} not found!")
                
            
                credit = 0
                for event in event_rows:
                    
                    event_type = event['event_type']
                    payload = event['payload']
                    
                    if event_type == 'USER_CREATED':
                        credit = payload.get('credit', 0)
                        
                    elif event_type == 'FUNDS_ADDED':
                        credit += int(payload.get('amount', 0))
                    
                    elif event_type == 'FUNDS_DEBITED':
                        credit -= int(payload.get('amount', 0))
                return UserValue(credit=credit)
                
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)

    return UserValue(credit=row['credit'])


@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                # Append creation event and create initial snapshot
                version = 1
                payload = std_json.dumps({"credit": 0})
                cur.execute(
                    "INSERT INTO events (aggregate_id, event_type, payload, version) VALUES (%s, %s, %s, %s)",
                    (key, 'USER_CREATED', payload, version)
                )
                cur.execute(
                    "INSERT INTO user_snapshots (user_id, credit, version) VALUES (%s, %s, %s)",
                    (key, 0, version)
                )
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n = int(n)
    starting_money = int(starting_money)
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                for i in range(n):
                    user_id = f"{i}"
                    version = 1
                    payload = std_json.dumps({"credit": starting_money})
                    cur.execute(
                        "INSERT INTO events (aggregate_id, event_type, payload, version) VALUES (%s, %s, %s, %s)",
                        (user_id, 'USER_CREATED', payload, version)
                    )
                    cur.execute(
                        "INSERT INTO user_snapshots (user_id, credit, version) VALUES (%s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET credit = EXCLUDED.credit, version = EXCLUDED.version",
                        (user_id, starting_money, version)
                    )
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user_entry: UserValue = get_user_from_db(user_id) # type: ignore
    return jsonify(
        {
            "user_id": user_id,
            "credit": user_entry.credit
        }
    )




@app.post('/pay/<user_id>/<amount>')
def http_remove_credit(user_id: str, amount: int):
    # Use event-sourced removal to ensure atomic check-and-append semantics
    result = remove_user_credit(user_id, amount)
    if isinstance(result, utils.Success):
        return Response(f"User: {user_id} credit updated to: {result.value}", status=200)
    else:
        return abort(400, result.error)




def load_aggregate_state():
    """Load aggregate state from database on startup"""
    # Extract all the events related to 


def remove_user_credit(user_id: str, amount: int) -> utils.CreditResult:
    app.logger.debug(f"Removing {amount} credit from user: {user_id}")
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    # Read WITHOUT lock
                    cur.execute(
                        "SELECT credit, version FROM user_snapshots WHERE user_id = %s",
                        (user_id,)
                    )
                    row = cur.fetchone()
                    if row is None:
                        return utils.Failure(f"User: {user_id} not found")

                    current_credit = int(row['credit'])
                    current_version = int(row['version'])

                    if current_credit - int(amount) < 0:
                        return utils.Failure("Insufficient credit")

                    new_credit = current_credit - int(amount)
                    new_version = current_version + 1
                    payload = std_json.dumps({"amount": int(amount)})

                    event_id = str(uuid.uuid4())
                    cur.execute(
                        "INSERT INTO events (id, user_id, event_type, payload, version) VALUES (%s, %s, %s, %s, %s)",
                        (event_id, user_id, 'FUNDS_DEBITED', payload, new_version)
                    )

                    # Version guard: only succeeds if no one wrote between our read and write
                    cur.execute(
                        """UPDATE user_snapshots
                           SET credit = %s, version = %s
                           WHERE user_id = %s AND version = %s""",
                        (new_credit, new_version, user_id, current_version)
                    )

                    if cur.rowcount == 0:
                        # Conflict detected — rollback and retry
                        conn.rollback()
                        app.logger.warning(f"Version conflict user {user_id}, retry {attempt + 1}/{MAX_RETRIES}")
                        continue

                    app.logger.info(f"User: {user_id} credit updated to: {new_credit}")
                    return utils.Success(new_credit)

        except psycopg.Error as e:
            app.logger.error(f"Database error: {e}")
            return utils.Failure(DB_ERROR_STR)

    return utils.Failure("Too many concurrent updates, please retry")


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    # Read WITHOUT lock
                    cur.execute(
                        "SELECT credit, version FROM user_snapshots WHERE user_id = %s",
                        (user_id,)
                    )
                    row = cur.fetchone()
                    if row is None:
                        return abort(400, f"User: {user_id} not found!")

                    new_credit = int(row['credit']) + int(amount)
                    current_version = int(row['version'])
                    new_version = current_version + 1
                    payload = std_json.dumps({"amount": int(amount)})

                    event_id = str(uuid.uuid4())
                    cur.execute(
                        "INSERT INTO events (id, user_id, event_type, payload, version) VALUES (%s, %s, %s, %s, %s)",
                        (event_id, user_id, 'FUNDS_ADDED', payload, new_version)
                    )

                    cur.execute(
                        """UPDATE user_snapshots
                           SET credit = %s, version = %s
                           WHERE user_id = %s AND version = %s""",
                        (new_credit, new_version, user_id, current_version)
                    )

                    if cur.rowcount == 0:
                        conn.rollback()
                        app.logger.warning(f"Version conflict user {user_id}, retry {attempt + 1}/{MAX_RETRIES}")
                        continue

                    return Response(f"User: {user_id} credit updated to: {new_credit}", status=200)

        except psycopg.Error:
            return abort(400, DB_ERROR_STR)

    return abort(409, "Too many concurrent updates, please retry")


@app.get('/users')
def get_users():
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT user_id, credit FROM user_snapshots")
                rows = cur.fetchall()
                return jsonify(rows)
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
