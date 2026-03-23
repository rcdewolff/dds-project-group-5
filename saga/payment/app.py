import logging
import os
import atexit
import uuid
import psycopg # type: ignore
from psycopg_pool import ConnectionPool # type: ignore
from psycopg.rows import dict_row # type: ignore
from msgspec import Struct
from flask import Flask, jsonify, abort, Response
from services import utils
from db_repository import PaymentRepository


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
        correlation_id = utils.event_correlation_id(event)

        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                repo = PaymentRepository(cur)
                if repo.inbox_event_exists(event.id):
                    app.logger.info("Payment inbox dedupe hit event_id=%s correlation_id=%s", event.id, correlation_id)
                    conn.commit()
                    continue

                raw_payload = message.value.decode() if isinstance(message.value, bytes) else str(message.value)
                repo.insert_inbox_event(
                    event_id=event.id,
                    topic=message.topic,
                    partition=message.partition,
                    kafka_offset=message.offset,
                    correlation_id=correlation_id,
                    payload=event,
                    payload_hash=utils.payload_hash(raw_payload),
                )

        print(f"Received message on topic {message.topic}: {event.event_type}.") 
        handle_checkout(event)

        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                repo = PaymentRepository(cur)
                repo.set_inbox_event_result(
                    event_id=event.id,
                    status="PROCESSED",
                    result={"status": "processed", "event_type": event.event_type},
                )


def dispatch_event(event: utils.BaseEvent):
    if event.event_type == utils.Commands.START_PAYMENT:
        handle_checkout(event)
    else:
        print(f"Unknown event type: {event.event_type}")


def handle_checkout(event: utils.BaseEvent):
    # Create Query
    result = remove_user_credit(event)

    # print(f"Payment processing result for user: {event.payload.user_id}, order: {event.payload.order_id}: {result}")
    # If valid, emit valid event
    if isinstance(result, utils.Success):
        # Log the message and result for debugging
        print(f"Payment succeeded for user: {event.payload.user_id}, order: {event.payload.order_id}. Remaining credit: {result.value}")
    
    else: 
        print(f"Payment failed for user: {event.payload.user_id}, order: {event.payload.order_id}. Reason: {result.error}")
        




def init_db():
    """Initialize payment persistence tables."""
    tmp_pool = init_db_pool()
    with tmp_pool.connection() as conn:
        with conn.cursor() as cur:
            repo = PaymentRepository(cur)
            repo.create_tables()
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
                repo = PaymentRepository(cur)
                row = repo.get_account(user_id)
                if row is None:
                    abort(400, f"User: {user_id} not found!")
                return UserValue(credit=row['credit'])
                
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)

@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                repo = PaymentRepository(cur)
                repo.insert_account(key, 0)
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
                repo = PaymentRepository(cur)
                for i in range(n):
                    user_id = f"{i}"
                    repo.upsert_account(user_id, starting_money, 1)
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
    # Direct HTTP mode: update state and reply immediately without inbox/outbox writes.
    result = remove_user_credit_direct(user_id, int(amount))
    if isinstance(result, utils.Success):
        return Response(f"User: {user_id} credit updated to: {result.value}", status=200)
    else:
        return abort(400, result.error)

def load_aggregate_state():
    """Load aggregate state from database on startup"""
    # Extract all the events related to 


def remove_user_credit_direct(user_id: str, amount: int):
    """Synchronous debit path used by HTTP endpoint; no inbox/outbox side effects."""
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    repo = PaymentRepository(cur)
                    row = repo.get_account(user_id)
                    if row is None:
                        return utils.Failure(f"User: {user_id} not found")

                    current_credit = int(row['credit'])
                    current_version = int(row['version'])
                    if current_credit - int(amount) < 0:
                        return utils.Failure("Insufficient credit")

                    new_credit = current_credit - int(amount)
                    new_version = current_version + 1

                    if repo.update_account_versioned(user_id, new_credit, new_version, current_version):
                        app.logger.info(f"User: {user_id} credit updated to: {new_credit}")
                        return utils.Success(new_credit)

                    conn.rollback()
                    app.logger.warning(f"Version conflict user {user_id}, retry {attempt + 1}/{MAX_RETRIES}")

        except psycopg.Error as e:
            app.logger.error(f"Database error: {e}")
            return utils.Failure(DB_ERROR_STR)

    return utils.Failure("Too many concurrent updates, please retry")


def remove_user_credit(event: utils.BaseEvent) :
    app.logger.debug(f"Removing {event.payload.amount} credit from user: {event.payload.user_id}")
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    repo = PaymentRepository(cur)
                    correlation_id = utils.event_correlation_id(event)
                    row = repo.get_account(event.payload.user_id)
                    if row is None:
                        # Write Outbox message for failed payment due to user not found
                        repo.insert_outbox_message(
                            topic="order.request",
                            payload=utils.BaseEvent(
                                id=str(uuid.uuid4()),
                                event_type=utils.PaymentIntegrationEvent.PAYMENT_FAILED,
                                payload=utils.PaymentFailedPayload(
                                    order_id=event.payload.order_id,  # This would be filled in a real implementation
                                    user_id=event.payload.user_id,
                                    amount=event.payload.amount,
                                    reason="User not found"
                                ),
                                order_id=event.payload.order_id,
                                correlation_id=correlation_id,
                                saga_id=correlation_id,
                            ),
                            message_key=correlation_id,
                            correlation_id=correlation_id,
                        )
                        return utils.Failure(f"User: {event.payload.user_id} not found")

                    current_credit = int(row['credit'])
                    current_version = int(row['version'])

                    if current_credit - int(event.payload.amount) < 0:
                        # Write outbox message for failed payment due to insufficient credit
                        repo.insert_outbox_message(
                            topic="order.request",
                            payload=utils.BaseEvent(
                                id=str(uuid.uuid4()),
                                event_type=utils.PaymentIntegrationEvent.PAYMENT_FAILED,
                                payload=utils.PaymentFailedPayload(
                                    order_id=event.payload.order_id,  # This would be filled in a real implementation
                                    user_id=event.payload.user_id,
                                    amount=event.payload.amount,
                                    reason="Insufficient credit"
                                ),
                                order_id=event.payload.order_id,
                                correlation_id=correlation_id,
                                saga_id=correlation_id,
                            ),
                            message_key=correlation_id,
                            correlation_id=correlation_id,
                        )
                        return utils.Failure("Insufficient credit")

                    new_credit = current_credit - int(event.payload.amount)
                    new_version = current_version + 1

                    if repo.update_account_versioned(event.payload.user_id, new_credit, new_version, current_version):
                        app.logger.info(f"User: {event.payload.user_id} credit updated to: {new_credit}")
                        # Write to outbox
                        repo.insert_outbox_message(
                            topic="order.request",
                            payload=utils.BaseEvent(
                                id=str(uuid.uuid4()),
                                event_type=utils.PaymentIntegrationEvent.PAYMENT_SUCCEEDED,
                                payload=utils.PaymentProcessedPayload(
                                    order_id=event.payload.order_id,  # This would be filled in a real implementation
                                    user_id=event.payload.user_id,
                                    amount=event.payload.amount,
                                    remaining_credit=new_credit
                                ),
                                order_id=event.payload.order_id,
                                correlation_id=correlation_id,
                                saga_id=correlation_id,
                            ),
                            message_key=correlation_id,
                            correlation_id=correlation_id,
                        )  
                        return utils.Success(new_credit)
                        

                    
                    # Conflict detected — rollback and retry
                    conn.rollback()
                    app.logger.warning(f"Version conflict user {event.payload.user_id}, retry {attempt + 1}/{MAX_RETRIES}")
                    
                    if attempt == MAX_RETRIES - 1:
                    # Write outbox message for failed payment due to version conflict
                        repo.insert_outbox_message(
                            topic="order.request",
                            payload=utils.BaseEvent(
                                id=str(uuid.uuid4()),
                                event_type=utils.PaymentIntegrationEvent.PAYMENT_FAILED,
                                payload=utils.PaymentFailedPayload(
                                    order_id=event.payload.order_id,  # This would be filled in a real implementation
                                    user_id=event.payload.user_id,
                                    amount=event.payload.amount,
                                    reason="Version conflict, please retry"
                                ),
                                order_id=event.payload.order_id,
                                correlation_id=correlation_id,
                                saga_id=correlation_id,
                            ),
                            message_key=correlation_id,
                            correlation_id=correlation_id,
                        )
                        

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
                    repo = PaymentRepository(cur)
                    row = repo.get_account(user_id)
                    if row is None:
                        return abort(400, f"User: {user_id} not found!")

                    new_credit = int(row['credit']) + int(amount)
                    current_version = int(row['version'])
                    new_version = current_version + 1

                    if not repo.update_account_versioned(user_id, new_credit, new_version, current_version):
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
                repo = PaymentRepository(cur)
                rows = repo.list_accounts()
                return jsonify(rows)
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
