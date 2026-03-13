import logging
import os
import atexit
import random
import time
from typing import Dict
import uuid
import redis # type: ignore
import psycopg # type: ignore
from psycopg_pool import ConnectionPool # type: ignore
from psycopg.rows import dict_row # type: ignore
import requests
from flask import Flask, jsonify, abort, Response
from services import utils
from msgspec import json, Struct
import threading
from orchestrator_sync import CheckoutSagaOrchestrator


SAGA_TIMEOUT_SECONDS = 30
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
SAGA_TIMEOUT_SECONDS = 30.0

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

service_name = "order"
kafka_producer = None
kafka_consumer = None
_pending_sagas: Dict[str, dict] = {}
_pending_sagas_lock = threading.Lock()


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
        handle_event(event)



# Instantiate once at module level — reused across all requests
# kafka_producer and db_pool are set in post_fork so we pass them lazily via a factory
def get_orchestrator() -> CheckoutSagaOrchestrator:
    return CheckoutSagaOrchestrator(
        kafka_producer=kafka_producer,
        redis_client=redis_client,
        db_pool=db_pool,
        timeout_seconds=SAGA_TIMEOUT_SECONDS,
    )



def init_db():
    """Initialize database table"""
    tmp_pool = init_db_pool()
    with tmp_pool.connection() as conn:
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

            cur.execute("""
                CREATE TABLE IF NOT EXISTS sagas (
                    order_id TEXT NOT NULL,
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    step TEXT NOT NULL,
                    results JSONB
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS log (
                    id TEXT PRIMARY KEY,   
                    order_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    data TEXT NOT NULL
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


def log_event(event: utils.BaseEvent) -> bool: # type: ignore
    
    event_details = json.encode(event)

    query = """
        INSERT INTO log (order_id, event_type, data) 
        VALUES (%s,%s::jsonb,%s)
        """
    
    with db_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                query, 
                # TODO Make sure this correlation id is right
                (event.correlation_id, event.event_type,event_details)
            )
           
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
    empty_items = []
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO orders (order_id, paid, items, user_id, total_cost) VALUES (%s, %s, %s::jsonb, %s, %s)",
                    (key, False, empty_items, user_id, 0)
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
        return (f"{order_id}", False, json.encode(list()), f"{user_id}", 2*item_price)

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
    order_entry: OrderValue = get_order_from_db(order_id) # type: ignore
    return jsonify(
        {
            "order_id": order_id,
            "paid": order_entry.paid,
            "items": order_entry.items,
            "user_id": order_entry.user_id,
            "total_cost": order_entry.total_cost
        }
    )


@app.get("/routes")
def list_routes():
    return {"routes": [str(rule) for rule in app.url_map.iter_rules()]}

@app.get("/test/<service>")
def test_kafka(service: str):
    test_msg = "TEST"
    print(f"Order service testing kafka on {service}...")
    order_id=str(uuid.uuid4())

    payload = utils.OrderCheckoutPayload(
        order_id=order_id,
        items=[]
    )

    event = utils.BaseEvent.create(
        event_type="CHECKOUT_INITIATED",
        payload=payload,
    )

    print("Sending message via kafka...")
    try: 
        kafka_producer.send( # type: ignore
            topic = f'stock.request',
            value=event
        )   
    except:
        print("Kafka producer failed...")
        return {
            "result":"Server error"
        }, 500
    finally:
        print("Should be fine")
    # Create a flag event that can be set to true 
    wait_event = threading.Event()
    with _pending_sagas_lock:
        # Save the event using the correlation id
        _pending_sagas[event.correlation_id] = {
            "event": wait_event,
            "result": None
        }

    kafka_producer.send(topic='stock.request', value=event) # type: ignore

    # Block until Kafka consumer resolves the saga or timeout expires
    completed = wait_event.wait(timeout=SAGA_TIMEOUT_SECONDS)

    with _pending_sagas_lock:
        saga_entry = _pending_sagas.pop(event.correlation_id, None)
    
    print(f"Saga details: {completed}, {saga_entry}")

    if not completed or saga_entry is None:
        return {
            "status": "timeout",
            "order_id": event.correlation_id,
            "message": "Saga did not complete in time."
        }, 504

    result = saga_entry["result"]
    if result["status"] == "success":
        return {
            "status": "success",
            "correlation_id": event.correlation_id,
            "message": "Checkout completed successfully."
        }, 200
    else:
        return {
            "status": "failed",
            "correlation_id": event.correlation_id,
            "message": result.get("reason", "Checkout failed.")
        }, 400
    


# TODO Delete this
def send_post_request(url: str):
    print('Placeholder')
    response = None
   


# TODO Delete this
def send_get_request(url: str):
    try:
        response = requests.get(url)
    except requests.exceptions.RequestException:
        abort(400, REQ_ERROR_STR)
    else:
        return response


@app.post('/addItem/<order_id>/<item_id>/<int:quantity>')
def add_item(order_id: str, item_id: str, quantity: int):
    app.logger.info(f"item: {item_id} quantity: {quantity} order: {order_id}")
    order_entry: OrderValue = get_order_from_db(order_id) # type: ignore
    
    # Convert items to JSON format for storage
    items_json = [{'item_id': item[0], 'quantity': item[1]} for item in order_entry.items]
    items_json.append({'item_id': item_id, 'quantity': quantity})
    serialized_items = json.encode(items_json).decode()
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    "UPDATE orders SET items = %s::jsonb, total_cost = %s WHERE order_id = %s",
                    (serialized_items, order_entry.total_cost, order_id)
                )
                # conn.commit()
    except psycopg.Error as e:
        # .exception() automatically includes the traceback
        app.logger.exception(f"Database error while updating order {order_id}")
        
        # Or print specific details if you prefer a single line:
        # app.logger.error(f"SQL Error: {e.pgcode} - {e}")
        
        return abort(400, f"Database error: {str(e)}")

    items_as_dicts = [
        {"item_id": item[0], "quantity": item[1]} 
        for item in order_entry.items
    ]

    return jsonify({
        "order_id": order_id,
        "items": items_as_dicts, # Now a list of dicts
        "user_id": order_entry.user_id
    }),200

def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


def handle_rollback(event: utils.BaseEvent):
    
    pass




@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    order_value: OrderValue = get_order_from_db(order_id) # type: ignore
    orchestrator = get_orchestrator()

    if orchestrator.check_saga_exists(order_id):
        return {
            "status": "pending",
            "order_id": order_id,
            "message": "Saga already in progress.",
        }, 202

    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                # Use order_id as idempotency key — prevents double checkout
                cur.execute(
                    """INSERT INTO received_events (event_id, event_type)
                       VALUES (%s, %s)
                    """,
                    (order_id, utils.OrderInternalEvent.CHECKOUT_INITIATED)
                )
                
    except psycopg.Error as e:
        app.logger.error(f"Failed to log checkout event: {e}")
        return abort(400, DB_ERROR_STR)

    app.logger.info(f"Starting checkout for order: {order_id}")

    return orchestrator.run(order_id, order_value)


def handle_decoding_error(result: utils.Failure):
    app.logger.error(f"Failed to decode saga response: {result.error}")
    if result.error == "UNKNOWN_EVENT_TYPE":
        return {
            "status": "failed",
            "message": f"Received unknown event type: {result.error}"
        }, 400
    elif result.error == "EMPTY_MESSAGE":
        return {
            "status": "failed",
            "message": "Received empty message value (Tombstone)"
        }, 400
    else:
        return None

def handle_event(event: utils.BaseEvent):
    event_type = event.event_type
    correlation_id = event.correlation_id

    if event_type == utils.StockIntegrationEvent.STOCK_ALLOCATED:
        # log success message
        app.logger.info("Stock reservation success.")

        # _resolve_saga(correlation_id, {"status": "success"})
        payload: utils.StockReservedPayload = event.payload
        trigger_payment(
            correlation_id=correlation_id, 
            order_id=payload.order_id,
            amount=payload.amount,
            saga_id=event.saga_id
        )

    elif event_type == utils.StockIntegrationEvent.STOCK_UNAVAILABLE:
        app.logger.info("Stock reservation failed.")
        # TODO implement rollback logic if needed (probably not since stock reservation is the first step)
        
    

    elif event_type == utils.PaymentIntegrationEvent.PAYMENT_FAILED:
        app.logger.info(f"Payment failed for reason: {event.payload.reason}")
        # TODO implement rollback logic
        
def trigger_payment(correlation_id: str, order_id: str, amount: int, saga_id: str):
    """
        Triggered after the stock has been reserved.
    """
    order_value: OrderValue = get_order_from_db(order_id) # type: ignore
    
    event = utils.BaseEvent(
        event_type=utils.Commands.START_PAYMENT,
        correlation_id=correlation_id,
        saga_id = saga_id,
        payload=utils.StartPaymentCommandPayload(
            order_id=order_id,
            user_id=order_value.user_id,
            amount=amount
        )
    )

    kafka_producer.send( # type: ignore
        topic='payment.request',
        value=event
    )
    app.logger.info(f"Triggered payment for order: {order_id}, correlation_id: {correlation_id}")


    







if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
