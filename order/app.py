import logging
import os
import atexit
import random
from typing import Dict
import uuid
from collections import defaultdict

import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
import requests
from flask import Flask, jsonify, abort, Response
from services import utils
from msgspec import json, Struct
import threading

DB_ERROR_STR = "DB error"
REQ_ERROR_STR = "Requests error"
SAGA_TIMEOUT_SECONDS = 20.0

GATEWAY_URL = os.environ['GATEWAY_URL']

app = Flask("order-service")

service_name = "order"
kafka_producer = None
kafka_consumer = None
_pending_sagas: Dict[str, dict] = {}
_pending_sagas_lock = threading.Lock()



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
    print("Kafka consumer started...")
    for message in consumer:
        print(f"Received message on topic {message.topic}: {message.value}") 
        event = json.decode(
            message.value, 
            type=utils.BaseEvent
        )

        handle_event(event)

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
            
            # conn.commit()


def log_event(event: utils.BaseEvent) -> bool:
    # Event json to be stored
    event_details = json.encode(event)

    query = """
        INSERT INTO log (idempotency_key, event_type, details) 
        VALUES (%s,%s,%s)
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
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO orders (order_id, paid, items, user_id, total_cost) VALUES (%s, %s, %s, %s, %s)",
                    (key, False, json.encode(list()), user_id, 0)
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
        kafka_producer.send(
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

    kafka_producer.send(topic='stock.request', value=event)

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


# TODO Improve naming
# def handle_stock_ok(event: utils.BaseEvent):
#     """
#         Handler for the kafka message that notifies that the item is valid and can 
#         be add to the order.

#         NOTE:
#         I don't know how to handle responses since we are async.
#     """
#     # TODO if the response event is negative
#     if event.event_type == "STOCK_ITEM_INVALID":

#     item_json: dict = item_reply.json()
#     order_entry.items.append((item_id, int(quantity)))
#     order_entry.total_cost += int(quantity) * item_json["price"]

#      # Convert items to JSON format for storage
#     items_json = [{'item_id': item[0], 'quantity': item[1]} for item in order_entry.items]
    
#     try:
#         with db_pool.connection() as conn:
#             with conn.cursor() as cur:
#                 cur.execute(
#                     "UPDATE orders SET items = %s, total_cost = %s WHERE order_id = %s",
#                     (json.dumps(items_json), order_entry.total_cost, order_id)
#                 )
#                 # conn.commit()

@app.post('/addItem/<order_id>/<item_id>/<quantity>')
def add_item(order_id: str, item_id: str, quantity: int):

    order_entry: OrderValue = get_order_from_db(order_id)
    
    # Convert items to JSON format for storage
    items_json = [{'item_id': item[0], 'quantity': item[1]} for item in order_entry.items]
    
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE orders SET items = %s, total_cost = %s WHERE order_id = %s",
                    (json.encode(items_json), order_entry.total_cost, order_id)
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
                    (json.encode(items_json), order_entry.total_cost, order_id)
                )
                # conn.commit()
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return Response(f"Item: {item_id} added to: {order_id} price updated to: {order_entry.total_cost}",
                    status=200)



def rollback_stock(removed_items: list[tuple[str, int]]):
    for item_id, quantity in removed_items:
        send_post_request(f"{GATEWAY_URL}/stock/add/{item_id}/{quantity}")


def handle_rollback():
    pass

@app.post('/checkout/<order_id>')
def checkout(order_id: str):

    order_value: OrderValue = get_order_from_db(order_id)
    items_list = [(item_id, qty) for item_id, qty in order_value.items]

    payload = utils.OrderCheckoutPayload(order_id=order_id,items=items_list)

    event = utils.BaseEvent.create(
        event_type="CHECKOUT_INITIATED",
        payload=payload
    )

    # Register the pending saga BEFORE sending to Kafka
    # to avoid a race where the response arrives before we're listening
    wait_event = threading.Event()
    with _pending_sagas_lock:
        _pending_sagas[event.correlation_id] = {
            "event": wait_event,
            "result": None
        }

    kafka_producer.send(topic='stock.request', value=event)

    # Block until Kafka consumer resolves the saga or timeout expires
    # wait: Block until the internal flag is true.
    completed = wait_event.wait(timeout=SAGA_TIMEOUT_SECONDS)

    with _pending_sagas_lock:
        saga_entry = _pending_sagas.pop(event.correlation_id, None)

    if not completed or saga_entry is None:
        return {
            "status": "timeout",
            "order_id": order_id,
            "correlation_id": event.correlation_id,
            "message": "Saga did not complete in time."
        }, 504

    result = saga_entry["result"]

    if result["status"] == "success":
        return {
            "status": "success",
            "order_id": order_id,
            "correlation_id": event.correlation_id,
            "message": "Checkout completed successfully."
        }, 200
    else:
        return {
            "status": "failed",
            "order_id": order_id,
            "correlation_id": event.correlation_id,
            "message": result.get("reason", "Checkout failed.")
        }, 400




def handle_event(event: utils.BaseEvent):
    event_type = event.event_type
    correlation_id = event.correlation_id

    if event_type == utils.StockIntegrationEvent.STOCK_ALLOCATED:
        print("Stock reservation success.")
        # _resolve_saga(correlation_id, {"status": "success"})
        _trigger_payment(correlation_id=correlation_id, order_id=event.payload.order_id)

    elif event_type == utils.StockIntegrationEvent.STOCK_UNAVAILABLE:
        # TODO send 404 back
        _resolve_saga(correlation_id, {
            "status": "failed",
            "reason": "Stock failure: not enough items"
        })
    
    elif event_type == utils.PaymentIntegrationEvent.PAYMENT_SUCCEEDED:
        _resolve_saga(correlation_id, {
            "status":"success"
        })

    elif event_type == utils.PaymentIntegrationEvent.PAYMENT_SUCCEEDED:
        # TODO implement rollback logic
        _resolve_saga(correlation_id, {
            "status": "failed",
            "reason": "Payment failure: Not enough credit"
        })



def _trigger_payment(correlation_id: str, order_id: str, price: float):
    """
        Triggered after the stock has been reserved.
    """
    event = utils.BaseEvent(
        event_type=utils.Commands.START_PAYMENT,
        correlation_id=correlation_id,
        payload=utils.PaymentCheckoutPayload(
            order_id=order_id,
            price=price
        )
    )
    pass


def _resolve_saga(correlation_id: str, result: dict):
    """Called by the Kafka consumer to unblock the waiting HTTP handler."""
    with _pending_sagas_lock:
        entry = _pending_sagas.get(correlation_id)

    if entry is None:
        # Saga already timed out and was cleaned up — discard the result
        return

    entry["result"] = result
    entry["event"].set()  # unblocks wait_event.wait() in the HTTP handler








if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
