import logging
import os
import atexit
import uuid

import psycopg # type: ignore
from psycopg_pool import ConnectionPool # type: ignore
from psycopg.rows import dict_row # type: ignore
from collections import defaultdict

from msgspec import Struct, json
from flask import Flask, jsonify, abort, Response
from services import utils
from db_repository import StockRepository


DB_ERROR_STR = "DB error"

app = Flask("payment-service")
service_name = "stock"

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
    """Initialize event-store and snapshot tables"""
    tmp_pool = init_db_pool()
    with tmp_pool.connection() as conn:
        with conn.cursor() as cur:
            repo = StockRepository(cur)
            repo.create_tables()

            
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
                repo = StockRepository(cur)
                row = repo.get_item_snapshot(item_id)

                if row is not None:
                    return StockValue(stock=row['stock'], price=row['price'])

                # Fallback: replay events to rebuild state
                event_rows = repo.get_log_events_for_item(item_id)
                if not event_rows:
                    abort(400, f"Item: {item_id} not found!")

                stock, price = 0, 0
                for event in event_rows:
                    et = event['event_type']
                    p = event['payload']
                    if et == 'ITEM_CREATED':
                        stock = p.get('stock', 0)
                        price = p.get('price', 0)
                    elif et == 'STOCK_ADDED':
                        stock += int(p.get('amount', 0))
                    elif et == 'STOCK_SUBTRACTED':
                        stock -= int(p.get('amount', 0))

                return StockValue(stock=stock, price=price)

    except psycopg.Error:
        return abort(400, DB_ERROR_STR)



def consume_messages(consumer):
    """Background task to process Kafka messages."""
    print("Kafka consumer started...")
    for message in consumer:
        result = utils.decode_and_type_event(message)
        if isinstance(result, utils.Failure):
            print(f"Failed to decode message: {result.error}")
            # Write to outbox
            with db_pool.connection() as conn:
                with conn.cursor() as cur:
                    repo = StockRepository(cur)
                    error_event = utils.build_generic_error_event(
                        order_id=str(uuid.uuid4()),
                        saga_id=str(uuid.uuid4()),
                        error_message=f"Failed to decode message: {result.error}",
                    )
                    repo.insert_outbox_message('order.request', error_event)
            continue
            
        event = result.value
        print(f"Received message on topic {message.topic}: {event.event_type}.")

        if isinstance(is_already_processed(event), utils.Success):
            print(f"Event {event.saga_id} already processed, skipping.")
            handle_already_processed(event)
            continue
            

        dispatch_event(event)
            
        
def is_already_processed(event: utils.BaseEvent):
    with db_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            repo = StockRepository(cur)
            if repo.received_event_exists(event.id):
                return utils.Success(0)  # Already processed, skip
            
            # Mark event as received for idempotency
            repo.insert_received_event(event.id)


def dispatch_event(event: utils.BaseEvent, already_processed=False):

    if already_processed:
        handle_already_processed(event)
        return

    if event.event_type == utils.Commands.RESERVE_STOCK:
        handle_stock_reservation(event)
    elif event.event_type == utils.Commands.FREE_STOCK:
        handle_rollback(event)



def handle_already_processed(event: utils.BaseEvent):
    """
    Handle events that have already been processed.
    """
    # Extract the result from the received_events table and resend the appropriate response based on the event type
    with db_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            repo = StockRepository(cur)
            result = repo.get_received_event_result(event.id)

            # FIXME This should not happen, but we log it just in case to investigate potential issues with the idempotency mechanism
            if result is None:
                app.logger.error(f"Event {event.saga_id} marked as processed but no result found in DB")
                return

            if event.event_type == utils.Commands.RESERVE_STOCK:
                if result.get('status') == 'success':
                    response_event = utils.build_stock_allocated_event(
                        saga_id=event.saga_id,
                        order_id=event.payload.order_id,
                        amount=result.get('amount', 0),
                    )
                else:
                    response_event = utils.build_stock_unavailable_event(
                        saga_id=event.saga_id,
                        order_id=event.payload.order_id,
                    )

                # write to outbox
                repo.insert_outbox_message('order.request', response_event)
                app.logger.info(f"Resent response for already processed event {event.saga_id}: {response_event.event_type}")
            

def handle_stock_reservation(event: utils.BaseEvent):
   
    payload = event.payload
    print(f"Handling stock reservation for order: {payload.order_id} with items: {payload.items}")

    result = subtract_stock_batch(payload.order_id, payload.items, event.id, event.saga_id)
    
    if isinstance(result, utils.Success):
        app.logger.info(f"Stock reservation successful for order {payload.order_id}, total cost: {result.value}")
    else:
        app.logger.info(f"Stock reservation failed for order {payload.order_id}, reason: {result.error}")
    

def handle_rollback(event: utils.BaseEvent):

    order_id = event.payload.order_id
    qty_by_item: dict[str, int] = defaultdict(int)
    for item_id, qty in event.payload.items:
        qty_by_item[item_id] += int(qty)

    items = sorted(qty_by_item.items(), key=lambda x: x[0])
    print(f"Handling stock rollback for order: {order_id}")
    MAX_RETRIES = 3

    for attempt in range(MAX_RETRIES):
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    repo = StockRepository(cur)
                    item_ids = [item_id for item_id, _ in items]
                    rows = repo.get_item_snapshots_for_ids(item_ids)

                    conflict = False
                    for item_id, qty in items:
                        if item_id not in rows:
                            app.logger.warning(f"Rollback requested for unknown item {item_id} in order {order_id}")
                            continue
                        row = rows[item_id]
                        current_version = int(row['version'])
                        new_stock = int(row['stock']) + int(qty)
                        new_version = current_version + 1
                        app.logger.info(f"Rolling back stock for item {item_id}: adding back {qty} to stock {row['stock']} => new stock = {new_stock} (version {current_version})")
                        repo.insert_log_event(
                            item_id=item_id,
                            event_type='STOCK_ADDED',
                            payload={"order_id": order_id, "amount": int(qty)},
                            version=new_version,
                        )
                        
                        if not repo.update_snapshot_versioned(item_id, new_stock, new_version, current_version):
                            conflict = True
                            app.logger.info(f"Version conflict during stock rollback for item {item_id} in order {order_id}")
                            break

                    if conflict:
                        conn.rollback()
                        app.logger.warning(f"Version conflict in rollback for order {order_id}, retry {attempt + 1}/{MAX_RETRIES}")
                        continue

                    print(f"Stock rollback completed for order: {order_id}")
                    rollback_event = utils.build_stock_freed_event(
                        saga_id=event.saga_id,
                        order_id=order_id,
                    )

                    # Write to the outbox
                    repo.insert_outbox_message('order.request', rollback_event)
                    return

        except psycopg.Error as e:
            app.logger.error(f"Stock rollback DB error: {e}")
            return

    app.logger.error(f"Stock rollback failed after {MAX_RETRIES} retries for order: {order_id}")




def subtract_stock_batch(order_id: str, items: list[tuple[str, int]], event_id: str, saga_id: str):
    if not items:
        return utils.Failure("No items to reserve")

    items = sorted(items, key=lambda x: x[0])  # ← add this
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    repo = StockRepository(cur)
                    # Read all items WITHOUT lock
                    item_ids = [item_id for item_id, _ in items]
                    rows = repo.get_item_snapshots_for_ids(item_ids, include_price=True)

                    # Check that all items exist
                    missing = [iid for iid, _ in items if iid not in rows]
                    if missing:
                        
                        return utils.Failure(f"Items not found: {missing}")

                    # Check availability before writing anything
                    unavailable = [
                        iid for iid, qty in items
                        if int(rows[iid]['stock']) < int(qty)
                    ]
                    if unavailable:
                        # write to outbox
                        repo.insert_outbox_message(
                            'order.request',
                            utils.build_stock_unavailable_event(
                                saga_id=saga_id,
                                order_id=order_id,
                            ),
                        )
                        return utils.Failure(f"Insufficient stock for items: {unavailable}")

                    # Write phase: version-guarded update for every item
                    conflict = False
                    total_cost = 0
                    for item_id, qty in items:
                        row = rows[item_id]
                        current_version = int(row['version'])
                        new_stock = int(row['stock']) - int(qty)
                        new_version = current_version + 1
                        price = int(row['price'])

                        repo.insert_log_event(
                            item_id=item_id,
                            event_type='STOCK_SUBTRACTED',
                            payload={"order_id": order_id, "amount": int(qty)},
                            version=new_version,
                        )

                        if not repo.update_snapshot_versioned(item_id, new_stock, new_version, current_version):
                            conflict = True
                            break

                        total_cost += price * int(qty)

                    if not conflict:
                        # Log the successful reservation in the received_events table for idempotency
                        repo.set_received_event_result(
                            event_id=event_id,
                            status='PROCESSED',
                            result={"status": "success", "amount": total_cost},
                        )
                        
                        # log to outbox
                        repo.insert_outbox_message(
                            'order.request',
                            utils.build_stock_allocated_event(
                                saga_id=saga_id,
                                order_id=order_id,
                                amount=total_cost,
                            ),
                        )

                        return utils.Success(total_cost)
                    
                    
                    conn.rollback()
                    app.logger.warning(f"Version conflict in batch stock update, retry {attempt + 1}/{MAX_RETRIES}")
                    
                    # TODO Add option for transient failure (RETRYABLE_FAILURE in case of max retry reached)
                    if attempt == MAX_RETRIES - 1:
                        # Log the failure in the received_events table for idempotency
                        repo.set_received_event_result(
                            event_id=event_id,
                            status='PROCESSED',
                            result={"status": "failure", "reason": "version conflict"},
                        )

                        repo.insert_outbox_message(
                            'order.request',
                            utils.build_stock_unavailable_event(
                                saga_id=event_id,
                                order_id=order_id,
                            ),
                        )

                    

        except psycopg.Error as e:
            app.logger.error(f"Stock subtraction failed: {e}")
            return utils.Failure("Database error during stock subtraction")

    # Write failure on outbox

    return utils.Failure("Too many concurrent updates, please retry")

@app.post('/item/create/<price>')
def create_item(price: int):
    key = str(uuid.uuid4())
    app.logger.debug(f"Item: {key} created")
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                repo = StockRepository(cur)
                repo.insert_log_event(
                    item_id=key,
                    event_type='ITEM_CREATED',
                    payload={"price": int(price), "stock": 0},
                    version=1,
                )
                repo.insert_item_snapshot(key, 0, int(price), 1)
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({'item_id': key})


@app.get('/items')
def get_items():
    with db_pool.connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            repo = StockRepository(cur)
            items = repo.list_item_snapshots()
            return jsonify(items)
    

@app.post('/batch_init/<n>/<starting_stock>/<item_price>')
def batch_init_users(n: int, starting_stock: int, item_price: int):
    n = int(n)
    starting_stock = int(starting_stock)
    item_price = int(item_price)
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                repo = StockRepository(cur)
                for i in range(n):
                    item_id = str(i)
                    repo.insert_log_event(
                        item_id=item_id,
                        event_type='ITEM_CREATED',
                        payload={"stock": starting_stock, "price": item_price},
                        version=1,
                    )
                    repo.insert_item_snapshot(item_id, starting_stock, item_price, 1)
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for stock successful"})


@app.get('/find/<item_id>')
def find_item(item_id: str):
    item_entry: StockValue = get_item_from_db(item_id) # type: ignore
    return jsonify(
        {
            "stock": item_entry.stock,
            "price": item_entry.price
        }
    )

@app.post('/add/<item_id>/<amount>')
def add_stock(item_id: str, amount: int):
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    repo = StockRepository(cur)
                    row = repo.get_item_snapshot(item_id)
                    if row is None:
                        return abort(400, f"Item: {item_id} not found!")

                    current_version = int(row['version'])
                    new_stock = int(row['stock']) + int(amount)
                    new_version = current_version + 1

                    repo.insert_log_event(
                        item_id=item_id,
                        event_type='STOCK_ADDED',
                        payload={"amount": int(amount)},
                        version=new_version,
                    )

                    if not repo.update_snapshot_versioned(item_id, new_stock, new_version, current_version):
                        conn.rollback()
                        app.logger.warning(f"Version conflict for item {item_id}, retry {attempt + 1}/{MAX_RETRIES}")
                        continue

                    return Response(f"Item: {item_id} stock updated to: {new_stock}", status=200)

        except psycopg.Error:
            return abort(400, DB_ERROR_STR)

    return abort(409, "Too many concurrent updates, please retry")


@app.post('/subtract/<item_id>/<amount>')
def remove_stock(item_id: str, amount: int):
    MAX_RETRIES = 3
    for attempt in range(MAX_RETRIES):
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    repo = StockRepository(cur)
                    row = repo.get_item_snapshot(item_id)
                    if row is None:
                        return abort(400, f"Item: {item_id} not found!")

                    current_stock = int(row['stock'])
                    current_version = int(row['version'])
                    new_stock = current_stock - int(amount)

                    if new_stock < 0:
                        return abort(400, f"Item: {item_id} stock cannot get reduced below zero!")

                    new_version = current_version + 1

                    repo.insert_log_event(
                        item_id=item_id,
                        event_type='STOCK_SUBTRACTED',
                        payload={"amount": int(amount)},
                        version=new_version,
                    )

                    if not repo.update_snapshot_versioned(item_id, new_stock, new_version, current_version):
                        conn.rollback()
                        app.logger.warning(f"Version conflict for item {item_id}, retry {attempt + 1}/{MAX_RETRIES}")
                        continue

                    app.logger.debug(f"Item: {item_id} stock updated to: {new_stock}")
                    return Response(f"Item: {item_id} stock updated to: {new_stock}", status=200)

        except psycopg.Error:
            return abort(400, DB_ERROR_STR)

    return abort(409, "Too many concurrent updates, please retry")




if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
