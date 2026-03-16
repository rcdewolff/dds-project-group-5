# app.py (relevant parts)
import threading
from typing import Dict, Any
from flask import Flask
from services import utils

app = Flask(__name__)

# --- Pending saga registry ---
# Maps correlation_id -> {"event": threading.Event, "result": Any}
# Single instance only — not safe for horizontal scaling
_pending_sagas: Dict[str, dict] = {}
_pending_sagas_lock = threading.Lock()

SAGA_TIMEOUT_SECONDS = 30.0


# --- HTTP Handler ---

@app.post('/checkout/<order_id>')
def checkout(order_id: str):
    order_value: OrderValue = get_order_from_db(order_id)
    items_list = [(item_id, qty) for item_id, qty in order_value.items]

    payload = utils.OrderCheckoutPayload(order_id=order_id, items=items_list)
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


# --- Kafka Consumer ---

def consume_messages(consumer):
    """
    Runs in a background thread (started in post_fork).
    Listens for saga outcome events and unblocks the waiting HTTP handler.
    """
    for message in consumer:
        event = message.value
        handle_event(event)


def handle_event(event: dict):
    event_type = event.get("event_type")
    correlation_id = event.get("correlation_id")

    if event_type == "CHECKOUT_COMPLETED":
        _resolve_saga(correlation_id, {"status": "success"})

    elif event_type == "CHECKOUT_FAILED":
        _resolve_saga(correlation_id, {
            "status": "failed",
            "reason": event.get("payload", {}).get("reason", "Unknown failure")
        })


def _resolve_saga(correlation_id: str, result: dict):
    """Called by the Kafka consumer to unblock the waiting HTTP handler."""
    with _pending_sagas_lock:
        entry = _pending_sagas.get(correlation_id)

    if entry is None:
        # Saga already timed out and was cleaned up — discard the result
        return

    entry["result"] = result
    entry["event"].set()  # unblocks wait_event.wait() in the HTTP handler