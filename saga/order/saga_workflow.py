import json as std_json
import uuid
from typing import Any

from msgspec import json

from services import utils


FINAL_STATUSES = {"completed", "failed", "compensated"}
ACTIVE_STATUSES = {"pending", "running", "compensating"}


def _load_results(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("results")
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return std_json.loads(value)
        except std_json.JSONDecodeError:
            return {}
    return {}


def _insert_outbox_event(cur, topic: str, event: utils.BaseEvent[Any]) -> None:
    payload_text = json.encode(event).decode()
    cur.execute(
        """
        INSERT INTO outbox (
            id,
            event_id,
            topic,
            message_key,
            correlation_id,
            payload,
            status,
            publish_attempts,
            created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s::jsonb, 'PENDING', 0, now())
        """,
        (
            str(uuid.uuid4()),
            event.id,
            topic,
            utils.event_correlation_id(event),
            utils.event_correlation_id(event),
            payload_text,
        ),
    )


def _upsert_saga(cur, saga_id: str, order_id: str, status: str, step: str, results: dict[str, Any]) -> None:
    cur.execute(
        """
        INSERT INTO sagas (order_id, id, status, step, results)
        VALUES (%s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (id) DO UPDATE
        SET status = EXCLUDED.status,
            step = EXCLUDED.step,
            results = EXCLUDED.results
        """,
        (order_id, saga_id, status, step, std_json.dumps(results)),
    )


def start_checkout(cur, order_id: str, user_id: str, items: list[tuple[str, int]]) -> tuple[str, bool]:
    cur.execute(
        """
        SELECT id, status
        FROM sagas
        WHERE order_id = %s
        ORDER BY created_at DESC NULLS LAST, id DESC
        LIMIT 1
        """,
        (order_id,),
    )
    existing = cur.fetchone()
    if existing and existing["status"] in ACTIVE_STATUSES:
        return existing["id"], False

    correlation_id = str(uuid.uuid4())
    topic, payload = utils.build_reserve_stock_command(correlation_id, order_id, items)
    event = json.decode(payload, type=utils.BaseEvent[dict])

    results = {
        "order_snapshot": {
            "order_id": order_id,
            "user_id": user_id,
            "items": [[item_id, qty] for item_id, qty in items],
        },
        "transitions": [
            {
                "event_type": utils.OrderInternalEvent.CHECKOUT_INITIATED.value,
                "status": "success",
            }
        ],
    }

    _upsert_saga(
        cur,
        saga_id=correlation_id,
        order_id=order_id,
        status="running",
        step="STOCK_RESERVATION_PHASE",
        results=results,
    )
    _insert_outbox_event(cur, topic, event)
    return correlation_id, True


def apply_saga_event(cur, event: utils.BaseEvent[Any]) -> dict[str, Any]:
    correlation_id = utils.event_correlation_id(event)
    cur.execute(
        """
        SELECT id, order_id, status, step, results
        FROM sagas
        WHERE id = %s
        """,
        (correlation_id,),
    )
    saga = cur.fetchone()
    if saga is None:
        return {"handled": False, "reason": "saga_not_found"}

    status = saga["status"]
    results = _load_results(saga)
    results.setdefault("transitions", [])

    if status in FINAL_STATUSES:
        return {"handled": False, "reason": "saga_already_final"}

    if event.event_type == utils.StockIntegrationEvent.STOCK_ALLOCATED:
        amount = int(getattr(event.payload, "amount", 0))
        snapshot = results.get("order_snapshot", {})
        order_id = snapshot.get("order_id", saga["order_id"])
        user_id = snapshot.get("user_id", "")

        topic, payload = utils.build_start_payment_command(
            correlation_id=correlation_id,
            order_id=order_id,
            user_id=user_id,
            amount=amount,
        )
        payment_event = json.decode(payload, type=utils.BaseEvent[dict])

        results["stock"] = {"status": "success", "amount": amount}
        results["transitions"].append({"event_type": event.event_type, "status": "success"})
        _upsert_saga(
            cur,
            saga_id=correlation_id,
            order_id=saga["order_id"],
            status="running",
            step="PAYMENT_PHASE",
            results=results,
        )
        _insert_outbox_event(cur, topic, payment_event)
        return {"handled": True, "status": "running"}

    if event.event_type == utils.StockIntegrationEvent.STOCK_UNAVAILABLE:
        results["stock"] = {"status": "failed", "reason": "stock_unavailable"}
        results["transitions"].append({"event_type": event.event_type, "status": "failed"})
        _upsert_saga(
            cur,
            saga_id=correlation_id,
            order_id=saga["order_id"],
            status="failed",
            step=saga["step"],
            results=results,
        )
        return {"handled": True, "status": "failed"}

    if event.event_type == utils.PaymentIntegrationEvent.PAYMENT_SUCCEEDED:
        results["payment"] = {"status": "success"}
        results["transitions"].append({"event_type": event.event_type, "status": "success"})
        cur.execute(
            "UPDATE orders SET paid = TRUE WHERE order_id = %s",
            (saga["order_id"],),
        )
        _upsert_saga(
            cur,
            saga_id=correlation_id,
            order_id=saga["order_id"],
            status="completed",
            step=saga["step"],
            results=results,
        )
        return {"handled": True, "status": "completed"}

    if event.event_type == utils.PaymentIntegrationEvent.PAYMENT_FAILED:
        snapshot = results.get("order_snapshot", {})
        raw_items = snapshot.get("items", [])
        items: list[tuple[str, int]] = [(item[0], int(item[1])) for item in raw_items]

        topic, payload = utils.build_free_stock_command(
            correlation_id=correlation_id,
            order_id=saga["order_id"],
            items=items,
        )
        rollback_event = json.decode(payload, type=utils.BaseEvent[dict])
        reason = getattr(event.payload, "reason", "payment_failed")

        results["payment"] = {"status": "failed", "reason": reason}
        results["transitions"].append({"event_type": event.event_type, "status": "failed"})
        _upsert_saga(
            cur,
            saga_id=correlation_id,
            order_id=saga["order_id"],
            status="compensating",
            step="STOCK_RESERVATION_PHASE",
            results=results,
        )
        _insert_outbox_event(cur, topic, rollback_event)
        return {"handled": True, "status": "compensating"}

    if event.event_type == utils.StockIntegrationEvent.STOCK_FREED:
        results["compensation"] = {"status": "success"}
        results["transitions"].append({"event_type": event.event_type, "status": "compensated"})
        _upsert_saga(
            cur,
            saga_id=correlation_id,
            order_id=saga["order_id"],
            status="compensated",
            step=saga["step"],
            results=results,
        )
        return {"handled": True, "status": "compensated"}

    return {"handled": False, "reason": "unsupported_event_type"}
