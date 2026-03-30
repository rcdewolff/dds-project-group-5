import json as std_json
import logging
import os
import time
import uuid
from typing import Any

import psycopg  # type: ignore
from msgspec import json
from psycopg.rows import dict_row  # type: ignore
from psycopg_pool import ConnectionPool  # type: ignore

from saga_workflow import apply_saga_event, start_checkout
from services import kafka_client, utils


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CHECKOUT_COMMANDS_TOPIC = os.getenv("CHECKOUT_COMMANDS_TOPIC", "checkout-commands")
CHECKOUT_RESULTS_TOPIC = os.getenv("CHECKOUT_RESULTS_TOPIC", "checkout-results")


def init_db_pool() -> ConnectionPool:
    conninfo = (
        f"host={os.environ['POSTGRES_HOST']} "
        f"port={os.environ['POSTGRES_PORT']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']} "
        f"dbname={os.environ['POSTGRES_DB']}"
    )
    return ConnectionPool(
        conninfo=conninfo,
        min_size=1,
        max_size=5,
        reconnect_timeout=30,
        kwargs={"connect_timeout": 10},
    )


def _cleanup_inbox(db_pool: ConnectionPool) -> None:
    """Delete old processed inbox rows to prevent table bloat."""
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM inbox
                    WHERE status = 'PROCESSED'
                    AND processed_at < now() - interval '1 minutes'
                    """
                )
            conn.commit()
            logger.debug("Inbox cleanup completed")
    except Exception as exc:
        logger.warning("Inbox cleanup failed: %s", exc)


def _mark_inbox_received(cur, message, event: utils.BaseEvent) -> bool:
    raw_payload = message.value.decode() if isinstance(message.value, bytes) else str(message.value)
    cur.execute(
        """
        INSERT INTO inbox (
            id,
            event_id,
            topic,
            partition,
            kafka_offset,
            correlation_id,
            status,
            payload_hash,
            payload
        )
        VALUES (%s, %s, %s, %s, %s, %s, 'RECEIVED', %s, %s::jsonb)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING event_id
        """,
        (
            str(uuid.uuid4()),
            event.id,
            message.topic,
            message.partition,
            message.offset,
            utils.event_correlation_id(event),
            utils.payload_hash(raw_payload),
            raw_payload,
        ),
    )
    return cur.fetchone() is not None


def _mark_inbox_processed(cur, event: utils.BaseEvent, status: str, error: str | None = None):
    cur.execute(
        """
        UPDATE inbox
        SET status = %s,
            error = %s,
            processed_at = now()
        WHERE event_id = %s
        """,
        (status, error, event.id),
    )


def _decode_checkout_command(message) -> dict[str, Any] | None:
    raw_value = message.value
    if isinstance(raw_value, bytes):
        payload = std_json.loads(raw_value.decode("utf-8"))
    elif isinstance(raw_value, str):
        payload = std_json.loads(raw_value)
    else:
        payload = raw_value

    if not isinstance(payload, dict):
        return None

    if "payload" in payload and isinstance(payload["payload"], dict):
        body = payload["payload"]
        return {
            "id": payload.get("id") or body.get("id"),
            "event_type": body.get("event_type") or payload.get("event_type"),
            "correlation_id": body.get("correlation_id") or payload.get("correlation_id"),
            "order_id": body.get("order_id") or payload.get("order_id"),
            "timestamp": body.get("timestamp") or payload.get("timestamp"),
        }

    return {
        "id": payload.get("id"),
        "event_type": payload.get("event_type"),
        "correlation_id": payload.get("correlation_id"),
        "order_id": payload.get("order_id"),
        "timestamp": payload.get("timestamp"),
    }


def _get_order_snapshot(cur, order_id: str) -> tuple[str, list[tuple[str, int]]] | None:
    cur.execute(
        """
        SELECT user_id, items
        FROM orders
        WHERE order_id = %s
        """,
        (order_id,),
    )
    row = cur.fetchone()
    if row is None:
        return None

    raw_items = row.get("items") or []
    items = [(item["item_id"], int(item["quantity"])) for item in raw_items]
    return row["user_id"], items


def _emit_checkout_terminal_result(
    cur,
    *,
    correlation_id: str,
    order_id: str,
    status: str,
    results: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    event_payload = {
        "event_type": "checkout.result",
        "correlation_id": correlation_id,
        "order_id": order_id,
        "status": status,
        "results": results or {},
        "error": error,
        "timestamp": time.time(),
    }
    event = utils.BaseEvent.create(
        event_type="checkout.result",
        payload=event_payload,
        order_id=order_id,
        correlation_id=correlation_id,
        id=f"checkout-result:{correlation_id}",
    )
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
        ON CONFLICT (event_id) DO NOTHING
        """,
        (
            str(uuid.uuid4()),
            event.id,
            CHECKOUT_RESULTS_TOPIC,
            correlation_id,
            correlation_id,
            payload_text,
        ),
    )


def _handle_checkout_command(cur, command: dict[str, Any]) -> dict[str, Any]:
    event_type = command.get("event_type")
    correlation_id = command.get("correlation_id")
    order_id = command.get("order_id")

    if event_type != "checkout.command":
        return {"handled": False, "reason": "unsupported_command_event"}

    if not correlation_id or not order_id:
        return {"handled": False, "reason": "invalid_command"}

    snapshot = _get_order_snapshot(cur, order_id)
    if snapshot is None:
        _emit_checkout_terminal_result(
            cur,
            correlation_id=correlation_id,
            order_id=order_id,
            status="failed",
            error="order_not_found",
            results={"reason": "order_not_found"},
        )
        return {"handled": True, "status": "failed", "reason": "order_not_found"}

    user_id, items = snapshot
    if not items:
        _emit_checkout_terminal_result(
            cur,
            correlation_id=correlation_id,
            order_id=order_id,
            status="failed",
            error="order_has_no_items",
            results={"reason": "order_has_no_items"},
        )
        return {"handled": True, "status": "failed", "reason": "order_has_no_items"}

    started_correlation_id, created = start_checkout(
        cur,
        order_id=order_id,
        user_id=user_id,
        items=items,
        correlation_id=correlation_id,
    )

    if started_correlation_id != correlation_id:
        _emit_checkout_terminal_result(
            cur,
            correlation_id=correlation_id,
            order_id=order_id,
            status="failed",
            error="checkout_already_in_progress",
            results={"active_correlation_id": started_correlation_id},
        )
        return {"handled": True, "status": "failed", "reason": "checkout_already_in_progress"}

    return {
        "handled": True,
        "status": "running",
        "created": created,
        "correlation_id": started_correlation_id,
        "order_id": order_id,
    }


def main():
    logger.info("Starting order consumer with durable inbox processing")
    service_name = "order"
    client = kafka_client.Client(service_name, [f"{service_name}.request", CHECKOUT_COMMANDS_TOPIC])
    consumer = client.consumer
    db_pool = init_db_pool()
    message_count = 0

    for message in consumer:
        message_count += 1
        if message_count % 100 == 0:
            _cleanup_inbox(db_pool)

        if message.topic == CHECKOUT_COMMANDS_TOPIC:
            try:
                command = _decode_checkout_command(message)
            except Exception as exc:
                logger.error("Checkout command decode failed topic=%s error=%s", message.topic, exc)
                continue

            if not command:
                logger.error("Checkout command dropped: invalid payload")
                continue

            correlation_id = command.get("correlation_id")
            order_id = command.get("order_id")
            logger.info(
                "Checkout command received correlation_id=%s order_id=%s topic=%s partition=%s offset=%s",
                correlation_id,
                order_id,
                message.topic,
                message.partition,
                message.offset,
            )

            try:
                with db_pool.connection() as conn:
                    with conn.cursor(row_factory=dict_row) as cur:
                        outcome = _handle_checkout_command(cur, command)
                        conn.commit()
                logger.info(
                    "Checkout command handled correlation_id=%s order_id=%s outcome=%s",
                    correlation_id,
                    order_id,
                    std_json.dumps(outcome),
                )
            except psycopg.Error as exc:
                logger.exception(
                    "Checkout command DB failure correlation_id=%s order_id=%s error=%s",
                    correlation_id,
                    order_id,
                    exc,
                )
                if correlation_id and order_id:
                    try:
                        with db_pool.connection() as conn:
                            with conn.cursor(row_factory=dict_row) as cur:
                                _emit_checkout_terminal_result(
                                    cur,
                                    correlation_id=correlation_id,
                                    order_id=order_id,
                                    status="failed",
                                    error="internal_db_error",
                                    results={"reason": "internal_db_error"},
                                )
                                conn.commit()
                    except Exception:
                        logger.exception(
                            "Failed to emit checkout terminal failure for correlation_id=%s",
                            correlation_id,
                        )
            continue

        result = utils.decode_and_type_event(message)
        if isinstance(result, utils.Failure):
            logger.error("Kafka message dropped: %s", result.error)
            continue

        event = result.value
        correlation_id = utils.event_correlation_id(event)
        logger.info(
            "Order consumer received event_id=%s correlation_id=%s topic=%s partition=%s offset=%s",
            event.id,
            correlation_id,
            message.topic,
            message.partition,
            message.offset,
        )

        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    inbox_inserted = _mark_inbox_received(cur, message, event)
                    if not inbox_inserted:
                        logger.info(
                            "Order inbox dedupe hit event_id=%s correlation_id=%s",
                            event.id,
                            correlation_id,
                        )
                        conn.commit()
                        continue
                    outcome = apply_saga_event(cur, event)
                    _mark_inbox_processed(cur, event, status="PROCESSED")
                    conn.commit()
                    logger.info(
                        "Order event handled event_id=%s correlation_id=%s outcome=%s",
                        event.id,
                        correlation_id,
                        std_json.dumps(outcome),
                    )
        except psycopg.Error as exc:
            logger.exception(
                "Order consumer DB failure event_id=%s correlation_id=%s error=%s",
                event.id,
                correlation_id,
                exc,
            )
            try:
                with db_pool.connection() as conn:
                    with conn.cursor() as cur:
                        _mark_inbox_processed(cur, event, status="FAILED", error=str(exc))
                        conn.commit()
            except Exception:
                logger.exception("Failed to update inbox error state for event_id=%s", event.id)


if __name__ == "__main__":
    main()
