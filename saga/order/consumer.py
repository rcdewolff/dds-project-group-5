import asyncio
import json as std_json
import logging
import os
import uuid
from typing import Any

from aiokafka import AIOKafkaConsumer
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from saga_workflow import apply_saga_event, start_checkout
from services import utils


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CHECKOUT_COMMANDS_TOPIC = os.getenv("CHECKOUT_COMMANDS_TOPIC", "checkout-commands")
CHECKOUT_RESULTS_TOPIC = os.getenv("CHECKOUT_RESULTS_TOPIC", "checkout-results")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def _conninfo() -> str:
    return (
        f"host={os.environ['POSTGRES_HOST']} "
        f"port={os.environ['POSTGRES_PORT']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']} "
        f"dbname={os.environ['POSTGRES_DB']}"
    )


async def _cleanup_inbox(pool: AsyncConnectionPool) -> None:
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    DELETE FROM inbox
                    WHERE status = 'PROCESSED'
                    AND processed_at < now() - interval '1 minutes'
                    """
                )
            await conn.commit()
            logger.debug("Inbox cleanup completed")
    except Exception as exc:
        logger.warning("Inbox cleanup failed: %s", exc)


async def _mark_inbox_received(cur, message, event: utils.BaseEvent) -> bool:
    raw_payload = message.value.decode() if isinstance(message.value, bytes) else str(message.value)
    await cur.execute(
        """
        INSERT INTO inbox (
            id, event_id, topic, partition, kafka_offset,
            correlation_id, status, payload_hash, payload
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
    return await cur.fetchone() is not None


async def _mark_inbox_processed(cur, event: utils.BaseEvent, status: str, error: str | None = None) -> None:
    await cur.execute(
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


async def _emit_checkout_terminal_result(
    cur,
    *,
    correlation_id: str,
    order_id: str,
    status: str,
    results: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    from msgspec import json
    import time

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

    await cur.execute(
        """
        INSERT INTO outbox (
            id, event_id, topic, message_key, correlation_id,
            payload, status, publish_attempts, created_at
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


async def _process_checkout_command(message, pool: AsyncConnectionPool) -> None:
    try:
        command = _decode_checkout_command(message)
    except Exception as exc:
        logger.error("Checkout command decode failed topic=%s error=%s", message.topic, exc)
        return

    if not command:
        logger.error("Checkout command dropped: invalid payload")
        return

    correlation_id = command.get("correlation_id")
    order_id = command.get("order_id")

    if command.get("event_type") != "checkout.command" or not correlation_id or not order_id:
        logger.error("Checkout command invalid correlation_id=%s order_id=%s", correlation_id, order_id)
        return

    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                # Fetch order snapshot
                await cur.execute(
                    "SELECT user_id, items FROM orders WHERE order_id = %s",
                    (order_id,),
                )
                row = await cur.fetchone()

                if row is None:
                    await _emit_checkout_terminal_result(
                        cur,
                        correlation_id=correlation_id,
                        order_id=order_id,
                        status="failed",
                        error="order_not_found",
                        results={"reason": "order_not_found"},
                    )
                    await conn.commit()
                    return

                raw_items = row.get("items") or []
                items = [(item["item_id"], int(item["quantity"])) for item in raw_items]
                user_id = row["user_id"]

                if not items:
                    await _emit_checkout_terminal_result(
                        cur,
                        correlation_id=correlation_id,
                        order_id=order_id,
                        status="failed",
                        error="order_has_no_items",
                        results={"reason": "order_has_no_items"},
                    )
                    await conn.commit()
                    return

                started_correlation_id, created = await start_checkout(
                    cur,
                    order_id=order_id,
                    user_id=user_id,
                    items=items,
                    correlation_id=correlation_id,
                )

                if started_correlation_id != correlation_id:
                    await _emit_checkout_terminal_result(
                        cur,
                        correlation_id=correlation_id,
                        order_id=order_id,
                        status="failed",
                        error="checkout_already_in_progress",
                        results={"active_correlation_id": started_correlation_id},
                    )

                await conn.commit()
                logger.info(
                    "Checkout command handled correlation_id=%s order_id=%s created=%s",
                    correlation_id, order_id, created,
                )
    except Exception as exc:
        logger.exception(
            "Checkout command DB failure correlation_id=%s order_id=%s error=%s",
            correlation_id, order_id, exc,
        )


async def _process_order_event(message, pool: AsyncConnectionPool) -> None:
    result = utils.decode_and_type_event(message)
    if isinstance(result, utils.Failure):
        logger.error("Kafka message dropped: %s", result.error)
        return

    event = result.value
    correlation_id = utils.event_correlation_id(event)

    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                inbox_inserted = await _mark_inbox_received(cur, message, event)
                if not inbox_inserted:
                    logger.info(
                        "Order inbox dedupe hit event_id=%s correlation_id=%s",
                        event.id, correlation_id,
                    )
                    await conn.commit()
                    return

                outcome = await apply_saga_event(cur, event)
                await _mark_inbox_processed(cur, event, status="PROCESSED")
                await conn.commit()
                logger.info(
                    "Order event handled event_id=%s correlation_id=%s outcome=%s",
                    event.id, correlation_id, std_json.dumps(outcome),
                )
    except Exception as exc:
        logger.exception(
            "Order consumer DB failure event_id=%s correlation_id=%s error=%s",
            event.id, correlation_id, exc,
        )
        try:
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await _mark_inbox_processed(cur, event, status="FAILED", error=str(exc))
                    await conn.commit()
        except Exception:
            logger.exception("Failed to update inbox error state for event_id=%s", event.id)


async def _process_message(message, pool: AsyncConnectionPool) -> None:
    if message.topic == CHECKOUT_COMMANDS_TOPIC:
        await _process_checkout_command(message, pool)
    else:
        await _process_order_event(message, pool)


async def _cleanup_loop(pool: AsyncConnectionPool) -> None:
    while True:
        await asyncio.sleep(30)
        await _cleanup_inbox(pool)


async def main() -> None:
    pool_size = int(os.getenv("DB_POOL_MAX_SIZE", "4"))
    logger.info("Order consumer starting pool_size=%s", pool_size)

    async with AsyncConnectionPool(conninfo=_conninfo(), min_size=1, max_size=pool_size) as pool:
        consumer = AIOKafkaConsumer(
            "order.request",
            CHECKOUT_COMMANDS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="order-consumer-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            fetch_max_wait_ms=10,
            max_poll_records=500,
        )
        await consumer.start()
        logger.info("Order consumer started topics=order.request,%s", CHECKOUT_COMMANDS_TOPIC)

        cleanup_task = asyncio.create_task(_cleanup_loop(pool))
        try:
            while True:
                records = await consumer.getmany(timeout_ms=100, max_records=pool_size * 2)
                if not records:
                    continue
                tasks = [
                    asyncio.create_task(_process_message(msg, pool))
                    for msgs in records.values()
                    for msg in msgs
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                await consumer.commit()
                for r in results:
                    if isinstance(r, Exception):
                        logger.exception("Order consumer task failed: %s", r)
        finally:
            cleanup_task.cancel()
            await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
