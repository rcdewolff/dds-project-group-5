import asyncio
import logging
import os
import time
from typing import Any

from aiokafka import AIOKafkaProducer
from msgspec import json
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from services import utils


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def _conninfo() -> str:
    return (
        f"host={os.environ['POSTGRES_HOST']} "
        f"port={os.environ['POSTGRES_PORT']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']} "
        f"dbname={os.environ['POSTGRES_DB']}"
    )


def _parse_poll_interval(raw_value: str) -> float:
    value = raw_value.strip().lower().rstrip("s")
    interval = float(value)
    if interval <= 0:
        raise ValueError("OUTBOX_POLL_INTERVAL must be > 0")
    return interval


def _payload_to_base_event(payload: Any) -> utils.BaseEvent:
    if isinstance(payload, bytes):
        return json.decode(payload, type=utils.BaseEvent[dict])
    if isinstance(payload, str):
        return json.decode(payload.encode(), type=utils.BaseEvent[dict])
    if isinstance(payload, dict):
        correlation_id = payload.get("correlation_id") or payload.get("saga_id")
        return utils.BaseEvent(
            id=payload["id"],
            event_type=payload["event_type"],
            order_id=payload["order_id"],
            correlation_id=correlation_id,
            saga_id=payload.get("saga_id", correlation_id),
            payload=payload["payload"],
            timestamp=payload.get("timestamp", time.time()),
        )
    raise ValueError(f"Unsupported outbox payload type: {type(payload)}")


async def _cleanup_published(pool: AsyncConnectionPool) -> None:
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    DELETE FROM outbox
                    WHERE status = 'PUBLISHED'
                    AND published_at < now() - interval '1 minutes'
                    """
                )
            await conn.commit()
    except Exception as exc:
        logger.warning("Outbox cleanup failed: %s", exc)


async def _relay_oldest_unsent(
    pool: AsyncConnectionPool,
    producer: AIOKafkaProducer,
    fetch_batch_size: int,
) -> bool:
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT id, topic, payload, message_key, event_id, correlation_id, publish_attempts
                    FROM outbox
                    WHERE status = 'PENDING'
                    ORDER BY created_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    (fetch_batch_size,),
                )
                rows = await cur.fetchall()
                if not rows:
                    return False

                # Pipeline all sends, then await acks in bulk
                send_futures = []
                for row in rows:
                    try:
                        event = _payload_to_base_event(row["payload"])
                        message_key = str(row["message_key"]).encode("utf-8")
                        fut = await producer.send(row["topic"], key=message_key, value=json.encode(event))
                        send_futures.append((fut, row))
                    except Exception as row_exc:
                        await cur.execute(
                            "UPDATE outbox SET publish_attempts = publish_attempts + 1, last_error = %s WHERE id = %s",
                            (str(row_exc), row["id"]),
                        )
                        logger.exception(
                            "Outbox relay send failed row=%s: %s", row.get("id"), row_exc
                        )

                relayed_count = 0
                for fut, row in send_futures:
                    try:
                        await fut
                        await cur.execute(
                            """
                            UPDATE outbox
                            SET status = 'PUBLISHED',
                                publish_attempts = publish_attempts + 1,
                                published_at = now(),
                                last_error = NULL
                            WHERE id = %s
                            """,
                            (row["id"],),
                        )
                        relayed_count += 1
                    except Exception as row_exc:
                        await cur.execute(
                            "UPDATE outbox SET publish_attempts = publish_attempts + 1, last_error = %s WHERE id = %s",
                            (str(row_exc), row["id"]),
                        )
                        logger.exception(
                            "Outbox relay ack failed row=%s topic=%s: %s",
                            row.get("id"), row.get("topic"), row_exc,
                        )

        return relayed_count > 0
    except Exception as exc:
        logger.exception("Outbox relay error: %s", exc)
        return False


async def _relay_loop(
    pool: AsyncConnectionPool,
    producer: AIOKafkaProducer,
    poll_interval: float,
    fetch_batch_size: int,
    worker_id: int,
) -> None:
    cycle = 0
    while True:
        sent = await _relay_oldest_unsent(pool, producer, fetch_batch_size)
        if not sent:
            await asyncio.sleep(poll_interval)
        cycle += 1
        if cycle % 200 == worker_id:
            await _cleanup_published(pool)


async def main() -> None:
    poll_interval = _parse_poll_interval(os.getenv("OUTBOX_POLL_INTERVAL", "0.05s"))
    fetch_batch_size = int(os.getenv("OUTBOX_FETCH_BATCH_SIZE", "20"))
    num_workers = int(os.getenv("RELAY_WORKERS", "4"))
    logger.info(
        "Stock outbox relay starting poll_interval=%s batch=%s workers=%s",
        poll_interval, fetch_batch_size, num_workers,
    )

    async with AsyncConnectionPool(
        conninfo=_conninfo(), min_size=num_workers, max_size=num_workers + 2
    ) as pool:
        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        logger.info("Stock outbox relay started")

        try:
            await asyncio.gather(
                *[
                    _relay_loop(pool, producer, poll_interval, fetch_batch_size, i)
                    for i in range(num_workers)
                ]
            )
        finally:
            await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
