import json as std_json
import logging
import os
import uuid

import psycopg  # type: ignore
from psycopg.rows import dict_row  # type: ignore
from psycopg_pool import ConnectionPool  # type: ignore

from saga_workflow import apply_saga_event
from services import kafka_client, utils


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


def _inbox_exists(cur, event_id: str) -> bool:
    cur.execute(
        "SELECT event_id FROM inbox WHERE event_id = %s AND status = 'PROCESSED'",
        (event_id,),
    )
    return cur.fetchone() is not None


def _mark_inbox_received(cur, message, event: utils.BaseEvent):
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


def main():
    logger.info("Starting order consumer with durable inbox processing")
    service_name = "order"
    client = kafka_client.Client(service_name, [f"{service_name}.request"])
    consumer = client.consumer
    db_pool = init_db_pool()

    for message in consumer:
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
                    if _inbox_exists(cur, event.id):
                        logger.info(
                            "Order inbox dedupe hit event_id=%s correlation_id=%s",
                            event.id,
                            correlation_id,
                        )
                        conn.commit()
                        continue

                    _mark_inbox_received(cur, message, event)
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