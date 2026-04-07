import asyncio
import logging
import os

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import CommitFailedError
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from db_repository import PaymentRepository
from services import utils


logging.basicConfig(level=logging.INFO)
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
    except Exception as exc:
        logger.warning("Payment inbox cleanup failed: %s", exc)


async def _handle_checkout(
    repo: PaymentRepository, event: utils.BaseEvent, correlation_id: str
) -> dict:
    if event.event_type != utils.Commands.START_PAYMENT:
        return {"status": "failure", "reason": "unsupported_event_type"}

    amount = int(event.payload.amount)
    user_id = event.payload.user_id
    order_id = event.payload.order_id

    for _ in range(3):
        row = await repo.get_account(user_id)
        if row is None:
            return {"status": "failure", "reason": "user_not_found"}

        current_credit = int(row["credit"])
        current_version = int(row["version"])

        if current_credit < amount:
            return {"status": "failure", "reason": "insufficient_credit"}

        new_credit = current_credit - amount
        new_version = current_version + 1
        updated = await repo.update_account_versioned(
            user_id=user_id,
            credit=new_credit,
            new_version=new_version,
            current_version=current_version,
        )
        if updated:
            logger.info("Payment succeeded order_id=%s user_id=%s", order_id, user_id)
            return {
                "status": "success",
                "remaining_credit": new_credit,
                "amount": amount,
            }

        await asyncio.sleep(0)

    return {"status": "failure", "reason": "version_conflict"}


async def _emit_payment_result(
    repo: PaymentRepository,
    event: utils.BaseEvent,
    correlation_id: str,
    result: dict,
) -> None:
    if result.get("status") == "success":
        payload = utils.PaymentProcessedPayload(
            order_id=event.payload.order_id,
            user_id=event.payload.user_id,
            amount=int(result.get("amount", event.payload.amount)),
            remaining_credit=int(result.get("remaining_credit", 0)),
        )
        event_type = utils.PaymentIntegrationEvent.PAYMENT_SUCCEEDED
    else:
        payload = utils.PaymentFailedPayload(
            order_id=event.payload.order_id,
            user_id=event.payload.user_id,
            amount=event.payload.amount,
            reason=str(result.get("reason", "payment_failed")),
        )
        event_type = utils.PaymentIntegrationEvent.PAYMENT_FAILED

    await repo.insert_outbox_message(
        topic="order.request",
        payload=utils.BaseEvent(
            id=f"payment-result:{event.id}",
            event_type=event_type,
            payload=payload,
            order_id=event.payload.order_id,
            correlation_id=correlation_id,
            saga_id=correlation_id,
        ),
        message_key=correlation_id,
        correlation_id=correlation_id,
    )


async def _process_payment_message(message, pool: AsyncConnectionPool) -> None:
    result = utils.decode_and_type_event(message)
    if isinstance(result, utils.Failure):
        logger.error("Payment decode failed: %s", result.error)
        return

    event = result.value
    correlation_id = utils.event_correlation_id(event)

    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                repo = PaymentRepository(cur)
                if await repo.inbox_event_exists(event.id):
                    inbox_result = await repo.get_inbox_event_result(event.id) or {
                        "status": "failure",
                        "reason": "duplicate_event",
                    }
                    await _emit_payment_result(repo, event, correlation_id, inbox_result)
                    logger.info("Payment inbox dedupe hit event_id=%s", event.id)
                    await conn.commit()
                    return

                raw_payload = message.value.decode() if isinstance(message.value, bytes) else str(message.value)
                await repo.insert_inbox_event(
                    event_id=event.id,
                    topic=message.topic,
                    partition=message.partition,
                    kafka_offset=message.offset,
                    correlation_id=correlation_id,
                    payload=event,
                    payload_hash=utils.payload_hash(raw_payload),
                )

                checkout_result = await _handle_checkout(repo, event, correlation_id)
                await repo.set_inbox_event_result(
                    event_id=event.id,
                    status="PROCESSED",
                    result=checkout_result,
                    error=checkout_result.get("reason") if checkout_result.get("status") == "failure" else None,
                )
                await _emit_payment_result(repo, event, correlation_id, checkout_result)
                await conn.commit()
    except Exception as exc:
        logger.exception(
            "Payment consumer error event_id=%s correlation_id=%s: %s",
            event.id, correlation_id, exc,
        )


async def _cleanup_loop(pool: AsyncConnectionPool) -> None:
    while True:
        await asyncio.sleep(30)
        await _cleanup_inbox(pool)


async def main() -> None:
    pool_size = int(os.getenv("DB_POOL_MAX_SIZE", "4"))
    logger.info("Payment consumer starting pool_size=%s", pool_size)

    async with AsyncConnectionPool(conninfo=_conninfo(), min_size=1, max_size=pool_size) as pool:
        consumer = AIOKafkaConsumer(
            "payment.request",
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="payment-consumer-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            fetch_max_wait_ms=10,
            max_poll_records=500,
            max_poll_interval_ms=300000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,
        )
        await consumer.start()
        logger.info("Payment consumer started")

        cleanup_task = asyncio.create_task(_cleanup_loop(pool))
        try:
            while True:
                records = await consumer.getmany(timeout_ms=100, max_records=pool_size)
                if not records:
                    continue
                tasks = [
                    asyncio.create_task(_process_payment_message(msg, pool))
                    for msgs in records.values()
                    for msg in msgs
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                try:
                    await consumer.commit()
                except CommitFailedError:
                    logger.warning("Payment consumer commit failed after rebalance, messages will be replayed")
                for r in results:
                    if isinstance(r, Exception):
                        logger.exception("Payment consumer task failed: %s", r)
        finally:
            cleanup_task.cancel()
            await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
