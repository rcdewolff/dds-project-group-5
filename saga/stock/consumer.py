import asyncio
import logging
import os
from collections import defaultdict

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import CommitFailedError
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from db_repository import StockRepository
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
        logger.warning("Stock inbox cleanup failed: %s", exc)


async def _reserve_stock(repo: StockRepository, event: utils.BaseEvent) -> dict:
    items = sorted(event.payload.items, key=lambda x: x[0])
    if not items:
        return {"status": "failure", "reason": "no_items"}

    for _ in range(3):
        item_ids = [item_id for item_id, _ in items]
        rows = await repo.get_inventory_items_for_ids(item_ids, include_price=True)

        missing = [item_id for item_id, _ in items if item_id not in rows]
        if missing:
            return {"status": "failure", "reason": "items_not_found"}

        unavailable = [item_id for item_id, qty in items if int(rows[item_id]["stock"]) < int(qty)]
        if unavailable:
            return {"status": "failure", "reason": "insufficient_stock"}

        total_cost = 0
        conflict = False
        for item_id, qty in items:
            row = rows[item_id]
            current_version = int(row["version"])
            new_stock = int(row["stock"]) - int(qty)
            new_version = current_version + 1
            if not await repo.update_inventory_item_versioned(item_id, new_stock, new_version, current_version):
                conflict = True
                break
            total_cost += int(row["price"]) * int(qty)

        if not conflict:
            return {"status": "success", "amount": total_cost}

        await asyncio.sleep(0)

    return {"status": "failure", "reason": "version_conflict"}


async def _free_stock(repo: StockRepository, event: utils.BaseEvent) -> dict:
    qty_by_item: dict = defaultdict(int)
    for item_id, qty in event.payload.items:
        qty_by_item[item_id] += int(qty)
    items = sorted(qty_by_item.items(), key=lambda x: x[0])

    for _ in range(3):
        item_ids = [item_id for item_id, _ in items]
        rows = await repo.get_inventory_items_for_ids(item_ids)

        conflict = False
        for item_id, qty in items:
            row = rows.get(item_id)
            if row is None:
                continue
            current_version = int(row["version"])
            new_stock = int(row["stock"]) + int(qty)
            new_version = current_version + 1
            if not await repo.update_inventory_item_versioned(item_id, new_stock, new_version, current_version):
                conflict = True
                break

        if not conflict:
            return {"status": "success"}

        await asyncio.sleep(0)

    return {"status": "failure", "reason": "version_conflict"}


async def _emit_stock_result(
    repo: StockRepository,
    event: utils.BaseEvent,
    correlation_id: str,
    result: dict,
) -> None:
    if event.event_type == utils.Commands.RESERVE_STOCK:
        if result.get("status") == "success":
            response_event = utils.build_stock_allocated_event(
                correlation_id=correlation_id,
                order_id=event.payload.order_id,
                amount=int(result.get("amount", 0)),
            )
        else:
            response_event = utils.build_stock_unavailable_event(
                correlation_id=correlation_id,
                order_id=event.payload.order_id,
            )
        response_event.id = f"stock-result:{event.id}"
        await repo.insert_outbox_message("order.request", response_event)

    if event.event_type == utils.Commands.FREE_STOCK and result.get("status") == "success":
        response_event = utils.build_stock_freed_event(
            correlation_id=correlation_id,
            order_id=event.payload.order_id,
        )
        response_event.id = f"stock-compensation:{event.id}"
        await repo.insert_outbox_message("order.request", response_event)


async def _process_stock_message(message, pool: AsyncConnectionPool) -> None:
    result = utils.decode_and_type_event(message)
    if isinstance(result, utils.Failure):
        logger.error("Stock decode failed: %s", result.error)
        return

    event = result.value
    correlation_id = utils.event_correlation_id(event)

    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                repo = StockRepository(cur)
                if await repo.inbox_event_exists(event.id):
                    inbox_result = await repo.get_inbox_event_result(event.id) or {
                        "status": "failure",
                        "reason": "duplicate_event",
                    }
                    await _emit_stock_result(repo, event, correlation_id, inbox_result)
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

                if event.event_type == utils.Commands.RESERVE_STOCK:
                    checkout_result = await _reserve_stock(repo, event)
                elif event.event_type == utils.Commands.FREE_STOCK:
                    checkout_result = await _free_stock(repo, event)
                else:
                    checkout_result = {"status": "failure", "reason": "unsupported_event_type"}

                await repo.set_inbox_event_result(
                    event_id=event.id,
                    status="PROCESSED",
                    result=checkout_result,
                    error=checkout_result.get("reason") if checkout_result.get("status") == "failure" else None,
                )
                await _emit_stock_result(repo, event, correlation_id, checkout_result)
                await conn.commit()
    except Exception as exc:
        logger.exception(
            "Stock consumer error event_id=%s correlation_id=%s: %s",
            event.id, correlation_id, exc,
        )


async def _cleanup_loop(pool: AsyncConnectionPool) -> None:
    while True:
        await asyncio.sleep(30)
        await _cleanup_inbox(pool)


async def main() -> None:
    pool_size = int(os.getenv("DB_POOL_MAX_SIZE", "4"))
    logger.info("Stock consumer starting pool_size=%s", pool_size)

    async with AsyncConnectionPool(conninfo=_conninfo(), min_size=1, max_size=pool_size) as pool:
        consumer = AIOKafkaConsumer(
            "stock.request",
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id="stock-consumer-group",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            fetch_max_wait_ms=10,
            max_poll_records=500,
            max_poll_interval_ms=300000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000,
        )
        await consumer.start()
        logger.info("Stock consumer started")

        cleanup_task = asyncio.create_task(_cleanup_loop(pool))
        try:
            while True:
                records = await consumer.getmany(timeout_ms=100, max_records=pool_size)
                if not records:
                    continue
                tasks = [
                    asyncio.create_task(_process_stock_message(msg, pool))
                    for msgs in records.values()
                    for msg in msgs
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                try:
                    await consumer.commit()
                except CommitFailedError:
                    logger.warning("Stock consumer commit failed after rebalance, messages will be replayed")
                for r in results:
                    if isinstance(r, Exception):
                        logger.exception("Stock consumer task failed: %s", r)
        finally:
            cleanup_task.cancel()
            await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
