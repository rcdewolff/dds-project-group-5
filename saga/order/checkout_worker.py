import asyncio
import json
import logging
import os
import socket
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from time import monotonic
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from kafka import KafkaConsumer, KafkaProducer


CHECKOUT_MAX_INFLIGHT = int(os.getenv("CHECKOUT_MAX_INFLIGHT", "600"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CHECKOUT_COMMANDS_TOPIC = os.getenv("CHECKOUT_COMMANDS_TOPIC", "checkout-commands")
CHECKOUT_RESULTS_TOPIC = os.getenv("CHECKOUT_RESULTS_TOPIC", "checkout-results")
CHECKOUT_WORKER_GROUP_ID = os.getenv("CHECKOUT_WORKER_GROUP_ID", "checkout-worker")

TERMINAL_STATUSES = {"completed", "failed", "compensated"}

logger = logging.getLogger("checkout-worker")


@dataclass(slots=True)
class WorkerState:
    producer: KafkaProducer
    consumer: KafkaConsumer
    consumer_task: asyncio.Task[None]
    stop_event: asyncio.Event
    waiters: dict[str, asyncio.Future[dict[str, Any]]] = field(default_factory=dict)
    waiters_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    inflight_semaphore: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(CHECKOUT_MAX_INFLIGHT))
    inflight_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    inflight_count: int = 0


def _map_final_status(status_payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """Map terminal saga status to HTTP response."""
    status = status_payload.get("status")
    body = {
        "status": "success" if status == "completed" else "failed",
        "order_id": status_payload.get("order_id"),
        "correlation_id": status_payload.get("correlation_id"),
        "results": status_payload.get("results") or {},
        "error": status_payload.get("error"),
    }
    if status == "completed":
        body["message"] = "Checkout completed successfully."
        return 200, body
    # failed or compensated
    body["message"] = "Checkout failed."
    return 400, body


def _worker_group_id() -> str:
    return f"{CHECKOUT_WORKER_GROUP_ID}-{socket.gethostname()}-{os.getpid()}"


def _build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        CHECKOUT_RESULTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=_worker_group_id(),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        consumer_timeout_ms=1000,
    )


def _build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def _decode_checkout_result(raw_value: bytes | str | dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(raw_value, bytes):
        payload = json.loads(raw_value.decode("utf-8"))
    elif isinstance(raw_value, str):
        payload = json.loads(raw_value)
    else:
        payload = raw_value

    if not isinstance(payload, dict):
        return None

    if "payload" in payload and "correlation_id" in payload:
        inner_payload = payload.get("payload")
        if isinstance(inner_payload, dict):
            return {
                "correlation_id": inner_payload.get("correlation_id") or payload.get("correlation_id"),
                "order_id": inner_payload.get("order_id") or payload.get("order_id"),
                "status": inner_payload.get("status"),
                "results": inner_payload.get("results") or {},
                "error": inner_payload.get("error"),
                "event_type": inner_payload.get("event_type") or payload.get("event_type"),
                "timestamp": inner_payload.get("timestamp") or payload.get("timestamp"),
            }

    return {
        "correlation_id": payload.get("correlation_id"),
        "order_id": payload.get("order_id"),
        "status": payload.get("status"),
        "results": payload.get("results") or {},
        "error": payload.get("error"),
        "event_type": payload.get("event_type"),
        "timestamp": payload.get("timestamp"),
    }


async def _register_waiter(state: WorkerState, correlation_id: str) -> asyncio.Future[dict[str, Any]]:
    loop = asyncio.get_running_loop()
    waiter: asyncio.Future[dict[str, Any]] = loop.create_future()
    async with state.waiters_lock:
        state.waiters[correlation_id] = waiter
    return waiter


async def _remove_waiter(
    state: WorkerState,
    correlation_id: str,
) -> None:
    async with state.waiters_lock:
        state.waiters.pop(correlation_id, None)


async def _publish_checkout_command(state: WorkerState, *, correlation_id: str, order_id: str) -> None:
    command_event = {
        "id": str(uuid.uuid4()),
        "event_type": "checkout.command",
        "correlation_id": correlation_id,
        "order_id": order_id,
        "timestamp": time.time(),
    }

    send_future = await asyncio.to_thread(
        state.producer.send,
        CHECKOUT_COMMANDS_TOPIC,
        key=correlation_id.encode("utf-8"),
        value=command_event,
    )
    await asyncio.to_thread(send_future.get, 10)


async def _consume_checkout_results(state: WorkerState) -> None:
    while not state.stop_event.is_set():
        try:
            records = await asyncio.to_thread(state.consumer.poll, timeout_ms=1000, max_records=200)
            if not records:
                continue

            for _, messages in records.items():
                for message in messages:
                    try:
                        decoded = _decode_checkout_result(message.value)
                    except Exception as exc:
                        logger.warning("checkout_result_decode_error error=%s", exc)
                        continue

                    if not decoded:
                        continue

                    status = decoded.get("status")
                    correlation_id = decoded.get("correlation_id")
                    if status not in TERMINAL_STATUSES or not correlation_id:
                        continue

                    async with state.waiters_lock:
                        waiter = state.waiters.pop(correlation_id, None)

                    if waiter and not waiter.done():
                        waiter.set_result(decoded)
        except Exception:
            logger.exception("checkout_result_consumer_loop_error")
            await asyncio.sleep(0.5)


async def _cancel_all_waiters(state: WorkerState, reason: str) -> None:
    async with state.waiters_lock:
        pending = list(state.waiters.values())
        state.waiters.clear()

    for waiter in pending:
        if not waiter.done():
            waiter.set_exception(asyncio.CancelledError(reason))


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = _build_producer()
    consumer = _build_consumer()
    stop_event = asyncio.Event()
    state = WorkerState(
        producer=producer,
        consumer=consumer,
        stop_event=stop_event,
        consumer_task=asyncio.create_task(asyncio.sleep(0)),
    )
    state.consumer_task = asyncio.create_task(_consume_checkout_results(state))
    app.state.worker = state
    logger.info(
        "checkout_worker_started command_topic=%s result_topic=%s group_id=%s max_inflight=%s",
        CHECKOUT_COMMANDS_TOPIC,
        CHECKOUT_RESULTS_TOPIC,
        consumer.config.get("group_id"),
        CHECKOUT_MAX_INFLIGHT,
    )
    try:
        yield
    finally:
        stop_event.set()
        state.consumer_task.cancel()
        try:
            await state.consumer_task
        except asyncio.CancelledError:
            pass
        await _cancel_all_waiters(state, "worker shutdown")
        await asyncio.to_thread(producer.flush, 5)
        await asyncio.to_thread(producer.close)
        await asyncio.to_thread(consumer.close)


app = FastAPI(title="order-checkout-worker", lifespan=lifespan)


@app.post("/orders/checkout/{order_id}")
async def checkout(order_id: str, request: Request):
    state: WorkerState = request.app.state.worker

    try:
        await asyncio.wait_for(state.inflight_semaphore.acquire(), timeout=0.05)
    except TimeoutError:
        raise HTTPException(
            status_code=429,
            detail={"status": "saturated", "message": "Too many in-flight checkout requests."},
        )

    started_at = monotonic()
    acquired = True
    correlation_id = str(uuid.uuid4())

    async with state.inflight_lock:
        state.inflight_count += 1

    try:
        # Register waiter before publish to avoid command/result race windows.
        waiter = await _register_waiter(state, correlation_id)
        try:
            try:
                await _publish_checkout_command(state, correlation_id=correlation_id, order_id=order_id)
            except Exception as exc:
                logger.exception("checkout_command_publish_failed order_id=%s correlation_id=%s", order_id, correlation_id)
                raise HTTPException(
                    status_code=400,
                    detail={"status": "failed", "message": "Unable to enqueue checkout command.", "error": str(exc)},
                )

            status_payload = await asyncio.shield(waiter)
            code, body = _map_final_status(status_payload)
            logger.info(
                json.dumps(
                    {
                        "order_id": order_id,
                        "correlation_id": correlation_id,
                        "total_ms": int((monotonic() - started_at) * 1000),
                        "terminal_status": status_payload.get("status"),
                        "inflight_count": state.inflight_count,
                    }
                )
            )
            if code >= 400:
                raise HTTPException(code, detail=body)
            return body
        finally:
            await _remove_waiter(state, correlation_id)
            if not waiter.done():
                waiter.cancel()
    finally:
        if acquired:
            state.inflight_semaphore.release()
        async with state.inflight_lock:
            state.inflight_count = max(0, state.inflight_count - 1)
