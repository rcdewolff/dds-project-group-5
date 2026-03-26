import asyncio
import json
import logging
import os
import socket
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from time import monotonic
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from kafka import KafkaConsumer


ORDER_SERVICE_URL = os.getenv("ORDER_SERVICE_URL", "http://order-service:5000")
CHECKOUT_TIMEOUT_SECONDS = float(os.getenv("CHECKOUT_TIMEOUT_SECONDS", "35"))
CHECKOUT_MAX_INFLIGHT = int(os.getenv("CHECKOUT_MAX_INFLIGHT", "200"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CHECKOUT_RESULTS_TOPIC = os.getenv("CHECKOUT_RESULTS_TOPIC", "checkout-results")
CHECKOUT_WORKER_GROUP_ID = os.getenv("CHECKOUT_WORKER_GROUP_ID", "checkout-worker")

TERMINAL_STATUSES = {"completed", "failed", "compensated"}
WAIT_SLICE_SECONDS = 0.5

logger = logging.getLogger("checkout-worker")


@dataclass(slots=True)
class WorkerState:
    http_client: httpx.AsyncClient
    consumer: KafkaConsumer
    consumer_task: asyncio.Task[None]
    stop_event: asyncio.Event
    waiters: dict[str, set[asyncio.Future[dict[str, Any]]]] = field(default_factory=dict)
    waiters_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    inflight_semaphore: asyncio.Semaphore = field(default_factory=lambda: asyncio.Semaphore(CHECKOUT_MAX_INFLIGHT))
    inflight_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    inflight_count: int = 0


def _map_final_status(status_payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
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
    if status in {"failed", "compensated"}:
        body["message"] = "Checkout failed."
        return 400, body
    return 500, {
        "status": "failed",
        "message": f"Unexpected final saga status: {status}",
    }


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
        bucket = state.waiters.setdefault(correlation_id, set())
        bucket.add(waiter)
    return waiter


async def _remove_waiter(
    state: WorkerState,
    correlation_id: str,
    waiter: asyncio.Future[dict[str, Any]],
) -> None:
    async with state.waiters_lock:
        bucket = state.waiters.get(correlation_id)
        if not bucket:
            return
        bucket.discard(waiter)
        if not bucket:
            state.waiters.pop(correlation_id, None)


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
                        bucket = state.waiters.pop(correlation_id, set())
                    for waiter in bucket:
                        if not waiter.done():
                            waiter.set_result(decoded)
        except Exception:
            logger.exception("checkout_result_consumer_loop_error")
            await asyncio.sleep(0.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    timeout = httpx.Timeout(connect=5.0, read=10.0, write=10.0, pool=10.0)
    http_client = httpx.AsyncClient(timeout=timeout)
    consumer = _build_consumer()
    stop_event = asyncio.Event()
    state = WorkerState(
        http_client=http_client,
        consumer=consumer,
        stop_event=stop_event,
        consumer_task=asyncio.create_task(asyncio.sleep(0)),
    )
    state.consumer_task = asyncio.create_task(_consume_checkout_results(state))
    app.state.worker = state
    logger.info(
        "checkout_worker_started topic=%s group_id=%s max_inflight=%s",
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
        await asyncio.to_thread(consumer.close)
        await http_client.aclose()


app = FastAPI(title="order-checkout-worker", lifespan=lifespan)


@app.post("/orders/checkout/{order_id}")
async def checkout(order_id: str, request: Request):
    state: WorkerState = request.app.state.worker

    try:
        await asyncio.wait_for(state.inflight_semaphore.acquire(), timeout=0.01)
    except TimeoutError:
        raise HTTPException(status_code=503, detail={"status": "saturated", "message": "Too many in-flight checkout requests."})

    started_at = monotonic()
    acquired = True
    correlation_id: str | None = None

    async with state.inflight_lock:
        state.inflight_count += 1

    try:
        start_resp = await state.http_client.post(f"{ORDER_SERVICE_URL}/checkout/start/{order_id}")
        if start_resp.status_code >= 400:
            raise HTTPException(start_resp.status_code, detail=start_resp.text)

        start_payload = start_resp.json()
        correlation_id = start_payload.get("correlation_id")
        if not correlation_id:
            raise HTTPException(500, detail="Order service did not return correlation_id")

        waiter = await _register_waiter(state, correlation_id)
        try:
            deadline = monotonic() + CHECKOUT_TIMEOUT_SECONDS
            while True:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    pending_body = {
                        "status": "pending",
                        "order_id": order_id,
                        "correlation_id": correlation_id,
                    }
                    logger.info(
                        json.dumps(
                            {
                                "order_id": order_id,
                                "correlation_id": correlation_id,
                                "total_ms": int((monotonic() - started_at) * 1000),
                                "terminal_status": None,
                                "pending_returned": True,
                                "inflight_count": state.inflight_count,
                            }
                        )
                    )
                    return JSONResponse(status_code=202, content=pending_body)

                try:
                    status_payload = await asyncio.wait_for(
                        asyncio.shield(waiter),
                        timeout=min(WAIT_SLICE_SECONDS, remaining),
                    )
                    code, body = _map_final_status(status_payload)
                    logger.info(
                        json.dumps(
                            {
                                "order_id": order_id,
                                "correlation_id": correlation_id,
                                "total_ms": int((monotonic() - started_at) * 1000),
                                "terminal_status": status_payload.get("status"),
                                "pending_returned": False,
                                "inflight_count": state.inflight_count,
                            }
                        )
                    )
                    if code >= 400:
                        raise HTTPException(code, detail=body)
                    return body
                except TimeoutError:
                    if await request.is_disconnected():
                        logger.info(
                            "checkout_client_disconnected order_id=%s correlation_id=%s",
                            order_id,
                            correlation_id,
                        )
                        raise HTTPException(status_code=499, detail="Client disconnected")
        finally:
            await _remove_waiter(state, correlation_id, waiter)
            if not waiter.done():
                waiter.cancel()
    finally:
        if acquired:
            state.inflight_semaphore.release()
        async with state.inflight_lock:
            state.inflight_count = max(0, state.inflight_count - 1)
