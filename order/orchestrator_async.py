# saga_orchestrator_async.py
#
# Async CheckoutSagaOrchestrator using Redis pub/sub for multi-instance support.
#
# Why Redis instead of asyncio.Event:
#   With multiple uvicorn replicas behind a load balancer, the Kafka consumer
#   reply may arrive on a different instance than the one holding the open
#   HTTP connection. asyncio.Event only works within a single process.
#   Redis pub/sub is external to all instances — any replica can publish,
#   and the correct waiting coroutine (on any instance) will wake up.
#
# Flow per saga:
#   HTTP handler      subscribes to "order:saga:<correlation_id>"
#   Kafka consumer    publishes result to "order:saga:<correlation_id>"
#   HTTP handler      wakes up, reads result, advances or terminates saga

import asyncio
import json
import logging
from typing import Optional
import uuid

import redis.asyncio as aioredis

from services import utils
from saga_core import SagaStatus, SagaContext, SagaStep


logger = logging.getLogger(__name__)


class CheckoutSagaOrchestrator:
    """
    Async checkout saga orchestrator with Redis pub/sub for cross-replica signaling.

    Saga flow:
        1. Subscribe to Redis channel for this saga
        2. Emit RESERVE_STOCK → stock.request
        3. Await Redis message → STOCK_ALLOCATED or STOCK_UNAVAILABLE
        4. On STOCK_ALLOCATED → emit START_PAYMENT → payment.request
        5. Await Redis message → PAYMENT_SUCCEEDED or PAYMENT_FAILED
        6. On PAYMENT_FAILED → emit FREE_STOCK (compensation)
        7. Return final result to HTTP caller
    """

    def __init__(
        self,
        kafka_producer,
        redis_client: aioredis.Redis,
        db_pool,
        timeout_seconds: float = 30.0,
    ):
        self.kafka_producer = kafka_producer
        self.redis_client = redis_client
        self.db_pool = db_pool
        self.timeout_seconds = timeout_seconds

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run(self, order_id: str, order_value) -> tuple:
        items_list = [(item_id, qty) for item_id, qty in order_value.items]
        if not items_list:
            return {"status": "failed", "message": "Order has no items."}, 400

        context = SagaContext(
            data={
                "order_id": order_id,
                "user_id": order_value.user_id,
                "items": items_list,
            }, 
            saga_id=str(uuid.uuid4()),
            step=SagaStep.CHECKOUT,
            status=SagaStatus.PENDING,
            results={}
        )
        context.status = SagaStatus.RUNNING

        channel = f"order:saga:{context.saga_id}"

        # ives befSubscribe BEFORE sending to Kafka — eliminates the race condition
        # where the reply arrore we start listening
        pubsub = self.redis_client.pubsub()
        await pubsub.subscribe(channel)

        try:
            # Phase 1 — stock reservation
            await self._emit_reserve_stock(context)
            stock_msg = await self._wait_for_message(pubsub, context.saga_id)

            if stock_msg is None:
                return self._timeout_response(order_id, context.saga_id)

            if stock_msg["status"] == "unavailable":
                context.status = SagaStatus.FAILED
                return {
                    "status": "failed",
                    "order_id": order_id,
                    "correlation_id": context.saga_id,
                    "message": "Stock unavailable.",
                }, 400

            # Stock allocated — advance to payment phase
            context.set_result(SagaStep.STOCK_RESERVATION.value, stock_msg)
            context.advance()
            logger.info(f"Saga [{context.saga_id}] stock allocated, triggering payment.")

            # Phase 2 — payment
            # Still subscribed to the same channel — correlation_id is unchanged
            await self._emit_start_payment(
                context,
                order_id=stock_msg["order_id"],
                amount=stock_msg["amount"]
            )
            payment_msg = await self._wait_for_message(pubsub, context.saga_id)

            if payment_msg is None:
                return self._timeout_response(order_id, context.saga_id)

            if payment_msg["status"] == "success":
                context.set_result(SagaStep.PAYMENT.value, payment_msg)
                context.status = SagaStatus.COMPLETED
                logger.info(f"Saga [{context.saga_id}] completed successfully.")
                return {
                    "status": "success",
                    "order_id": order_id,
                    "correlation_id": context.saga_id,
                    "message": "Checkout completed successfully.",
                }, 200
            else:
                context.status = SagaStatus.COMPENSATING
                reason = payment_msg.get("reason", "Payment failed.")
                logger.info(f"Saga [{context.saga_id}] payment failed: {reason}. Compensating.")
                await self._compensate(context)
                context.status = SagaStatus.COMPENSATED
                return {
                    "status": "failed",
                    "order_id": order_id,
                    "correlation_id": context.saga_id,
                    "message": reason,
                }, 400

        finally:
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()

    # ------------------------------------------------------------------
    # Redis pub/sub wait — suspends coroutine, never blocks a thread
    # ------------------------------------------------------------------

    async def _wait_for_message(
        self,
        pubsub: aioredis.client.PubSub,
        saga_id: str,
    ) -> Optional[dict]:
        """
        Asynchronously poll for the next meaningful Redis pub/sub message.

        Each `await` suspends this coroutine and yields to the event loop —
        other HTTP requests are served normally during the wait.
        Returns the parsed result dict, or None on timeout.
        """
        loop = asyncio.get_event_loop()
        deadline = loop.time() + self.timeout_seconds

        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                logger.warning(f"Saga [{saga_id}] timed out waiting for message.")
                return None

            try:
                # Suspend for up to 1s — event loop is free during this await
                message = await asyncio.wait_for(
                    pubsub.get_message(ignore_subscribe_messages=True),
                    timeout=min(1.0, remaining)
                )
            except asyncio.TimeoutError:
                continue  # check deadline, then retry

            if message and message["type"] == "message":
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode()
                return json.loads(data)

    # ------------------------------------------------------------------
    # Kafka commands
    # ------------------------------------------------------------------

    async def _emit_reserve_stock(self, context: SagaContext):
        event = utils.BaseEvent(
            event_type=utils.Commands.RESERVE_STOCK,
            correlation_id=context.saga_id,
            payload=utils.ReserveStockCommandPayload(
                order_id=context.data["order_id"],
                items=context.data["items"]
            )
        )
        await self.kafka_producer.send("stock.request", value=event)
        logger.info(f"Saga [{context.saga_id}] emitted RESERVE_STOCK.")

    async def _emit_start_payment(
        self,
        context: SagaContext,
        order_id: str,
        amount: int
    ):
        event = utils.BaseEvent(
            event_type=utils.Commands.START_PAYMENT,
            correlation_id=context.saga_id,
            payload=utils.StartPaymentCommandPayload(
                order_id=order_id,
                user_id=context.data["user_id"],
                amount=amount
            )
        )
        await self.kafka_producer.send("payment.request", value=event)
        logger.info(f"Saga [{context.saga_id}] emitted START_PAYMENT.")

    async def _emit_free_stock(self, context: SagaContext, order_id: str):
        event = utils.BaseEvent(
            event_type=utils.Commands.FREE_STOCK,
            correlation_id=context.saga_id,
            payload=utils.ReserveStockCommandPayload(
                order_id=order_id,
                items=context.data["items"]
            )
        )
        await self.kafka_producer.send("stock.request", value=event)
        logger.info(f"Saga [{context.saga_id}] emitted FREE_STOCK.")

    async def _compensate(self, context: SagaContext):
        stock_result = context.get_result(SagaStep.STOCK_RESERVATION.value)
        if stock_result:
            await self._emit_free_stock(context, stock_result["order_id"])

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _timeout_response(order_id: str, saga_id: str) -> tuple:
        return {
            "status": "timeout",
            "order_id": order_id,
            "correlation_id": saga_id,
            "message": "Saga did not complete in time.",
        }, 504