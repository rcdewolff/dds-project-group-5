# saga_orchestrator.py
#
# Encapsulates the entire checkout saga coordination logic.
# The orchestrator owns:
#   - Starting the saga (RESERVE_STOCK command to stock service)
#   - Listening on Redis for saga events
#   - Advancing the saga on success (trigger payment after stock allocated)
#   - Handling failures and timeouts
#   - Returning a final HTTP-ready result tuple to the caller
#
# Dependencies injected at construction time to keep this testable
# and decoupled from Flask globals.

import time
import logging
from typing import Optional
import uuid
from msgspec import json

import redis as redis_lib

from services import utils
from saga_core import SagaStatus
from saga_core import SimpleSagaContext, SimpleSagaStep

logger = logging.getLogger(__name__)


class CheckoutSagaOrchestrator:
    """
    Coordinates the checkout saga across Stock and Payment services.

    Saga flow:
        1. Emit RESERVE_STOCK command → stock.request topic
        2. Wait on Redis for STOCK_ALLOCATED or STOCK_UNAVAILABLE
        3. On STOCK_ALLOCATED → emit START_PAYMENT command → payment.request topic
        4. Wait on Redis for PAYMENT_SUCCEEDED or PAYMENT_FAILED
        5. Return final result to the HTTP caller

    Compensation:
        - STOCK_UNAVAILABLE  → no compensation needed (nothing was committed)
        - PAYMENT_FAILED     → emit FREE_STOCK command → stock.request topic
    """

    def __init__(
        self,
        kafka_producer,
        redis_client: redis_lib.Redis,
        db_pool,
        timeout_seconds: float = 30.0,
    ):
        self.kafka_producer = kafka_producer
        self.redis_client = redis_client
        self.db_pool = db_pool
        self.timeout_seconds = timeout_seconds

    # ------------------------------------------------------------------
    # Public entry point — called directly from the checkout endpoint
    # ------------------------------------------------------------------

    def run(self, order_id: str, order_value) -> tuple:
        """
        Execute the full checkout saga synchronously (blocking the calling thread).

        Returns a (dict, int) tuple ready to be returned from a Flask endpoint.
        """
        items_list = [(item_id, qty) for item_id, qty in order_value.items]
        if not items_list:
            return {"status": "failed", "message": "Order has no items."}, 400

        context = SimpleSagaContext(
            data={
                "order_id": order_id,
                "user_id": order_value.user_id,
                "items": items_list,
            }, 
            saga_id=str(uuid.uuid4()),
            step=SimpleSagaStep.CHECKOUT,
            status=SagaStatus.PENDING,
            results={}
        )
        context.status = SagaStatus.RUNNING

        channel = f"order:saga:{context.saga_id}"

        # Subscribe BEFORE sending to Kafka — eliminates the race condition
        # where the reply arrives before we start listening
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(channel)
        pubsub.get_message()  # flush the subscribe confirmation message

        try:
            # Step 1 — kick off the saga
            self._emit_reserve_stock(context)

            # Main event loop — drives the saga through its phases
            return self._event_loop(context, pubsub, channel, order_id)

        finally:
            pubsub.unsubscribe(channel)
            pubsub.close()

    # ------------------------------------------------------------------
    # Event loop — blocks the thread, advancing the saga on each event
    # ------------------------------------------------------------------

    def _event_loop(
        self,
        context: SimpleSagaContext,
        pubsub,
        channel: str,
        order_id: str,
    ) -> tuple:
        deadline = time.time() + self.timeout_seconds

        while time.time() < deadline:
            message = pubsub.get_message(timeout=1.0)
            if not message or message["type"] != "message":
                continue

            data = message["data"]
            decode_result = utils.decode_and_type_event(utils.RedisMessageWrapper(data))

            if isinstance(decode_result, utils.Failure):
                logger.warning(f"Failed to decode saga message: {decode_result.error}")
                return self._handle_decode_failure(decode_result, order_id, context)

            event = decode_result.value
            logger.info(f"Saga [{context.saga_id}] received: {event.event_type}")

            outcome = self._handle_event(event, context, order_id)
            if outcome is not None:
                return outcome
            # outcome is None → saga is still in progress, keep looping

        # Deadline exceeded
        context.status = SagaStatus.FAILED
        logger.warning(f"Saga [{context.saga_id}] timed out for order: {order_id}")
        return {
            "status": "timeout",
            "order_id": order_id,
            "correlation_id": context.saga_id,
            "message": "Saga did not complete in time.",
        }, 504

    # ------------------------------------------------------------------
    # Event dispatch — returns a result tuple to terminate, or None to continue
    # ------------------------------------------------------------------

    def _handle_event(
        self,
        event: utils.BaseEvent,
        context: SimpleSagaContext,
        order_id: str,
    ) -> Optional[tuple]:

        event_type = event.event_type

        # --- Stock phase outcomes ---

        if event_type == utils.StockIntegrationEvent.STOCK_ALLOCATED:
            payload: utils.StockReservedPayload = event.payload
            context.set_result(SimpleSagaStep.STOCK_RESERVATION.value, {
                "status": "success",
                "order_id": payload.order_id,
                "amount": payload.amount
            })
            context.advance()  # STOCK_RESERVATION → PAYMENT
            logger.info(f"Saga [{context.saga_id}] stock allocated, triggering payment.")
            self._emit_start_payment(context, payload.order_id, payload.amount)
            return None  # keep looping — wait for payment outcome

        if event_type == utils.StockIntegrationEvent.STOCK_UNAVAILABLE:
            context.status = SagaStatus.FAILED
            logger.info(f"Saga [{context.saga_id}] stock unavailable for order: {order_id}")
            # No compensation needed — nothing was committed yet
            return {
                "status": "failed",
                "order_id": order_id,
                "correlation_id": context.saga_id,
                "message": "Stock unavailable.",
            }, 400

        # --- Payment phase outcomes ---

        if event_type == utils.PaymentIntegrationEvent.PAYMENT_SUCCEEDED:
            context.set_result(SimpleSagaStep.PAYMENT.value, {"status": "success"})
            context.status = SagaStatus.COMPLETED
            logger.info(f"Saga [{context.saga_id}] completed successfully.")
            return {
                "status": "success",
                "order_id": order_id,
                "correlation_id": context.saga_id,
                "message": "Checkout completed successfully.",
            }, 200

        if event_type == utils.PaymentIntegrationEvent.PAYMENT_FAILED:
            context.status = SagaStatus.COMPENSATING
            reason = getattr(event.payload, "reason", "Payment failed.")
            logger.info(f"Saga [{context.saga_id}] payment failed: {reason}. Compensating.")
            self._compensate(context)
            context.status = SagaStatus.COMPENSATED
            return {
                "status": "failed",
                "order_id": order_id,
                "correlation_id": context.saga_id,
                "message": reason,
            }, 400

        # Unknown event type — log and keep looping
        logger.warning(f"Saga [{context.saga_id}] unhandled event type: {event_type}")
        return None

    # ------------------------------------------------------------------
    # Kafka commands — one method per outgoing command
    # ------------------------------------------------------------------

    def _emit_reserve_stock(self, context: SimpleSagaContext):
        order_id = context.data["order_id"]
        items = context.data["items"]

        event = utils.BaseEvent(
            event_type=utils.Commands.RESERVE_STOCK,
            correlation_id=context.saga_id,
            payload=utils.ReserveStockCommandPayload(
                order_id=order_id,
                items=items
            )
        )
        self.kafka_producer.send(topic="stock.request", value=event)
        logger.info(f"Saga [{context.saga_id}] emitted RESERVE_STOCK for order: {order_id}")

    def _emit_start_payment(self, context: SimpleSagaContext, order_id: str, amount: int):
        user_id = context.data["user_id"]

        event = utils.BaseEvent(
            event_type=utils.Commands.START_PAYMENT,
            correlation_id=context.saga_id,
            payload=utils.StartPaymentCommandPayload(
                order_id=order_id,
                user_id=user_id,
                amount=amount
            )
        )
        self.kafka_producer.send(topic="payment.request", value=event)
        logger.info(f"Saga [{context.saga_id}] emitted START_PAYMENT for order: {order_id}")

    # ------------------------------------------------------------------
    # Compensation — reverse completed steps in reverse order
    # ------------------------------------------------------------------

    def _compensate(self, context: SimpleSagaContext):
        """
        Walk back completed steps in reverse order.
        Currently only stock reservation needs compensation.
        """
        stock_result = context.get_result(SimpleSagaStep.STOCK_RESERVATION.value)
        if stock_result:
            order_id = stock_result["order_id"]
            items = context.data["items"]
            logger.info(f"Saga [{context.saga_id}] compensating stock for order: {order_id}")
            self._emit_free_stock(context, order_id, items)

    def _emit_free_stock(self, context: SimpleSagaContext, order_id: str, items: list):
        event = utils.BaseEvent(
            event_type=utils.Commands.FREE_STOCK,
            correlation_id=context.saga_id,
            payload=utils.ReserveStockCommandPayload(
                order_id=order_id,
                items=items
            )
        )
        self.kafka_producer.send(topic="stock.request", value=event)
        logger.info(f"Saga [{context.saga_id}] emitted FREE_STOCK for order: {order_id}")

    # ------------------------------------------------------------------
    # Decode failure handling
    # ------------------------------------------------------------------

    def _handle_decode_failure(
        self,
        result: utils.Failure,
        order_id: str,
        context: SimpleSagaContext,
    ) -> tuple:
        context.status = SagaStatus.FAILED
        if result.error == "UNKNOWN_EVENT_TYPE":
            return {
                "status": "failed",
                "order_id": order_id,
                "correlation_id": context.saga_id,
                "message": f"Received unknown event type.",
            }, 400
        if result.error == "EMPTY_MESSAGE":
            return {
                "status": "failed",
                "order_id": order_id,
                "correlation_id": context.saga_id,
                "message": "Received empty message (tombstone).",
            }, 400
        return {
            "status": "failed",
            "order_id": order_id,
            "correlation_id": context.saga_id,
            "message": "Unrecognised decode error.",
        }, 500  # unrecognised decode error — keep looping