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
import psycopg # type: ignore
from psycopg.rows import dict_row # type: ignore

import redis as redis_lib # type: ignore
import json as std_json
from services import utils
from saga_core import SagaStatus, SagaStep
from saga_core import SagaContext, SagaStep

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
        order_id: str = ""  
    ):
        self.kafka_producer = kafka_producer
        self.redis_client = redis_client
        self.db_pool = db_pool
        self.timeout_seconds = timeout_seconds
        self.order_id = order_id
    # ------------------------------------------------------------------
    # Public entry point — called directly from the checkout endpoint
    # ------------------------------------------------------------------

    def run(self, order_id: str, order_value) -> tuple:
        """
        Starts the checkout saga for a given order_id and order details.
        Returns a tuple of (response_dict, http_status_code) to be returned by the Flask endpoint.
        """
        # Keep item shape consistent with stock handlers and payload structs.
        items_list = [(item_id, qty) for item_id, qty in order_value.items]
        if not items_list:
            return {"status": "failed", "message": "Order has no items."}, 400


        context = SagaContext(
            saga_id=str(uuid.uuid4()),
            step=SagaStep.CHECKOUT,
            status=SagaStatus.PENDING,  
            results={},
            order_id=order_id,
            user_id=order_value.user_id,
            items=items_list
        )

        topic, outbox_msg = utils.build_reserve_stock_command(context.saga_id, context.order_id, context.items)
        self._transition(

            context=context,
            new_step=SagaStep.STOCK_RESERVATION,
            new_status=SagaStatus.RUNNING,  
            
            incoming_event=utils.OrderInternalEvent.CHECKOUT_INITIATED.value,
            outgoing_command=utils.Commands.RESERVE_STOCK.value,
            
            incoming_payload={"items": context.items},
            outbox_topic=topic,
            outbox_message=outbox_msg,
        )

        channel = f"order:saga:{context.saga_id}"
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe(channel)
        pubsub.get_message()  # flush subscribe confirmation

        try:
            return self._event_loop(context, pubsub)

        finally:
            pubsub.unsubscribe(channel)
            pubsub.close()

    # ------------------------------------------------------------------
    # Event loop — blocks the thread, advancing the saga on each event
    # ------------------------------------------------------------------

    def check_saga_exists(self, order_id: str) -> bool:
        with self.db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute(
                    """
                    SELECT status FROM sagas
                    WHERE order_id = %s AND status IN ('pending', 'running', 'compensating')
                    """,
                    (order_id,)
                )
                return bool(cur.fetchone())
    


    def _transition(
        # TODO Check out saga_step = CHECKOUT_COMPLETED
        self,
        context: SagaContext,
        new_status: SagaStatus,
        new_step: SagaStep | None,
        incoming_event: Optional[str] = None,
        outgoing_command: Optional[str] = None,
        incoming_payload: dict = {},
        outgoing_payload: dict = {},
        outbox_topic: Optional[str] = None,
        outbox_message: Optional[bytes] = None,
    ):
        """
        Atomically, in one transaction:
                    1. Upsert the saga current state in `sagas`
                    2. Write the outbox row (if a Kafka command needs to go out)

        The outbox relay (separate process) reads undelivered rows and
        sends them to Kafka, decoupling DB writes from Kafka availability.
        """
        context.status = new_status
        context.step = new_step if new_step else context.step  # only update if new_step is provided
        try:
            with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    # 1. Upsert saga current state
                    cur.execute(
                        """
                        INSERT INTO sagas (id, order_id, status, step, results)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE
                            SET status = EXCLUDED.status,
                                step = EXCLUDED.step,
                                results = EXCLUDED.results
                        """,
                        (
                            context.saga_id,
                            context.order_id,
                            context.status.value,
                            context.step.value if context.step else None,
                            std_json.dumps({step.value: value for step, value in context.results.items()}),
                        ),
                    )
                    # 2. Outbox — written atomically so the command is never lost
                    if outbox_topic and outbox_message:
                        payload_json = std_json.loads(outbox_message.decode())
                        cur.execute(
                            """
                            INSERT INTO outbox (id, topic, payload, created_at)
                            VALUES (%s, %s, %s::jsonb, now())
                            """,
                            (str(uuid.uuid4()), outbox_topic, std_json.dumps(payload_json)),
                        )
        except psycopg.Error as e:
            logger.error(
                f"Transition [{incoming_event} -> {outgoing_command}] failed for saga [{context.saga_id}]: {e}"
            )


    
    def _event_loop(
        self,
        context: SagaContext,
        pubsub,
    ) -> tuple:
        deadline = time.time() + self.timeout_seconds
        print(f"Saga [{context.saga_id}] started for order: {context.order_id}, waiting for events...")
        while time.time() < deadline:
            # TODO decrease this loop time if needed
            message = pubsub.get_message(timeout=0.01)
            if not message or message["type"] != "message":
                continue

            data = message["data"]
            decode_result = utils.decode_and_type_event(utils.RedisMessageWrapper(data))

            if isinstance(decode_result, utils.Failure):
                logger.warning(f"Failed to decode saga message: {decode_result.error}")
                return self._handle_decode_failure(decode_result, context.order_id, context)

            event = decode_result.value
            logger.info(f"Saga [{context.saga_id}] received: {event.event_type}")

            outcome = self._dispatch_event(event, context)
            if outcome is not None:
                return outcome
            # outcome is None → saga is still in progress, keep looping

        # Deadline exceeded
        self._transition(
            context=context,
            new_status=SagaStatus.FAILED,
            new_step=None,
            incoming_event=utils.SagaEvents.SAGA_TIMEOUT.value,
            outgoing_command=utils.SagaEvents.SAGA_ENDED.value,
        )
        logger.warning(f"Saga [{context.saga_id}] timed out for order: {context.order_id}")
        return {
            "status": "timeout",
            "order_id": context.order_id,
            "saga_id": context.saga_id,
            "message": "Saga did not complete in time.",
        }, 504

    # ------------------------------------------------------------------
    # Event dispatch — routes incoming events to single-case handlers
    # ------------------------------------------------------------------

    def _dispatch_event(
        self,
        event: utils.BaseEvent,
        context: SagaContext,
    ) -> Optional[tuple]:
        handlers = {
            utils.StockIntegrationEvent.STOCK_ALLOCATED:    self._on_stock_allocated,
            utils.StockIntegrationEvent.STOCK_UNAVAILABLE:  self._on_stock_unavailable,
            utils.StockIntegrationEvent.STOCK_FREED:        self._on_stock_freed,
            utils.PaymentIntegrationEvent.PAYMENT_SUCCEEDED: self._on_payment_succeeded,
            utils.PaymentIntegrationEvent.PAYMENT_FAILED:   self._on_payment_failed,
        }
        handler = handlers.get(event.event_type)
        if handler is None:
            logger.warning(f"Saga [{context.saga_id}] unhandled event type: {event.event_type}")
            return None
        return handler(event, context)

    # ------------------------------------------------------------------
    # Single-case handlers
    # ------------------------------------------------------------------

    def _on_stock_allocated(
        self,
        event: utils.BaseEvent,
        context: SagaContext,
    ) -> None:
        """
        On stock allocated, we advance to the payment step and emit the START_PAYMENT command.
        """

        payload: utils.StockReservedPayload = event.payload
        context.set_result(SagaStep.STOCK_RESERVATION, {
            "status": "success",
            "order_id": payload.order_id,
            "amount": payload.amount,
        })

        topic, outbox_msg = utils.build_start_payment_command(context.saga_id, payload.order_id, context.user_id, payload.amount)
        self._transition(
            context=context,
            new_status=SagaStatus.RUNNING,
            new_step=SagaStep.PAYMENT,

            incoming_event=utils.StockIntegrationEvent.STOCK_ALLOCATED.value,
            outgoing_command=utils.Commands.START_PAYMENT.value,
            incoming_payload={"amount": payload.amount},
            outgoing_payload={"user_id": context.user_id, "amount": payload.amount},
            
            outbox_topic=topic,
            outbox_message=outbox_msg,
        )
        return None  # keep looping — wait for payment outcome

    def _on_stock_unavailable(
        self,
        event: utils.BaseEvent,
        context: SagaContext
    ) -> tuple:
        self._transition(
            context=context,
            new_status=SagaStatus.FAILED,
            new_step=None,
            incoming_event=utils.StockIntegrationEvent.STOCK_UNAVAILABLE.value,
            outgoing_command=utils.SagaEvents.SAGA_ENDED.value,
            incoming_payload={"reason": "Stock unavailable"},
        )
        return {
            "status": "failed",
            "order_id": event.order_id,
            "message": "Stock unavailable.",
        }, 400

    def _on_payment_succeeded(
        self,
        event: utils.BaseEvent,
        context: SagaContext,
    ) -> tuple:
        context.set_result(SagaStep.PAYMENT, {"status": "success"})
        self._transition(
            context=context,
            new_status=SagaStatus.COMPLETED,
            new_step=None,
            incoming_event=utils.PaymentIntegrationEvent.PAYMENT_SUCCEEDED.value,
            outgoing_command=utils.SagaEvents.SAGA_ENDED.value,
        )
        return {
            "status": "success",
            "order_id": event.order_id,
            "message": "Checkout completed successfully.",
        }, 200

    def _on_payment_failed(
        self,
        event: utils.BaseEvent,
        context: SagaContext,
    ) -> Optional[tuple]:
        reason = getattr(event.payload, "reason", "Payment failed.")
        context.set_result(SagaStep.PAYMENT, {"status": "failure", "reason": reason})
        stock_result = context.get_result(SagaStep.STOCK_RESERVATION)
        comp_order_id = stock_result["order_id"] if stock_result else context.order_id
        topic, outbox_msg = utils.build_free_stock_command(context.saga_id, comp_order_id, context.items)
        # TX1: log failure + emit compensation command atomically
        self._transition(
            context=context,
            new_status=SagaStatus.COMPENSATING,
            new_step=SagaStep.STOCK_RESERVATION,
            incoming_event=utils.PaymentIntegrationEvent.PAYMENT_FAILED.value,
            outgoing_command=utils.Commands.FREE_STOCK.value,
            incoming_payload={"reason": reason},
            outbox_topic=topic,
            outbox_message=outbox_msg,
        )
        # Wait for STOCK_FREED before returning failure so compensation is complete.
        return None

    def _on_stock_freed(
        self,
        event: utils.BaseEvent,
        context: SagaContext,
    ) -> Optional[tuple]:
        # STOCK_FREED is only meaningful when we are compensating after payment failure.
        if context.status != SagaStatus.COMPENSATING:
            logger.info(f"Saga [{context.saga_id}] received STOCK_FREED outside compensation; ignoring.")
            return None

        payment_result = context.get_result(SagaStep.PAYMENT) or {}
        reason = payment_result.get("reason", "Payment failed.")

        self._transition(
            context=context,
            new_status=SagaStatus.COMPENSATED,
            new_step=None,
            incoming_event=utils.StockIntegrationEvent.STOCK_FREED.value,
            outgoing_command=utils.SagaEvents.SAGA_ENDED.value,
            incoming_payload={"reason": reason},
        )
        return {
            "status": "failed",
            "order_id": event.order_id,
            "message": reason,
        }, 400



    # ------------------------------------------------------------------
    # Decode failure handling
    # ------------------------------------------------------------------

    def _handle_decode_failure(
        self,
        result: utils.Failure,
        order_id: str,
        context: SagaContext,
    ) -> tuple:
        context.status = SagaStatus.FAILED
        if result.error == "UNKNOWN_EVENT_TYPE":
            return {
                "status": "failed",
                "order_id": order_id,
                "message": f"Received unknown event type.",
            }, 400
        if result.error == "EMPTY_MESSAGE":
            return {
                "status": "failed",
                "order_id": order_id,
                "message": "Received empty message (tombstone).",
            }, 400
        return {
            "status": "failed",
            "order_id": order_id,
            "message": "Unrecognised decode error.",
        }, 500  # unrecognised decode error — keep looping

