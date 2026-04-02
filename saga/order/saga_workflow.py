"""Checkout workflow adapter.

The order service supplies the concrete checkout steps here, while the generic
Orchestrator class handles saga persistence, dispatch, and terminal reporting.
"""

from typing import Any

from orchestrator import Orchestrator, SagaDefinition, SagaDispatch, SagaDispatchContext, SagaTransition
from services import utils


FINAL_STATUSES = {"completed", "failed", "compensated"}
ORCHESTRATOR = Orchestrator(final_statuses=FINAL_STATUSES)


def _build_initial_results(saga_data: dict[str, Any]) -> dict[str, Any]:
    return {
        "order_snapshot": {
            "order_id": saga_data["order_id"],
            "user_id": saga_data["user_id"],
            "items": [[item_id, qty] for item_id, qty in saga_data["items"]],
        },
        "transitions": [
            {
                "event_type": utils.OrderInternalEvent.CHECKOUT_INITIATED.value,
                "status": "success",
            }
        ],
    }


def _build_reserve_stock_event(context: SagaDispatchContext) -> utils.BaseEvent[Any]:
    snapshot = context.results["order_snapshot"]
    items = [(item_id, int(quantity)) for item_id, quantity in snapshot["items"]]
    return utils.reserve_stock_command_event(
        correlation_id=context.saga_id,
        order_id=snapshot["order_id"],
        items=items,
    )


def _build_start_payment_event(context: SagaDispatchContext) -> utils.BaseEvent[Any]:
    snapshot = context.results["order_snapshot"]
    amount = int(context.results.get("stock", {}).get("amount", 0))
    return utils.start_payment_command_event(
        correlation_id=context.saga_id,
        order_id=snapshot["order_id"],
        user_id=snapshot["user_id"],
        amount=amount,
    )


def _build_free_stock_event(context: SagaDispatchContext) -> utils.BaseEvent[Any]:
    snapshot = context.results["order_snapshot"]
    items = [(item_id, int(quantity)) for item_id, quantity in snapshot["items"]]
    return utils.free_stock_command_event(
        correlation_id=context.saga_id,
        order_id=snapshot["order_id"],
        items=items,
    )


def _record_stock_reserved(cur, results: dict[str, Any], saga: dict[str, Any], event: utils.BaseEvent[Any]) -> None:
    amount = int(getattr(event.payload, "amount", 0))
    results["stock"] = {"status": "success", "amount": amount}


def _record_stock_unavailable(cur, results: dict[str, Any], saga: dict[str, Any], event: utils.BaseEvent[Any]) -> None:
    results["stock"] = {"status": "failed", "reason": "stock_unavailable"}


async def _record_payment_success(cur, results: dict[str, Any], saga: dict[str, Any], event: utils.BaseEvent[Any]) -> None:
    results["payment"] = {"status": "success"}
    await cur.execute(
        "UPDATE orders SET paid = TRUE WHERE order_id = %s",
        (saga["order_id"],),
    )


def _record_payment_failure(cur, results: dict[str, Any], saga: dict[str, Any], event: utils.BaseEvent[Any]) -> None:
    reason = getattr(event.payload, "reason", "payment_failed")
    results["payment"] = {"status": "failed", "reason": reason}


def _record_compensation_success(cur, results: dict[str, Any], saga: dict[str, Any], event: utils.BaseEvent[Any]) -> None:
    results["compensation"] = {"status": "success"}


CHECKOUT_SAGA_DEFINITION = SagaDefinition(
    initial_status="running",
    initial_step="STOCK_RESERVATION_PHASE",
    build_initial_results=_build_initial_results,
    initial_dispatch=SagaDispatch(
        topic="stock.request",
        build_event=_build_reserve_stock_event,
    ),
    transitions={
        utils.StockIntegrationEvent.STOCK_ALLOCATED: SagaTransition(
            status="running",
            step="PAYMENT_PHASE",
            dispatch=SagaDispatch(
                topic="payment.request",
                build_event=_build_start_payment_event,
            ),
            apply=_record_stock_reserved,
        ),
        utils.StockIntegrationEvent.STOCK_UNAVAILABLE: SagaTransition(
            status="failed",
            step="STOCK_RESERVATION_PHASE",
            apply=_record_stock_unavailable,
            error="stock_unavailable",
        ),
        utils.PaymentIntegrationEvent.PAYMENT_SUCCEEDED: SagaTransition(
            status="completed",
            step="PAYMENT_PHASE",
            apply=_record_payment_success,
        ),
        utils.PaymentIntegrationEvent.PAYMENT_FAILED: SagaTransition(
            status="compensating",
            step="STOCK_RESERVATION_PHASE",
            dispatch=SagaDispatch(
                topic="stock.request",
                build_event=_build_free_stock_event,
            ),
            apply=_record_payment_failure,
        ),
        utils.StockIntegrationEvent.STOCK_FREED: SagaTransition(
            status="compensated",
            step="STOCK_RESERVATION_PHASE",
            apply=_record_compensation_success,
            error=lambda results, saga, event: results.get("payment", {}).get("reason", "payment_failed"),
        ),
    },
    result_topic="checkout-results",
)


async def start_checkout(
    cur,
    order_id: str,
    user_id: str,
    items: list[tuple[str, int]],
    correlation_id: str | None = None,
) -> tuple[str, bool]:
    """Start checkout by handing the generic orchestrator the concrete step inputs."""

    return await ORCHESTRATOR.start_saga(
        cur,
        definition=CHECKOUT_SAGA_DEFINITION,
        saga_data={"order_id": order_id, "user_id": user_id, "items": items},
        correlation_id=correlation_id,
    )


async def apply_saga_event(cur, event: utils.BaseEvent[Any]) -> dict[str, Any]:
    return await ORCHESTRATOR.apply_event(cur, event, definition=CHECKOUT_SAGA_DEFINITION)
