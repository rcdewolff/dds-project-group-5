import json as std_json
import inspect
import time
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from msgspec import json

from services import utils


FINAL_STATUSES = {"completed", "failed", "compensated"}
ACTIVE_STATUSES = {"pending", "running", "compensating"}


@dataclass(frozen=True, slots=True)
class SagaDispatchContext:
    saga_id: str
    saga: dict[str, Any]
    results: dict[str, Any]
    event: utils.BaseEvent[Any] | None = None


@dataclass(frozen=True, slots=True)
class SagaDispatch:
    topic: str
    build_event: Callable[[SagaDispatchContext], utils.BaseEvent[Any]]


@dataclass(frozen=True, slots=True)
class SagaTransition:
    status: str
    step: str
    dispatch: SagaDispatch | None = None
    apply: Callable[[Any, dict[str, Any], dict[str, Any], utils.BaseEvent[Any]], None | Awaitable[None]] | None = None
    error: str | Callable[[dict[str, Any], dict[str, Any], utils.BaseEvent[Any]], str | None] | None = None


@dataclass(frozen=True, slots=True)
class SagaDefinition:
    initial_status: str
    initial_step: str
    build_initial_results: Callable[[dict[str, Any]], dict[str, Any]]
    initial_dispatch: SagaDispatch
    transitions: Mapping[str, SagaTransition]
    result_topic: str
    result_event_type: str = "checkout.result"


class Orchestrator:
    def __init__(self, *, final_statuses: set[str] | None = None):
        self.final_statuses = final_statuses or FINAL_STATUSES

    def _load_results(self, row: dict[str, Any]) -> dict[str, Any]:
        value = row.get("results")
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                return std_json.loads(value)
            except std_json.JSONDecodeError:
                return {}
        return {}

    async def _insert_outbox_event(self, cur, topic: str, event: utils.BaseEvent[Any]) -> None:
        payload_text = json.encode(event).decode()
        await cur.execute(
            """
            INSERT INTO outbox (
                id,
                event_id,
                topic,
                message_key,
                correlation_id,
                payload,
                status,
                publish_attempts,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, 'PENDING', 0, now())
            """,
            (
                str(uuid.uuid4()),
                event.id,
                topic,
                utils.event_correlation_id(event),
                utils.event_correlation_id(event),
                payload_text,
            ),
        )

    async def _emit_terminal_result_once(
        self,
        cur,
        *,
        definition: SagaDefinition,
        correlation_id: str,
        order_id: str,
        status: str,
        results: dict[str, Any],
        error: str | None = None,
    ) -> None:
        event_payload = {
            "event_type": definition.result_event_type,
            "correlation_id": correlation_id,
            "order_id": order_id,
            "status": status,
            "results": results,
            "error": error,
            "timestamp": time.time(),
        }
        event = utils.BaseEvent.create(
            event_type=definition.result_event_type,
            payload=event_payload,
            order_id=order_id,
            correlation_id=correlation_id,
            id=f"checkout-result:{correlation_id}",
        )
        payload_text = json.encode(event).decode()

        await cur.execute(
            """
            INSERT INTO outbox (
                id,
                event_id,
                topic,
                message_key,
                correlation_id,
                payload,
                status,
                publish_attempts,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, 'PENDING', 0, now())
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                str(uuid.uuid4()),
                event.id,
                definition.result_topic,
                correlation_id,
                correlation_id,
                payload_text,
            ),
        )

    async def _upsert_saga(self, cur, saga_id: str, order_id: str, status: str, step: str, results: dict[str, Any]) -> None:
        await cur.execute(
            """
            INSERT INTO sagas (order_id, id, status, step, results)
            VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (id) DO UPDATE
            SET status = EXCLUDED.status,
                step = EXCLUDED.step,
                results = EXCLUDED.results
            """,
            (order_id, saga_id, status, step, std_json.dumps(results)),
        )

    def _append_transition(self, results: dict[str, Any], event: utils.BaseEvent[Any], status: str) -> None:
        results.setdefault("transitions", [])
        results["transitions"].append({"event_type": event.event_type, "status": status})

    async def start_saga(
        self,
        cur,
        *,
        definition: SagaDefinition,
        saga_data: dict[str, Any],
        correlation_id: str | None = None,
    ) -> tuple[str, bool]:
        order_id = saga_data["order_id"]

        if correlation_id:
            await cur.execute(
                """
                SELECT id
                FROM sagas
                WHERE id = %s
                """,
                (correlation_id,),
            )
            existing_by_id = await cur.fetchone()
            if existing_by_id:
                return existing_by_id["id"], False

        await cur.execute(
            """
            SELECT id, status
            FROM sagas
            WHERE order_id = %s
            ORDER BY created_at DESC NULLS LAST, id DESC
            LIMIT 1
            """,
            (order_id,),
        )
        existing = await cur.fetchone()
        if existing and existing["status"] in ACTIVE_STATUSES:
            return existing["id"], False

        saga_id = correlation_id or str(uuid.uuid4())
        results = definition.build_initial_results(saga_data)
        dispatch_context = SagaDispatchContext(saga_id=saga_id, saga={"order_id": order_id, **saga_data}, results=results)
        dispatch_event = definition.initial_dispatch.build_event(dispatch_context)

        await self._upsert_saga(
            cur,
            saga_id=saga_id,
            order_id=order_id,
            status=definition.initial_status,
            step=definition.initial_step,
            results=results,
        )
        await self._insert_outbox_event(cur, definition.initial_dispatch.topic, dispatch_event)
        return saga_id, True

    async def apply_event(self, cur, event: utils.BaseEvent[Any], *, definition: SagaDefinition) -> dict[str, Any]:
        correlation_id = utils.event_correlation_id(event)
        await cur.execute(
            """
            SELECT id, order_id, status, step, results
            FROM sagas
            WHERE id = %s
            """,
            (correlation_id,),
        )
        saga = await cur.fetchone()
        if saga is None:
            return {"handled": False, "reason": "saga_not_found"}

        status = saga["status"]
        results = self._load_results(saga)
        results.setdefault("transitions", [])

        if status in self.final_statuses:
            return {"handled": False, "reason": "saga_already_final"}

        transition = definition.transitions.get(event.event_type)
        if transition is None:
            return {"handled": False, "reason": "unsupported_event_type"}

        if transition.apply is not None:
            apply_result = transition.apply(cur, results, saga, event)
            if inspect.isawaitable(apply_result):
                await apply_result

        self._append_transition(results, event, transition.status)

        if transition.dispatch is not None:
            dispatch_context = SagaDispatchContext(saga_id=correlation_id, saga=saga, results=results, event=event)
            dispatch_event = transition.dispatch.build_event(dispatch_context)
            await self._upsert_saga(
                cur,
                saga_id=correlation_id,
                order_id=saga["order_id"],
                status=transition.status,
                step=transition.step,
                results=results,
            )
            await self._insert_outbox_event(cur, transition.dispatch.topic, dispatch_event)
            return {"handled": True, "status": transition.status}

        error_value: str | None = None
        if isinstance(transition.error, str):
            error_value = transition.error
        elif callable(transition.error):
            error_value = transition.error(results, saga, event)

        await self._upsert_saga(
            cur,
            saga_id=correlation_id,
            order_id=saga["order_id"],
            status=transition.status,
            step=transition.step,
            results=results,
        )
        await self._emit_terminal_result_once(
            cur,
            definition=definition,
            correlation_id=correlation_id,
            order_id=saga["order_id"],
            status=transition.status,
            results=results,
            error=error_value,
        )
        return {"handled": True, "status": transition.status}
