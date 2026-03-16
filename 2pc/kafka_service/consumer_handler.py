
from __future__ import annotations

import json
import logging
import threading
from typing import Callable

import msgspec
import psycopg

from kafka_service.kafka_event import BaseEvent

logger = logging.getLogger(__name__)

Handler = Callable[[BaseEvent], None]


class EventConsumer:

    def __init__(self, kafka_consumer, db_pool, service_name: str):

        self._consumer     = kafka_consumer
        self._db_pool      = db_pool
        self._service_name = service_name
        self._handlers: dict[str, list[Handler]] = {}


    def on(self, event_type: str) -> Callable[[Handler], Handler]:

        def decorator(fn: Handler) -> Handler:
            self._handlers.setdefault(event_type, []).append(fn)
            logger.debug("Registered handler  service=%s  event_type=%s  fn=%s",
                         self._service_name, event_type, fn.__name__)
            return fn
        return decorator

    def register(self, event_type: str, fn: Handler) -> None:
        """Imperative alternative to @on."""
        self._handlers.setdefault(event_type, []).append(fn)


    def start(self) -> None:
        """Spawn the consumer loop as a daemon background thread."""
        thread = threading.Thread(
            target=self._run,
            name=f"{self._service_name}-consumer",
            daemon=True,
        )
        thread.start()
        logger.info("EventConsumer started  service=%s", self._service_name)


    def _run(self) -> None:
        logger.info("Consumer loop running  service=%s", self._service_name)

        for raw_message in self._consumer:
            try:
                event = msgspec.json.decode(raw_message.value, type=BaseEvent)
            except Exception as exc:
                logger.error(
                    "Deserialise failed  topic=%s  offset=%s  exc=%s",
                    raw_message.topic, raw_message.offset, exc,
                )
                self._consumer.commit()
                continue

            logger.debug(
                "Received  topic=%s  offset=%s  event_type=%s  correlation_id=%s",
                raw_message.topic, raw_message.offset,
                event.event_type, event.correlation_id,
            )

            try:
                self._log_event(event, raw_message.topic, raw_message.offset)
            except Exception as exc:
                logger.error(
                    "Audit log failed  event_type=%s  correlation_id=%s  exc=%s",
                    event.event_type, event.correlation_id, exc,
                )
                # Do NOT commit — will retry on restart
                continue


            # 3. Commit offset only after full success 
            self._consumer.commit()
            logger.debug(
                "Offset committed  topic=%s  offset=%s",
                raw_message.topic, raw_message.offset,
            )

    def _dispatch(self, event: BaseEvent) -> None:
        handlers = self._handlers.get(event.event_type, [])
        if not handlers:
            logger.debug("No handlers registered  event_type=%s", event.event_type)
            return
        for fn in handlers:
            fn(event)  

    def _log_event(self, event: BaseEvent, topic: str, offset: int) -> None:
        """Persist the event to the service's event_log table."""
        with self._db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO event_log
                        (correlation_id, event_type, service, topic, kafka_offset, payload, received_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (correlation_id) DO NOTHING
                    """,
                    (
                        event.correlation_id,
                        event.event_type,
                        self._service_name,
                        topic,
                        offset,
                        json.dumps(event.payload),
                    ),
                )
                conn.commit()
