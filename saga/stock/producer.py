import logging
import os
import signal
import threading
import time
from typing import Any

from msgspec import json
from psycopg.rows import dict_row  # type: ignore
from psycopg_pool import ConnectionPool  # type: ignore

from services import kafka_client, utils


logger = logging.getLogger(__name__)


class OutboxRelay:
	"""Background worker that relays unsent outbox rows to Kafka."""

	def __init__(self, db_pool, kafka_producer, poll_interval: float = 0.5, fetch_batch_size: int = 20):
		self.db_pool = db_pool
		self.kafka_producer = kafka_producer
		self.poll_interval = poll_interval
		self.fetch_batch_size = fetch_batch_size
		self._stop_event = threading.Event()
		self._thread: threading.Thread | None = None
		self._cycle = 0

	@classmethod
	def from_env(
		cls,
		service_name: str = "stock-outbox-relay",
		poll_interval: float = 0.05,
		fetch_batch_size: int = 10,
	) -> tuple["OutboxRelay", ConnectionPool, Any]:
		"""Build a standalone relay from environment variables."""
		conninfo = (
			f"host={os.environ['POSTGRES_HOST']} "
			f"port={os.environ['POSTGRES_PORT']} "
			f"user={os.environ['POSTGRES_USER']} "
			f"password={os.environ['POSTGRES_PASSWORD']} "
			f"dbname={os.environ['POSTGRES_DB']}"
		)
		db_pool = ConnectionPool(
			conninfo=conninfo,
			min_size=1,
			max_size=5,
			reconnect_timeout=30,
			kwargs={"connect_timeout": 10},
		)
		kafka = kafka_client.Client(service_name, [])
		relay = cls(
			db_pool=db_pool,
			kafka_producer=kafka.producer,
			poll_interval=poll_interval,
			fetch_batch_size=fetch_batch_size,
		)
		return relay, db_pool, kafka

	def start(self):
		if self._thread is not None and self._thread.is_alive():
			return
		self._stop_event.clear()
		self._thread = threading.Thread(target=self._run, daemon=True, name="stock-outbox-relay")
		self._thread.start()
		logger.info("Outbox relay started")

	def stop(self):
		self._stop_event.set()
		if self._thread is not None:
			self._thread.join(timeout=2.0)
		logger.info("Outbox relay stopped")

	def run_forever(self):
		"""Run relay loop in foreground, suitable for dedicated container processes."""
		logger.info("Outbox relay running in foreground")
		self._run()

	def _run(self):
		while not self._stop_event.is_set():
			sent = self.relay_oldest_unsent()
			if not sent:
				time.sleep(self.poll_interval)
			self._cycle += 1
			if self._cycle % 100 == 0:
				self._cleanup_published()

	def _cleanup_published(self) -> None:
		"""Delete old published outbox rows to prevent table bloat."""
		try:
			with self.db_pool.connection() as conn:
				with conn.cursor() as cur:
					cur.execute(
						"""
						DELETE FROM outbox
						WHERE status = 'PUBLISHED'
						AND published_at < now() - interval '1 minutes'
						"""
					)
				conn.commit()
				logger.debug("Outbox cleanup completed")
		except Exception as exc:
			logger.warning("Outbox cleanup failed: %s", exc)

	def relay_oldest_unsent(self) -> bool:
		"""
		Reads a small batch of unsent outbox rows, publishes each to Kafka,
		and marks successful ones as sent.
		"""
		if self.db_pool is None or self.kafka_producer is None:
			return False

		try:
			with self.db_pool.connection() as conn:
				with conn.cursor(row_factory=dict_row) as cur:
					cur.execute(
						"""
						SELECT id, topic, payload, message_key, event_id, correlation_id, publish_attempts
						FROM outbox
						WHERE status = 'PENDING'
						ORDER BY created_at ASC
						LIMIT %s
						FOR UPDATE SKIP LOCKED
						""",
						(self.fetch_batch_size,),
					)
					rows = cur.fetchall()
					if not rows:
						return False

					relayed_count = 0
					for row in rows:
						try:
							event = _payload_to_base_event(row["payload"])
							message_key = str(row["message_key"]).encode("utf-8")
							future = self.kafka_producer.send(topic=row["topic"], key=message_key, value=event)
							future.get(timeout=10)

							cur.execute(
								"""
								UPDATE outbox
								SET status = 'PUBLISHED',
								    publish_attempts = publish_attempts + 1,
								    published_at = now(),
								    last_error = NULL
								WHERE id = %s
								""",
								(row["id"],),
							)
							relayed_count += 1
						except Exception as row_exc:
							cur.execute(
								"""
								UPDATE outbox
								SET publish_attempts = publish_attempts + 1,
								    last_error = %s
								WHERE id = %s
								""",
								(str(row_exc), row["id"]),
							)
							logger.exception(
								"Outbox relay failed for row %s on topic %s correlation_id=%s: %s",
								row.get("id"),
								row.get("topic"),
								row.get("correlation_id"),
								row_exc,
							)

			return relayed_count > 0
		except Exception as exc:
			logger.exception("Outbox relay failed: %s", exc)
			return False


def _payload_to_base_event(payload: Any) -> utils.BaseEvent:
	"""
	Normalise outbox payload to utils.BaseEvent.
	Supports payload persisted as dict/str/bytes.
	"""
	if isinstance(payload, bytes):
		return json.decode(payload, type=utils.BaseEvent[dict])

	if isinstance(payload, str):
		return json.decode(payload.encode(), type=utils.BaseEvent[dict])

	if isinstance(payload, dict):
		correlation_id = payload.get("correlation_id") or payload.get("saga_id")
		return utils.BaseEvent(
			id=payload["id"],
			event_type=payload["event_type"],
			order_id=payload["order_id"],
			correlation_id=correlation_id,
			saga_id=payload.get("saga_id", correlation_id),
			payload=payload["payload"],
			timestamp=payload.get("timestamp", time.time()),
		)

	raise ValueError(f"Unsupported outbox payload type: {type(payload)}")


def _parse_poll_interval(raw_value: str) -> float:
	"""Parses poll interval values like '0.5' or '0.5s'."""
	value = raw_value.strip().lower()
	if value.endswith("s"):
		value = value[:-1].strip()
	interval = float(value)
	if interval <= 0:
		raise ValueError("OUTBOX_POLL_INTERVAL must be > 0")
	return interval


def _parse_fetch_batch_size(raw_value: str) -> int:
	"""Parses and validates the outbox batch size."""
	batch_size = int(raw_value)
	if batch_size <= 0:
		raise ValueError("OUTBOX_FETCH_BATCH_SIZE must be > 0")
	return batch_size


def main():
	logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
	logging.getLogger("kafka").setLevel(logging.INFO)
	poll_interval = _parse_poll_interval(os.getenv("OUTBOX_POLL_INTERVAL", "0.05s"))

	relay, db_pool, kafka = OutboxRelay.from_env(
		poll_interval=poll_interval,
		fetch_batch_size=_parse_fetch_batch_size(os.getenv("OUTBOX_FETCH_BATCH_SIZE", "20")),
	)

	def _shutdown(_signum, _frame):
		relay.stop()
		db_pool.close()
		try:
			kafka.producer.flush(timeout=5)
			kafka.producer.close()
		except Exception:
			pass

	signal.signal(signal.SIGTERM, _shutdown)
	signal.signal(signal.SIGINT, _shutdown)

	try:
		relay.run_forever()
	finally:
		_shutdown(None, None)


if __name__ == "__main__":
	main()
