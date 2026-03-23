import json as std_json
import uuid
from typing import Any
from msgspec import json as msgspec_json

from services import utils


class PaymentRepository:
    def __init__(self, cursor):
        self.cur = cursor

    @staticmethod
    def _to_jsonb(payload: Any) -> str:
        if isinstance(payload, dict):
            return std_json.dumps(payload)
        return msgspec_json.encode(payload).decode("utf-8")

    def create_tables(self) -> None:
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS log (
                id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload JSONB NOT NULL,
                version INTEGER NOT NULL,
                created_at TIMESTAMPTZ DEFAULT now()
            )
            """
        )
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_snapshots (
                user_id TEXT PRIMARY KEY,
                credit INTEGER NOT NULL,
                version INTEGER NOT NULL
            )
            """
        )
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS inbox (
                id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                topic TEXT NOT NULL,
                partition INTEGER,
                kafka_offset BIGINT,
                correlation_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'RECEIVED',
                payload_hash TEXT,
                payload JSONB,
                result JSONB DEFAULT '{}',
                error TEXT,
                received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                processed_at TIMESTAMPTZ
            )
            """
        )
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_payment_inbox_correlation ON inbox(correlation_id)")
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_payment_inbox_status ON inbox(status, received_at)")
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS outbox (
                id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL UNIQUE,
                topic TEXT NOT NULL,
                message_key TEXT NOT NULL,
                correlation_id TEXT NOT NULL,
                payload JSONB NOT NULL,
                headers JSONB,
                status TEXT NOT NULL DEFAULT 'PENDING',
                publish_attempts INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT now(),
                published_at TIMESTAMPTZ,
                last_error TEXT
            )
            """
        )
        self.cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS event_id TEXT")
        self.cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS message_key TEXT")
        self.cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS correlation_id TEXT")
        self.cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS headers JSONB")
        self.cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING'")
        self.cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS publish_attempts INTEGER DEFAULT 0")
        self.cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ")
        self.cur.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS last_error TEXT")
        self.cur.execute("UPDATE outbox SET status='PENDING' WHERE status IS NULL")
        self.cur.execute("UPDATE outbox SET event_id = id WHERE event_id IS NULL")
        self.cur.execute("UPDATE outbox SET correlation_id = COALESCE(correlation_id, '')")
        self.cur.execute("UPDATE outbox SET message_key = COALESCE(message_key, correlation_id, '')")
        self.cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_payment_outbox_event_id ON outbox(event_id)")
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_payment_outbox_status ON outbox(status, created_at)")

    def get_user_snapshot(self, user_id: str) -> dict[str, Any] | None:
        self.cur.execute(
            "SELECT user_id, credit, version FROM user_snapshots WHERE user_id = %s",
            (user_id,),
        )
        return self.cur.fetchone()

    def list_user_snapshots(self) -> list[dict[str, Any]]:
        self.cur.execute("SELECT user_id, credit FROM user_snapshots")
        return self.cur.fetchall()

    def get_events_for_user(self, user_id: str) -> list[dict[str, Any]]:
        self.cur.execute(
            "SELECT event_type, payload FROM log WHERE user_id = %s ORDER BY id",
            (user_id,),
        )
        return self.cur.fetchall()

    def insert_user_event(self, user_id: str, event_type: str, payload: dict[str, Any], version: int) -> None:
        self.cur.execute(
            "INSERT INTO log (id, user_id, event_type, payload, version) VALUES (%s, %s, %s, %s, %s)",
            (str(uuid.uuid4()), user_id, event_type, self._to_jsonb(payload), version),
        )

    def insert_user_snapshot(self, user_id: str, credit: int, version: int) -> None:
        self.cur.execute(
            "INSERT INTO user_snapshots (user_id, credit, version) VALUES (%s, %s, %s)",
            (user_id, credit, version),
        )

    def upsert_user_snapshot(self, user_id: str, credit: int, version: int) -> None:
        self.cur.execute(
            """
            INSERT INTO user_snapshots (user_id, credit, version)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE
            SET credit = EXCLUDED.credit, version = EXCLUDED.version
            """,
            (user_id, credit, version),
        )

    def update_user_snapshot_versioned(self, user_id: str, credit: int, new_version: int, current_version: int) -> bool:
        self.cur.execute(
            """UPDATE user_snapshots
               SET credit = %s, version = %s
               WHERE user_id = %s AND version = %s""",
            (credit, new_version, user_id, current_version),
        )
        return self.cur.rowcount > 0

    def inbox_event_exists(self, event_id: str) -> bool:
        self.cur.execute(
            "SELECT event_id FROM inbox WHERE event_id = %s AND status = 'PROCESSED'",
            (event_id,),
        )
        return self.cur.fetchone() is not None

    def insert_inbox_event(
        self,
        event_id: str,
        topic: str,
        partition: int,
        kafka_offset: int,
        correlation_id: str,
        payload: Any,
        payload_hash: str,
    ) -> None:
        self.cur.execute(
            """
            INSERT INTO inbox (id, event_id, topic, partition, kafka_offset, correlation_id, payload, payload_hash)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                str(uuid.uuid4()),
                event_id,
                topic,
                partition,
                kafka_offset,
                correlation_id,
                self._to_jsonb(payload),
                payload_hash,
            ),
        )

    def get_inbox_event_result(self, event_id: str) -> dict[str, Any] | None:
        self.cur.execute("SELECT result FROM inbox WHERE event_id = %s", (event_id,))
        row = self.cur.fetchone()
        if row is None:
            return None
        return row["result"]

    def set_inbox_event_result(self, event_id: str, status: str, result: dict[str, Any], error: str | None = None) -> None:
        self.cur.execute(
            """
            UPDATE inbox
            SET status = %s,
                result = %s,
                error = %s,
                processed_at = now()
            WHERE event_id = %s
            """,
            (status, self._to_jsonb(result), error, event_id),
        )

    def insert_outbox_message(self, topic: str, payload: Any, message_key: str, correlation_id: str) -> None:
        event_id = payload.id if isinstance(payload, utils.BaseEvent) else str(uuid.uuid4())
        self.cur.execute(
            """
            INSERT INTO outbox (id, event_id, topic, message_key, correlation_id, payload)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                str(uuid.uuid4()),
                event_id,
                topic,
                message_key,
                correlation_id,
                self._to_jsonb(payload),
            ),
        )