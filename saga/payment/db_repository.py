import json as std_json
import uuid
from typing import Any
from msgspec import json as msgspec_json


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
            CREATE TABLE IF NOT EXISTS received_events (
                event_id TEXT PRIMARY KEY,
                created_at TIMESTAMPTZ DEFAULT now(),
                update_at TIMESTAMPTZ DEFAULT now(),
                status TEXT DEFAULT 'RECEIVED',
                result JSONB DEFAULT '{}'
            )
            """
        )
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS outbox (
                id TEXT PRIMARY KEY,
                topic TEXT NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT now(),
                sent BOOLEAN DEFAULT FALSE
            )
            """
        )

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

    def received_event_exists(self, event_id: str) -> bool:
        self.cur.execute("SELECT event_id FROM received_events WHERE event_id = %s", (event_id,))
        return self.cur.fetchone() is not None

    def insert_received_event(self, event_id: str) -> None:
        self.cur.execute(
            "INSERT INTO received_events (event_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (event_id,),
        )

    def get_received_event_result(self, event_id: str) -> dict[str, Any] | None:
        self.cur.execute("SELECT result FROM received_events WHERE event_id = %s", (event_id,))
        row = self.cur.fetchone()
        if row is None:
            return None
        return row["result"]

    def set_received_event_result(self, event_id: str, status: str, result: dict[str, Any]) -> None:
        self.cur.execute(
            "UPDATE received_events SET status = %s, result = %s, update_at = now() WHERE event_id = %s",
            (status, self._to_jsonb(result), event_id),
        )

    def insert_outbox_message(self, topic: str, payload: Any) -> None:
        self.cur.execute(
            "INSERT INTO outbox (id, topic, payload) VALUES (%s, %s, %s)",
            (str(uuid.uuid4()), topic, self._to_jsonb(payload)),
        )