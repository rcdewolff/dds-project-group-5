import uuid
import json as std_json
from typing import Any
from msgspec import json as msgspec_json

from services import utils


class StockRepository:
    def __init__(self, cursor):
        self.cur = cursor

    @staticmethod
    def _to_jsonb(payload: Any) -> str:
        if isinstance(payload, dict):
            return std_json.dumps(payload)
        # Supports msgspec Structs like utils.BaseEvent
        return msgspec_json.encode(payload).decode("utf-8")

    def create_tables(self) -> None:
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS log (
                id TEXT PRIMARY KEY,
                item_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                payload JSONB NOT NULL,
                version INTEGER NOT NULL,
                created_at TIMESTAMPTZ DEFAULT now()
            )
            """
        )
        self.cur.execute(
            """
            CREATE TABLE IF NOT EXISTS item_snapshots (
                item_id TEXT PRIMARY KEY,
                stock INTEGER NOT NULL,
                price INTEGER NOT NULL,
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
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_inbox_correlation ON inbox(correlation_id)")
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_inbox_status ON inbox(status, received_at)")
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
        self.cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_stock_outbox_event_id ON outbox(event_id)")
        self.cur.execute("CREATE INDEX IF NOT EXISTS idx_stock_outbox_status ON outbox(status, created_at)")

    def get_item_snapshot(self, item_id: str) -> dict[str, Any] | None:
        self.cur.execute(
            "SELECT item_id, stock, price, version FROM item_snapshots WHERE item_id = %s",
            (item_id,),
        )
        return self.cur.fetchone()

    def list_item_snapshots(self) -> list[dict[str, Any]]:
        self.cur.execute("SELECT item_id, stock, price FROM item_snapshots")
        return self.cur.fetchall()

    def get_item_snapshots_for_ids(self, item_ids: list[str], include_price: bool = False) -> dict[str, dict[str, Any]]:
        if not item_ids:
            return {}
        placeholders = ",".join(["%s"] * len(item_ids))
        cols = "item_id, stock, version"
        if include_price:
            cols = "item_id, stock, price, version"
        self.cur.execute(
            f"SELECT {cols} FROM item_snapshots WHERE item_id IN ({placeholders})",
            item_ids,
        )
        return {row["item_id"]: row for row in self.cur.fetchall()}

    def get_log_events_for_item(self, item_id: str) -> list[dict[str, Any]]:
        self.cur.execute(
            "SELECT event_type, payload FROM log WHERE item_id = %s ORDER BY version",
            (item_id,),
        )
        return self.cur.fetchall()

    def insert_log_event(self, item_id: str, event_type: str, payload: dict[str, Any], version: int) -> None:
        self.cur.execute(
            "INSERT INTO log (id, item_id, event_type, payload, version) VALUES (%s, %s, %s, %s, %s)",
            (str(uuid.uuid4()), item_id, event_type, self._to_jsonb(payload), version),
        )

    def update_snapshot_versioned(self, item_id: str, new_stock: int, new_version: int, current_version: int) -> bool:
        print(f"Attempting to update snapshot for item {item_id} from version {current_version} to {new_version} with stock {new_stock}")
        self.cur.execute(
            """UPDATE item_snapshots
               SET stock = %s, version = %s
               WHERE item_id = %s AND version = %s""",
            (new_stock, new_version, item_id, current_version),
        )
        return self.cur.rowcount > 0

    def insert_item_snapshot(self, item_id: str, stock: int, price: int, version: int) -> None:
        self.cur.execute(
            "INSERT INTO item_snapshots (item_id, stock, price, version) VALUES (%s, %s, %s, %s)",
            (item_id, stock, price, version),
        )

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
            (status, std_json.dumps(result), error, event_id),
        )

    def insert_outbox_message(self, topic: str, payload: utils.BaseEvent) -> None:
        correlation_id = utils.event_correlation_id(payload)
        self.cur.execute(
            """
            INSERT INTO outbox (id, event_id, topic, message_key, correlation_id, payload)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                str(uuid.uuid4()),
                payload.id,
                topic,
                correlation_id,
                correlation_id,
                self._to_jsonb(payload),
            ),
        )
