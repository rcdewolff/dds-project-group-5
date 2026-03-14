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
            (status, std_json.dumps(result), event_id),
        )

    def insert_outbox_message(self, topic: str, payload: utils.BaseEvent) -> None:
        self.cur.execute(
            "INSERT INTO outbox (id, topic, payload) VALUES (%s, %s, %s)",
            (str(uuid.uuid4()), topic, self._to_jsonb(payload)),
        )
