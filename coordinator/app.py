
from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import psycopg
import requests

logger = logging.getLogger(__name__)


class TxStatus(str, Enum):
    INITIATED = "INITIATED"
    PREPARED  = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED   = "ABORTED"


@dataclass
class Participant:

    name:         str
    prepare_url:  str
    commit_url:   str
    abort_url:    str
    prepare_body: dict[str, Any] = field(default_factory=dict)

    def url(self, template: str, transaction_id: str) -> str:
        return template.format(transaction_id=transaction_id)


@dataclass
class CoordinatorResult:
    transaction_id: str
    success:        bool
    error:          str | None = None

    @classmethod
    def ok(cls, transaction_id: str) -> "CoordinatorResult":
        return cls(transaction_id=transaction_id, success=True)

    @classmethod
    def failure(cls, transaction_id: str, error: str) -> "CoordinatorResult":
        return cls(transaction_id=transaction_id, success=False, error=error)


class TwoPhaseCommitCoordinator:

    def __init__(self, db_pool, timeout: int = 10):
        self.db_pool = db_pool
        self.timeout = timeout

    def run(self, order_id: str, participants: list[Participant]) -> CoordinatorResult:
        """Drive a full 2PC round. Never raises — returns CoordinatorResult."""
        transaction_id = str(uuid.uuid4())
        logger.debug("2PC START  tx=%s  order=%s", transaction_id, order_id)

        if not self._persist(transaction_id, order_id, TxStatus.INITIATED):
            return CoordinatorResult.failure(transaction_id, "Failed to create transaction record")

# Prepare Phase
        prepared: list[Participant] = []
        for p in participants:
            if self._prepare(transaction_id, p):
                prepared.append(p)
            else:
                logger.warning("2PC ABORT  tx=%s  participant=%s", transaction_id, p.name)
                self._abort_all(transaction_id, prepared)
                self._persist(transaction_id, order_id, TxStatus.ABORTED)
                return CoordinatorResult.failure(
                    transaction_id, f"Prepare failed for '{p.name}'"
                )

        self._persist(transaction_id, order_id, TxStatus.PREPARED)
        logger.debug("2PC PREPARED  tx=%s", transaction_id)

# Commit Phase
        committed: list[Participant] = []
        for p in participants:
            if self._commit(transaction_id, p):
                committed.append(p)
            else:
                logger.error("2PC COMMIT_FAIL  tx=%s  participant=%s", transaction_id, p.name)
                remaining = [r for r in participants if r not in committed and r is not p]
                self._abort_all(transaction_id, remaining)
                self._persist(transaction_id, order_id, TxStatus.ABORTED)
                return CoordinatorResult.failure(
                    transaction_id, f"Commit failed for '{p.name}'"
                )

        self._persist(transaction_id, order_id, TxStatus.COMMITTED)
        logger.debug("2PC COMMITTED  tx=%s", transaction_id)
        return CoordinatorResult.ok(transaction_id)


    def _prepare(self, transaction_id: str, p: Participant) -> bool:
        try:
            r = requests.post(p.url(p.prepare_url, transaction_id),
                              json=p.prepare_body, timeout=self.timeout)
            if r.status_code != 200:
                logger.warning("PREPARE fail  participant=%s  status=%s", p.name, r.status_code)
                return False
            return True
        except requests.exceptions.RequestException as exc:
            logger.error("PREPARE error  participant=%s  exc=%s", p.name, exc)
            return False

    def _commit(self, transaction_id: str, p: Participant) -> bool:
        try:
            r = requests.post(p.url(p.commit_url, transaction_id), timeout=self.timeout)
            if r.status_code != 200:
                logger.error("COMMIT fail  participant=%s  status=%s", p.name, r.status_code)
                return False
            return True
        except requests.exceptions.RequestException as exc:
            logger.error("COMMIT error  participant=%s  exc=%s", p.name, exc)
            return False

    def _abort(self, transaction_id: str, p: Participant) -> None:
        try:
            requests.post(p.url(p.abort_url, transaction_id), timeout=self.timeout)
        except requests.exceptions.RequestException as exc:
            logger.error("ABORT error  participant=%s  exc=%s", p.name, exc)

    def _abort_all(self, transaction_id: str, participants: list[Participant]) -> None:
        for p in participants:
            self._abort(transaction_id, p)


    def _persist(self, transaction_id: str, order_id: str, status: TxStatus) -> bool:
        try:
            with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO order_transactions (transaction_id, order_id, status)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (transaction_id)
                        DO UPDATE SET status = EXCLUDED.status
                        """,
                        (transaction_id, order_id, status.value),
                    )
                    if status == TxStatus.COMMITTED:
                        cur.execute(
                            "UPDATE orders SET paid = TRUE WHERE order_id = %s",
                            (order_id,),
                        )
                    conn.commit()
            return True
        except psycopg.Error as exc:
            logger.error("Failed to persist tx  tx=%s  status=%s  exc=%s",
                         transaction_id, status, exc)
            return False