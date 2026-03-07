from __future__ import annotations

import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import psycopg
import requests

logger = logging.getLogger(__name__)

# ── Retry configuration for commit phase ──
MAX_COMMIT_RETRIES = 5
INITIAL_RETRY_DELAY = 0.5  # seconds


class TxStatus(str, Enum):
    """Presumed Abort: only COMMITTED is persisted to the coordinator log."""
    COMMITTED = "COMMITTED"


class PrepareVote(str, Enum):
    YES       = "YES"
    NO        = "NO"
    READ_ONLY = "READ_ONLY"


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

    # ── public API ──

    def run(self, order_id: str, participants: list[Participant]) -> CoordinatorResult:
        """Drive a full 2PC round.  Never raises — returns CoordinatorResult."""
        transaction_id = str(uuid.uuid4())
        logger.debug("2PC START  tx=%s  order=%s", transaction_id, order_id)

        # ── Presumed Abort: no INITIATED log write needed ──

        # ── Phase 1: Prepare (parallel) ──
        votes: dict[str, PrepareVote] = {}
        prepared: list[Participant] = []

        with ThreadPoolExecutor(max_workers=len(participants)) as pool:
            futures = {
                pool.submit(self._prepare, transaction_id, p): p
                for p in participants
            }
            for future in as_completed(futures):
                p = futures[future]
                vote = future.result()
                votes[p.name] = vote
                if vote == PrepareVote.YES:
                    prepared.append(p)
                # READ_ONLY — noted but not added to 'prepared'

        # Any NO vote → abort everyone who voted YES
        if PrepareVote.NO in votes.values():
            failed = [n for n, v in votes.items() if v == PrepareVote.NO]
            logger.warning("2PC ABORT  tx=%s  vote=NO from %s", transaction_id, failed)
            self._abort_all(transaction_id, prepared)
            # Presumed Abort: no ABORTED log write needed
            return CoordinatorResult.failure(
                transaction_id, f"Prepare failed for: {failed}"
            )

        # ── Force-write COMMITTED before sending commits (Presumed Abort) ──
        if not self._persist(transaction_id, order_id, TxStatus.COMMITTED):
            self._abort_all(transaction_id, prepared)
            return CoordinatorResult.failure(
                transaction_id, "Failed to persist commit decision"
            )
        logger.debug("2PC COMMIT_DECIDED  tx=%s", transaction_id)

        # ── Phase 2: Commit (parallel, with retries) ──
        # Only commit participants that voted YES (skip READ_ONLY)
        to_commit = [p for p in participants if votes.get(p.name) == PrepareVote.YES]
        self._commit_all_with_retries(transaction_id, to_commit)

        logger.debug("2PC COMMITTED  tx=%s", transaction_id)
        return CoordinatorResult.ok(transaction_id)

    def get_transaction_status(self, transaction_id: str) -> str:
        """Inquiry protocol: participants can poll this to resolve uncertain txs.

        Returns 'COMMITTED' if committed, 'ABORTED' otherwise (presumed abort).
        """
        try:
            with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT status FROM order_transactions WHERE transaction_id = %s",
                        (transaction_id,),
                    )
                    row = cur.fetchone()
                    if row and row[0] == TxStatus.COMMITTED.value:
                        return "COMMITTED"
        except psycopg.Error as exc:
            logger.error("Inquiry error  tx=%s  exc=%s", transaction_id, exc)
        # Presumed Abort: unknown transaction → ABORTED
        return "ABORTED"

    # ── internal helpers ──

    def _prepare(self, transaction_id: str, p: Participant) -> PrepareVote:
        try:
            r = requests.post(
                p.url(p.prepare_url, transaction_id),
                json=p.prepare_body,
                timeout=self.timeout,
            )
            if r.status_code != 200:
                logger.warning("PREPARE fail  participant=%s  status=%s", p.name, r.status_code)
                return PrepareVote.NO
            # Read-only optimisation: participant signals no mutation needed
            body = r.json() if r.content else {}
            if body.get("read_only", False):
                logger.debug("PREPARE read-only  participant=%s", p.name)
                return PrepareVote.READ_ONLY
            return PrepareVote.YES
        except requests.exceptions.RequestException as exc:
            logger.error("PREPARE error  participant=%s  exc=%s", p.name, exc)
            return PrepareVote.NO

    def _commit_with_retries(self, transaction_id: str, p: Participant) -> bool:
        """Commit with exponential-backoff retries.

        The commit decision is irrevocable — we MUST keep retrying.
        """
        delay = INITIAL_RETRY_DELAY
        for attempt in range(1, MAX_COMMIT_RETRIES + 1):
            try:
                r = requests.post(
                    p.url(p.commit_url, transaction_id), timeout=self.timeout
                )
                if r.status_code == 200:
                    return True
                logger.warning(
                    "COMMIT fail  participant=%s  status=%s  attempt=%d",
                    p.name, r.status_code, attempt,
                )
            except requests.exceptions.RequestException as exc:
                logger.warning(
                    "COMMIT error  participant=%s  exc=%s  attempt=%d",
                    p.name, exc, attempt,
                )
            if attempt < MAX_COMMIT_RETRIES:
                time.sleep(delay)
                delay *= 2
        logger.critical(
            "COMMIT EXHAUSTED RETRIES  participant=%s  tx=%s", p.name, transaction_id
        )
        return False

    def _commit_all_with_retries(
        self, transaction_id: str, participants: list[Participant]
    ) -> None:
        """Commit all participants in parallel with retries."""
        if not participants:
            return
        with ThreadPoolExecutor(max_workers=len(participants)) as pool:
            futures = {
                pool.submit(self._commit_with_retries, transaction_id, p): p
                for p in participants
            }
            for future in as_completed(futures):
                p = futures[future]
                if not future.result():
                    logger.critical(
                        "COMMIT PERMANENTLY FAILED  participant=%s  tx=%s — "
                        "requires recovery sweep",
                        p.name, transaction_id,
                    )

    def _abort(self, transaction_id: str, p: Participant) -> None:
        try:
            requests.post(p.url(p.abort_url, transaction_id), timeout=self.timeout)
        except requests.exceptions.RequestException as exc:
            logger.error("ABORT error  participant=%s  exc=%s", p.name, exc)

    def _abort_all(self, transaction_id: str, participants: list[Participant]) -> None:
        """Abort all given participants in parallel."""
        if not participants:
            return
        with ThreadPoolExecutor(max_workers=len(participants)) as pool:
            futures = [
                pool.submit(self._abort, transaction_id, p) for p in participants
            ]
            for f in as_completed(futures):
                f.result()  # propagate any unexpected exceptions

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
                        prepare=False,
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