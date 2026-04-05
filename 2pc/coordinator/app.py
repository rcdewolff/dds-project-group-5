from __future__ import annotations

import logging
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

import psycopg
from psycopg import sql
import requests

logger = logging.getLogger(__name__)

# ── Retry / reconciler tunables ──
RETRIES_PER_ROUND = 3          # attempts per participant in one dissemination round
INITIAL_RETRY_DELAY = 0.5      # seconds
MAX_RETRY_DELAY = 30.0         # cap for exponential back-off
RECONCILE_INTERVAL = 15        # seconds between reconciler sweeps


class TxStatus(str, Enum):
    COMMIT_DECIDED = "COMMIT_DECIDED"   # durable commit decision, dissemination in progress
    ABORT_DECIDED  = "ABORT_DECIDED"    # durable abort decision, dissemination in progress
    COMMITTED      = "COMMITTED"        # terminal – all commit ACKs received
    ABORTED        = "ABORTED"          # terminal – all abort ACKs received


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
class OrchestratorResult:
    transaction_id: str
    success:        bool
    error:          str | None = None

    @classmethod
    def ok(cls, transaction_id: str) -> "OrchestratorResult":
        return cls(transaction_id=transaction_id, success=True)

    @classmethod
    def failure(cls, transaction_id: str, error: str) -> "OrchestratorResult":
        return cls(transaction_id=transaction_id, success=False, error=error)


class Orchestrator:
    """Durable, reusable 2PC orchestrator.

    Correctness invariants
    ----------------------
    1. COMMIT_DECIDED is force-written **before** any commit message is sent.
    2. ABORT_DECIDED is force-written **before** any abort message is sent
       (when at least one participant voted YES and needs explicit abort).
    3. Per-participant ACKs are tracked; the background reconciler retries
       indefinitely until every participant has ACK'd.
    4. On restart the reconciler resumes all incomplete txns automatically.
    5. If no durable decision exists for a transaction, the inquiry protocol
       returns ABORTED (presumed-abort rule).
    """

    def __init__(
        self,
        db_pool,
        timeout: int = 10,
        transaction_table: str = "orchestrator_transactions",
        participant_table: str = "orchestrator_tx_participants",
        on_commit_decided: Callable[[Any, str], None] | None = None,
    ):
        self.db_pool = db_pool
        self.timeout = timeout
        self.transaction_table = transaction_table
        self.participant_table = participant_table
        self.on_commit_decided = on_commit_decided

    # ── public API ─────────────────────────────────────────────────────────

    def run(self, business_id: str, participants: list[Participant]) -> OrchestratorResult:
        """Drive a full 2PC round. Never raises – returns OrchestratorResult."""
        transaction_id = str(uuid.uuid4())
        logger.debug("2PC START  tx=%s  business_id=%s", transaction_id, business_id)

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

        # ── Decision ──

        if PrepareVote.NO in votes.values():
            # === ABORT path ===
            failed = [n for n, v in votes.items() if v == PrepareVote.NO]
            logger.warning("2PC ABORT  tx=%s  vote=NO from %s", transaction_id, failed)

            if prepared:
                # Durably persist ABORT_DECIDED *before* sending any abort msgs
                if self._persist_decision(
                    transaction_id, business_id, TxStatus.ABORT_DECIDED, prepared
                ):
                    self._disseminate_aborts(transaction_id, prepared)
                else:
                    # DB unreachable – best-effort aborts; participants will
                    # resolve via inquiry (presumed abort) eventually.
                    self._best_effort_abort(transaction_id, prepared)

            return OrchestratorResult.failure(
                transaction_id, f"Prepare failed for: {failed}"
            )

        # All voted YES (or READ_ONLY)
        to_commit = [p for p in participants if votes.get(p.name) == PrepareVote.YES]

        if not to_commit:
            # All read-only – nothing to persist or commit
            return OrchestratorResult.ok(transaction_id)

        # === COMMIT path ===
        # Force-write COMMIT_DECIDED + participant list (+ optional commit hook)
        if not self._persist_decision(
            transaction_id, business_id, TxStatus.COMMIT_DECIDED, to_commit
        ):
            # Cannot persist commit → must abort
            # Try to durably record abort; if that also fails, best-effort.
            if self._persist_decision(
                transaction_id, business_id, TxStatus.ABORT_DECIDED, to_commit
            ):
                self._disseminate_aborts(transaction_id, to_commit)
            else:
                self._best_effort_abort(transaction_id, to_commit)
            return OrchestratorResult.failure(
                transaction_id, "Failed to persist commit decision"
            )

        logger.debug("2PC COMMIT_DECIDED  tx=%s", transaction_id)

        # ── Phase 2: Disseminate commits ──
        # Best-effort in request thread; reconciler guarantees eventual delivery.
        self._disseminate_commits(transaction_id, to_commit)

        logger.debug("2PC COMMITTED  tx=%s", transaction_id)
        return OrchestratorResult.ok(transaction_id)

    def get_transaction_status(self, transaction_id: str) -> str:
        """Inquiry protocol – participants poll this to resolve uncertain txns.

        Returns ``'COMMITTED'`` when the durable decision is commit,
        ``'ABORTED'`` otherwise (including unknown txns → presumed abort).
        """
        try:
            with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("SELECT status FROM {} WHERE transaction_id = %s").format(
                            sql.Identifier(self.transaction_table)
                        ),
                        (transaction_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        s = row[0]
                        if s in (TxStatus.COMMIT_DECIDED.value, TxStatus.COMMITTED.value):
                            return "COMMITTED"
                        if s in (TxStatus.ABORT_DECIDED.value, TxStatus.ABORTED.value):
                            return "ABORTED"
        except psycopg.Error as exc:
            logger.error("Inquiry error  tx=%s  exc=%s", transaction_id, exc)
        # Presumed abort: no record → ABORTED
        return "ABORTED"

    def reconcile(
        self,
        participant_factory: Callable[[str, str], Participant],
    ) -> int:
        """Resume every incomplete txn.  Called on startup **and** periodically.

        ``participant_factory(transaction_id, participant_name) -> Participant``
        must reconstruct a :class:`Participant` from just the name (the
        orchestrator only persists participant names, not full URLs, because
        those depend on the runtime ``GATEWAY_URL``).

        Returns the count of transactions that still have un-ACK'd
        participants (i.e. need further rounds).
        """
        remaining = 0

        # ── COMMIT_DECIDED with un-ACK'd participants ──
        remaining += self._reconcile_status(
            TxStatus.COMMIT_DECIDED,
            TxStatus.COMMITTED,
            self._disseminate_commits,
            participant_factory,
        )

        # ── ABORT_DECIDED with un-ACK'd participants ──
        remaining += self._reconcile_status(
            TxStatus.ABORT_DECIDED,
            TxStatus.ABORTED,
            self._disseminate_aborts,
            participant_factory,
        )

        return remaining

    # ── internal: Phase-1 ──────────────────────────────────────────────────

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
            body = r.json() if r.content else {}
            if body.get("read_only", False):
                logger.debug("PREPARE read-only  participant=%s", p.name)
                return PrepareVote.READ_ONLY
            return PrepareVote.YES
        except requests.exceptions.RequestException as exc:
            logger.error("PREPARE error  participant=%s  exc=%s", p.name, exc)
            return PrepareVote.NO

    # ── internal: Phase-2 dissemination ────────────────────────────────────

    def _disseminate_commits(
        self, transaction_id: str, participants: list[Participant]
    ) -> None:
        """Send commit to each participant; mark ACK on success.

        If every participant ACKs, the txn is finalised to COMMITTED.
        Un-ACK'd participants will be retried by the reconciler.
        """
        if not participants:
            return
        self._disseminate(
            transaction_id, participants, self._send_commit, TxStatus.COMMITTED
        )

    def _disseminate_aborts(
        self, transaction_id: str, participants: list[Participant]
    ) -> None:
        """Send abort to each participant; mark ACK on success."""
        if not participants:
            return
        self._disseminate(
            transaction_id, participants, self._send_abort, TxStatus.ABORTED
        )

    def _disseminate(
        self,
        transaction_id: str,
        participants: list[Participant],
        send_fn: Callable[[str, Participant], bool],
        final_status: TxStatus,
    ) -> None:
        with ThreadPoolExecutor(max_workers=len(participants)) as pool:
            futures = {
                pool.submit(send_fn, transaction_id, p): p
                for p in participants
            }
            all_acked = True
            for future in as_completed(futures):
                p = futures[future]
                if future.result():
                    self._mark_acked(transaction_id, p.name)
                else:
                    all_acked = False
        if all_acked:
            self._finalize_tx(transaction_id, final_status)

    def _send_commit(self, transaction_id: str, p: Participant) -> bool:
        """Send commit with a bounded number of retries for this round."""
        return self._send_with_retries(
            transaction_id, p, p.commit_url, "COMMIT"
        )

    def _send_abort(self, transaction_id: str, p: Participant) -> bool:
        """Send abort with a bounded number of retries for this round."""
        return self._send_with_retries(
            transaction_id, p, p.abort_url, "ABORT"
        )

    def _send_with_retries(
        self, transaction_id: str, p: Participant, url_template: str, label: str
    ) -> bool:
        delay = INITIAL_RETRY_DELAY
        for attempt in range(1, RETRIES_PER_ROUND + 1):
            try:
                r = requests.post(
                    p.url(url_template, transaction_id), timeout=self.timeout
                )
                if r.status_code == 200:
                    return True
                logger.warning(
                    "%s fail  participant=%s  status=%s  attempt=%d",
                    label, p.name, r.status_code, attempt,
                )
            except requests.exceptions.RequestException as exc:
                logger.warning(
                    "%s error  participant=%s  exc=%s  attempt=%d",
                    label, p.name, exc, attempt,
                )
            if attempt < RETRIES_PER_ROUND:
                time.sleep(delay)
                delay = min(delay * 2, MAX_RETRY_DELAY)
        return False

    def _best_effort_abort(
        self, transaction_id: str, participants: list[Participant]
    ) -> None:
        """Fire-and-forget aborts when no durable state could be written."""
        if not participants:
            return
        with ThreadPoolExecutor(max_workers=len(participants)) as pool:
            futs = [
                pool.submit(self._send_abort, transaction_id, p)
                for p in participants
            ]
            for f in as_completed(futs):
                f.result()

    # ── internal: reconciler ───────────────────────────────────────────────

    def _reconcile_status(
        self,
        in_progress_status: TxStatus,
        terminal_status: TxStatus,
        disseminate_fn: Callable[[str, list[Participant]], None],
        participant_factory: Callable[[str, str], Participant],
    ) -> int:
        remaining = 0
        try:
            with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL(
                            """
                            SELECT t.transaction_id, p.participant_name
                            FROM {transactions} t
                            JOIN {participants} p
                              ON t.transaction_id = p.transaction_id
                            WHERE t.status = %s AND p.acked = FALSE
                            """
                        ).format(
                            transactions=sql.Identifier(self.transaction_table),
                            participants=sql.Identifier(self.participant_table),
                        ),
                        (in_progress_status.value,),
                    )
                    rows = cur.fetchall()

            tx_parts: dict[str, list[str]] = defaultdict(list)
            for tx_id, p_name in rows:
                tx_parts[tx_id].append(p_name)

            for tx_id, names in tx_parts.items():
                parts: list[Participant] = []
                for name in names:
                    try:
                        parts.append(participant_factory(tx_id, name))
                    except Exception as exc:
                        logger.error(
                            "Cannot build participant %s for tx=%s: %s",
                            name, tx_id, exc,
                        )
                if parts:
                    disseminate_fn(tx_id, parts)
                    # Check if all are now ACK'd
                    if not self._all_acked(tx_id):
                        remaining += 1

        except psycopg.Error as exc:
            logger.error("Reconciler DB error: %s", exc)

        return remaining

    # ── internal: persistence ──────────────────────────────────────────────

    def _persist_decision(
        self,
        transaction_id: str,
        business_id: str,
        status: TxStatus,
        participants: list[Participant],
    ) -> bool:
        """Atomically persist decision + participant list.

        Optional business side-effects can be injected via ``on_commit_decided``.
        """
        try:
            with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL(
                            """
                            INSERT INTO {transactions}
                                   (transaction_id, business_id, status)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (transaction_id)
                            DO UPDATE SET status = EXCLUDED.status
                            """
                        ).format(transactions=sql.Identifier(self.transaction_table)),
                        (transaction_id, business_id, status.value),
                        prepare=False,
                    )
                    for p in participants:
                        cur.execute(
                            sql.SQL(
                                """
                                INSERT INTO {participants}
                                       (transaction_id, participant_name, acked)
                                VALUES (%s, %s, FALSE)
                                ON CONFLICT (transaction_id, participant_name)
                                DO NOTHING
                                """
                            ).format(participants=sql.Identifier(self.participant_table)),
                            (transaction_id, p.name),
                        )
                    if status == TxStatus.COMMIT_DECIDED and self.on_commit_decided:
                        self.on_commit_decided(cur, business_id)
                    conn.commit()
            return True
        except psycopg.Error as exc:
            logger.error(
                "Failed to persist decision  tx=%s  status=%s  exc=%s",
                transaction_id, status, exc,
            )
            return False

    def _mark_acked(self, transaction_id: str, participant_name: str) -> None:
        try:
            with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL(
                            """
                            UPDATE {participants} SET acked = TRUE
                            WHERE transaction_id = %s AND participant_name = %s
                            """
                        ).format(participants=sql.Identifier(self.participant_table)),
                        (transaction_id, participant_name),
                    )
                    conn.commit()
        except psycopg.Error as exc:
            logger.error(
                "Failed to mark ACK  tx=%s  p=%s  exc=%s",
                transaction_id, participant_name, exc,
            )

    def _all_acked(self, transaction_id: str) -> bool:
        try:
            with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL(
                            """
                            SELECT COUNT(*) FROM {participants}
                            WHERE transaction_id = %s AND acked = FALSE
                            """
                        ).format(participants=sql.Identifier(self.participant_table)),
                        (transaction_id,),
                    )
                    return cur.fetchone()[0] == 0
        except psycopg.Error:
            return False

    def _finalize_tx(self, transaction_id: str, final_status: TxStatus) -> None:
        """Advance to terminal state once every participant has ACK'd."""
        try:
            with self.db_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        sql.SQL("UPDATE {} SET status = %s WHERE transaction_id = %s").format(
                            sql.Identifier(self.transaction_table)
                        ),
                        (final_status.value, transaction_id),
                    )
                    conn.commit()
        except psycopg.Error as exc:
            logger.error(
                "Failed to finalize  tx=%s  status=%s  exc=%s",
                transaction_id, final_status, exc,
            )


# Backward-compatible aliases for existing imports.
TwoPhaseCommitCoordinator = Orchestrator
CoordinatorResult = OrchestratorResult