from .app import (
    Orchestrator,
    OrchestratorResult,
    Participant,
    RECONCILE_INTERVAL,
    TwoPhaseCommitCoordinator,
    CoordinatorResult,
)

def create_tables(
    conn,
    transaction_table: str = "orchestrator_transactions",
    participant_table: str = "orchestrator_tx_participants",
):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {transaction_table} (
                transaction_id TEXT PRIMARY KEY,
                business_id    TEXT NOT NULL,
                status         TEXT NOT NULL,
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {participant_table} (
                transaction_id   TEXT    NOT NULL,
                participant_name TEXT    NOT NULL,
                acked            BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (transaction_id, participant_name)
            )
        """)
        conn.commit()