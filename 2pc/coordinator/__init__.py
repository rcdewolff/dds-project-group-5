from .app import TwoPhaseCommitCoordinator, Participant, CoordinatorResult, RECONCILE_INTERVAL

def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS order_transactions (
                transaction_id TEXT PRIMARY KEY,
                order_id       TEXT NOT NULL,
                status         TEXT NOT NULL,
                created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tx_participants (
                transaction_id   TEXT    NOT NULL,
                participant_name TEXT    NOT NULL,
                acked            BOOLEAN NOT NULL DEFAULT FALSE,
                PRIMARY KEY (transaction_id, participant_name)
            )
        """)
        conn.commit()