import logging
import os
import atexit
import uuid
import json

import psycopg
from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row
from msgspec import Struct
from flask import Flask, jsonify, abort, Response, request

from kafka_service import kafka_client
from kafka_service.kafka_event import BaseEvent, CheckoutPayload
from kafka_service.consumer_handler import EventConsumer

DB_ERROR_STR = "DB error"

app = Flask("payment-service")
service_name = "payment"

order_kafka = kafka_client.Client(
    service_name,
    ['order.events', 'stock.events', 'payment.events']
)
kafka_producer, kafka_consumer = order_kafka.producer, order_kafka.consumer

# DB pool


db_pool = ConnectionPool(
    conninfo=(
        f"host={os.environ['POSTGRES_HOST']} "
        f"port={os.environ['POSTGRES_PORT']} "
        f"user={os.environ['POSTGRES_USER']} "
        f"password={os.environ['POSTGRES_PASSWORD']} "
        f"dbname={os.environ['POSTGRES_DB']}"
    ),
    min_size=1,
    max_size=10,
    reconnect_timeout=30,
    kwargs={"connect_timeout": 10}
)

event_consumer = EventConsumer(kafka_consumer, db_pool, service_name)


# DB initialization

def init_db():
    with db_pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT    PRIMARY KEY,
                    credit  INTEGER NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS payment_transactions (
                    transaction_id TEXT    PRIMARY KEY,
                    order_id       TEXT    NOT NULL,
                    user_id        TEXT    NOT NULL,
                    amount         INTEGER NOT NULL,
                    status         TEXT    NOT NULL DEFAULT 'PREPARED',
                    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS credit_holds (
                    hold_id        TEXT    PRIMARY KEY,
                    transaction_id TEXT    NOT NULL,
                    user_id        TEXT    NOT NULL,
                    amount         INTEGER NOT NULL,
                    FOREIGN KEY (transaction_id)
                        REFERENCES payment_transactions(transaction_id) ON DELETE CASCADE
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS event_log (
                    correlation_id TEXT   PRIMARY KEY,
                    event_type     TEXT   NOT NULL,
                    service        TEXT   NOT NULL,
                    topic          TEXT   NOT NULL,
                    kafka_offset   BIGINT NOT NULL,
                    payload        JSONB  NOT NULL,
                    received_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()


def close_db_connection():
    db_pool.close()


init_db()
atexit.register(close_db_connection)
def consume_messages():
    event_consumer._run()



class UserValue(Struct):
    credit: int


def get_user_from_db(user_id: str) -> UserValue:
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT credit FROM users WHERE user_id = %s", (user_id,))
                row = cur.fetchone()
    except psycopg.Error:
        abort(400, DB_ERROR_STR)
    if row is None:
        abort(400, f"User: {user_id} not found!")
    return UserValue(credit=row['credit'])



@app.post('/create_user')
def create_user():
    key = str(uuid.uuid4())
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO users (user_id, credit) VALUES (%s, %s)", (key, 0))
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({'user_id': key})


@app.post('/batch_init/<n>/<starting_money>')
def batch_init_users(n: int, starting_money: int):
    n, starting_money = int(n), int(starting_money)
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    "INSERT INTO users (user_id, credit) VALUES (%s, %s)",
                    [(f"{i}", starting_money) for i in range(n)]
                )
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return jsonify({"msg": "Batch init for users successful"})


@app.get('/find_user/<user_id>')
def find_user(user_id: str):
    user = get_user_from_db(user_id)
    return jsonify({"user_id": user_id, "credit": user.credit})


@app.post('/add_funds/<user_id>/<amount>')
def add_credit(user_id: str, amount: int):
    user_entry = get_user_from_db(user_id)
    user_entry.credit += int(amount)
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET credit = %s WHERE user_id = %s",
                            (user_entry.credit, user_id))
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)


@app.post('/pay/<user_id>/<amount>')
def remove_credit(user_id: str, amount: int):
    user_entry = get_user_from_db(user_id)
    user_entry.credit -= int(amount)
    if user_entry.credit < 0:
        abort(400, f"User: {user_id} credit cannot get reduced below zero!")
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE users SET credit = %s WHERE user_id = %s",
                            (user_entry.credit, user_id))
    except psycopg.Error:
        return abort(400, DB_ERROR_STR)
    return Response(f"User: {user_id} credit updated to: {user_entry.credit}", status=200)



# 2PC endpoints

@app.post('/prepare/<transaction_id>')
def prepare_payment(transaction_id: str):
    try:
        data     = request.get_json() or {}
        order_id = data.get('order_id')
        user_id  = data.get('user_id')
        amount   = int(data.get('amount', 0))
        if not order_id or not user_id or amount <= 0:
            abort(400, "Missing order_id, user_id or invalid amount")

        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT credit FROM users WHERE user_id = %s", (user_id,))
                row = cur.fetchone()
                if row is None:
                    abort(400, f"User {user_id} not found")
                current_credit = row['credit']
                cur.execute("""
                        SELECT SUM(amount) as held FROM credit_holds WHERE user_id = %s
                    """, (user_id,))
                held_row = cur.fetchone()
                held_credit = held_row['held'] if held_row['held'] is not None else 0
                if current_credit - held_credit < amount:
                    abort(400, f"Insufficient credit for user {user_id}")

                cur.execute(
                    "INSERT INTO payment_transactions (transaction_id, order_id, user_id, amount, status) VALUES (%s, %s, %s, %s, %s)",
                    (transaction_id, order_id, user_id, amount, 'PREPARED')
                )
                cur.execute(
                    "INSERT INTO credit_holds (hold_id, transaction_id, user_id, amount) VALUES (%s, %s, %s, %s)",
                    (f"{transaction_id}_hold", transaction_id, user_id, amount)
                )
                conn.commit()

        return jsonify({'status': 'PREPARED', 'transaction_id': transaction_id}), 200
    except psycopg.Error as e:
        abort(400, f"DB error in prepare: {str(e)}")


@app.post('/commit/<transaction_id>')
def commit_payment(transaction_id: str):
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM payment_transactions WHERE transaction_id = %s",
                            (transaction_id,))
                trans = cur.fetchone()
                if trans is None:
                    abort(400, f"Transaction {transaction_id} not found")
                cur.execute("UPDATE users SET credit = credit - %s WHERE user_id = %s",
                            (trans['amount'], trans['user_id']))
                cur.execute("DELETE FROM credit_holds WHERE transaction_id = %s", (transaction_id,))
                cur.execute("UPDATE payment_transactions SET status = 'COMMITTED' WHERE transaction_id = %s",
                            (transaction_id,))
                conn.commit()
        return jsonify({'status': 'COMMITTED', 'transaction_id': transaction_id}), 200
    except psycopg.Error as e:
        abort(400, f"DB error in commit: {str(e)}")


@app.post('/abort/<transaction_id>')
def abort_payment(transaction_id: str):
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM credit_holds WHERE transaction_id = %s", (transaction_id,))
                cur.execute("UPDATE payment_transactions SET status = 'ABORTED' WHERE transaction_id = %s",
                            (transaction_id,))
                conn.commit()
        return jsonify({'status': 'ABORTED', 'transaction_id': transaction_id}), 200
    except psycopg.Error as e:
        abort(400, f"DB error in abort: {str(e)}")


# ── Cooperative Termination: expose local tx status ──

@app.get('/transaction/<transaction_id>/status')
def transaction_status(transaction_id: str):
    """Other participants can query this to resolve uncertain transactions."""
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT status FROM payment_transactions WHERE transaction_id = %s",
                            (transaction_id,))
                row = cur.fetchone()
                if row:
                    return jsonify({'transaction_id': transaction_id, 'status': row['status']}), 200
                return jsonify({'transaction_id': transaction_id, 'status': 'UNKNOWN'}), 200
    except psycopg.Error:
        return jsonify({'transaction_id': transaction_id, 'status': 'UNKNOWN'}), 200


# ── Stale Transaction Recovery Sweep ──

def _recovery_commit(transaction_id: str) -> None:
    """Apply a commit for a stale PREPARED transaction."""
    try:
        with db_pool.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM payment_transactions WHERE transaction_id = %s",
                            (transaction_id,))
                tx = cur.fetchone()
                if tx is None or tx['status'] != 'PREPARED':
                    return
                cur.execute("UPDATE users SET credit = credit - %s WHERE user_id = %s",
                            (tx['amount'], tx['user_id']))
                cur.execute("DELETE FROM credit_holds WHERE transaction_id = %s", (transaction_id,))
                cur.execute("UPDATE payment_transactions SET status = 'COMMITTED' WHERE transaction_id = %s",
                            (transaction_id,))
                conn.commit()
        logging.info("RECOVERY committed  tx=%s", transaction_id)
    except psycopg.Error as exc:
        logging.error("RECOVERY commit error  tx=%s  exc=%s", transaction_id, exc)


def _recovery_abort(transaction_id: str) -> None:
    """Abort a stale PREPARED transaction."""
    try:
        with db_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM credit_holds WHERE transaction_id = %s",
                            (transaction_id,))
                cur.execute("UPDATE payment_transactions SET status = 'ABORTED' WHERE transaction_id = %s",
                            (transaction_id,))
                conn.commit()
        logging.info("RECOVERY aborted  tx=%s", transaction_id)
    except psycopg.Error as exc:
        logging.error("RECOVERY abort error  tx=%s  exc=%s", transaction_id, exc)


def _resolve_via_peers(transaction_id: str) -> str:
    """Cooperative termination: ask coordinator first, then sibling participant."""
    for url in [
        f"{GATEWAY_URL}/orders/transaction/{transaction_id}/status",
        f"{GATEWAY_URL}/stock/transaction/{transaction_id}/status",
    ]:
        try:
            r = requests.get(url, timeout=5)
            if r.status_code == 200:
                status = r.json().get('status', 'UNKNOWN')
                if status in ('COMMITTED', 'ABORTED'):
                    return status
        except requests.exceptions.RequestException:
            continue
    return 'UNKNOWN'


def _recovery_sweep() -> None:
    """Background thread: resolve stale PREPARED transactions."""
    while True:
        time.sleep(RECOVERY_INTERVAL_SECS)
        if not GATEWAY_URL:
            continue
        try:
            with db_pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    cur.execute("""
                        SELECT transaction_id
                        FROM payment_transactions
                        WHERE status = 'PREPARED'
                          AND created_at < NOW() - %s * INTERVAL '1 second'
                    """, (STALE_THRESHOLD_SECS,))
                    stale = cur.fetchall()
            for tx in stale:
                tid = tx['transaction_id']
                outcome = _resolve_via_peers(tid)
                if outcome == 'COMMITTED':
                    _recovery_commit(tid)
                elif outcome == 'ABORTED':
                    _recovery_abort(tid)
        except Exception as exc:
            logging.error("Payment recovery sweep error: %s", exc)


if GATEWAY_URL:
    threading.Thread(target=_recovery_sweep, daemon=True, name="payment-recovery").start()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    logging.root.handlers = gunicorn_logger.handlers
    logging.root.setLevel(gunicorn_logger.level)
