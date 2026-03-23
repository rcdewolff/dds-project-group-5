import os
import sys
from pathlib import Path

import requests
from kafka import KafkaAdminClient

import utils as tu


ROOT = Path(__file__).resolve().parents[1]
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EXPECTED_TOPICS = {"stock.request", "payment.request", "order.request"}
EXPECTED_PARTITIONS = 6


def check_topics() -> None:
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP)
    meta = admin.describe_topics(list(EXPECTED_TOPICS))
    admin.close()

    for topic_meta in meta:
        topic_name = topic_meta["topic"]
        partitions = topic_meta["partitions"]
        if topic_name not in EXPECTED_TOPICS:
            continue
        if len(partitions) != EXPECTED_PARTITIONS:
            raise AssertionError(f"Topic {topic_name} has {len(partitions)} partitions, expected {EXPECTED_PARTITIONS}")


def check_gateway_checkout_routing() -> None:
    response = requests.post("http://127.0.0.1:8000/orders/checkout/does-not-exist", timeout=5)
    if response.status_code not in (400, 404):
        raise AssertionError(f"Unexpected status for routed checkout call: {response.status_code}, body={response.text}")


def check_checkout_success_and_failure() -> None:
    user = tu.create_user()
    user_id = user["user_id"]
    add_credit_status = tu.add_credit_to_user(user_id, 50)
    if add_credit_status < 200 or add_credit_status >= 300:
        raise AssertionError("Failed to add test credit")

    order = tu.create_order(user_id)
    order_id = order["order_id"]

    item = tu.create_item(5)
    item_id = item["item_id"]
    tu.add_stock(item_id, 10)
    tu.add_item_to_order(order_id, item_id, 2)

    success_resp = tu.checkout_order(order_id)
    if success_resp.status_code != 200:
        raise AssertionError(f"Expected successful checkout 200, got {success_resp.status_code} body={success_resp.text}")

    order2 = tu.create_order(user_id)
    order2_id = order2["order_id"]
    tu.add_item_to_order(order2_id, item_id, 1000)
    fail_resp = tu.checkout_order(order2_id)
    if fail_resp.status_code < 400:
        raise AssertionError(f"Expected failed checkout, got {fail_resp.status_code} body={fail_resp.text}")


def check_redis_not_used_for_saga_critical_paths() -> None:
    forbidden = []
    for py_file in ROOT.rglob("*.py"):
        if "order/orchestrator_async.py" in py_file.as_posix():
            continue
        text = py_file.read_text(encoding="utf-8", errors="ignore")
        if "redis_client.publish(" in text and "order/consumer.py" in py_file.as_posix():
            forbidden.append(str(py_file))
    if forbidden:
        raise AssertionError(f"Redis publish still found in saga-critical consumer path: {forbidden}")


if __name__ == "__main__":
    try:
        check_topics()
        check_gateway_checkout_routing()
        check_checkout_success_and_failure()
        check_redis_not_used_for_saga_critical_paths()
    except Exception as exc:
        print(f"Validation failed: {exc}")
        sys.exit(1)

    print("Validation checks passed.")
