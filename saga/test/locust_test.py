import time
from typing import Any

from locust import HttpUser, between, task


def _eventually(fetch_fn, expected: int, timeout: float = 5.0, interval: float = 0.1) -> int:
    """Poll helper for eventually-consistent saga side effects."""
    deadline = time.time() + timeout
    last_value = -1
    while time.time() < deadline:
        last_value = int(fetch_fn())
        if last_value == expected:
            return last_value
        time.sleep(interval)
    return last_value


class CheckoutConsistencyUser(HttpUser):
    """
    Concurrent saga workload that verifies consistency invariants:
    - Failed checkout: stock unchanged, credit unchanged.
    - Successful checkout: stock decremented, credit decremented.
    """

    wait_time = between(0.1, 0.5)
    item_price = 5
    quantity = 1

    def _must_json(self, response, required_key: str) -> dict[str, Any]:
        if response.status_code >= 400:
            raise RuntimeError(f"HTTP {response.status_code} for {response.request.method} {response.request.path_url}: {response.text}")
        payload = response.json()
        if required_key not in payload:
            raise RuntimeError(f"Missing key '{required_key}' in response: {payload}")
        return payload

    def _create_user(self) -> str:
        body = self._must_json(self.client.post("/payment/create_user"), "user_id")
        return str(body["user_id"])

    def _find_user_credit(self, user_id: str) -> int:
        body = self._must_json(self.client.get(f"/payment/find_user/{user_id}"), "credit")
        return int(body["credit"])

    def _add_credit(self, user_id: str, amount: int) -> None:
        response = self.client.post(f"/payment/add_funds/{user_id}/{amount}")
        if response.status_code >= 400:
            raise RuntimeError(f"Failed to add credit: status={response.status_code}, body={response.text}")

    def _create_order(self, user_id: str) -> str:
        body = self._must_json(self.client.post(f"/orders/create/{user_id}"), "order_id")
        return str(body["order_id"])

    def _create_item(self, price: int) -> str:
        body = self._must_json(self.client.post(f"/stock/item/create/{price}"), "item_id")
        return str(body["item_id"])

    def _find_item_stock(self, item_id: str) -> int:
        body = self._must_json(self.client.get(f"/stock/find/{item_id}"), "stock")
        return int(body["stock"])

    def _add_stock(self, item_id: str, amount: int) -> None:
        response = self.client.post(f"/stock/add/{item_id}/{amount}")
        if response.status_code >= 400:
            raise RuntimeError(f"Failed to add stock: status={response.status_code}, body={response.text}")

    def _add_item_to_order(self, order_id: str, item_id: str, quantity: int) -> None:
        response = self.client.post(f"/orders/addItem/{order_id}/{item_id}/{quantity}")
        if response.status_code >= 400:
            raise RuntimeError(f"Failed to add item to order: status={response.status_code}, body={response.text}")

    @task
    def checkout_consistency(self):
        user_id = self._create_user()
        order_id = self._create_order(user_id)
        item_id = self._create_item(self.item_price)

        self._add_stock(item_id, 10)
        self._add_item_to_order(order_id, item_id, self.quantity)

        stock_before = self._find_item_stock(item_id)
        credit_before = self._find_user_credit(user_id)
        order_total = self.item_price * self.quantity

        # Alternate expected outcome by current timestamp to keep a mixed workload.
        should_succeed = int(time.time() * 1000) % 2 == 0
        if should_succeed and credit_before < order_total:
            self._add_credit(user_id, order_total - credit_before)
            credit_before = self._find_user_credit(user_id)

        with self.client.post(f"/orders/checkout/{order_id}", catch_response=True) as checkout:
            if should_succeed:
                if checkout.status_code >= 400:
                    checkout.failure(
                        f"Expected successful checkout, got status={checkout.status_code}, body={checkout.text}"
                    )
                    return
                checkout.success()
            else:
                if checkout.status_code < 400:
                    checkout.failure(
                        f"Expected failed checkout, got status={checkout.status_code}, body={checkout.text}"
                    )
                    return
                # 4xx is expected here (e.g., insufficient credit / saga fail), so don't count it as HTTP failure.
                checkout.success()

        if should_succeed:
            expected_stock = stock_before - self.quantity
            expected_credit = credit_before - order_total

            stock_after = _eventually(lambda: self._find_item_stock(item_id), expected_stock)
            credit_after = _eventually(lambda: self._find_user_credit(user_id), expected_credit)

            if stock_after != expected_stock:
                raise RuntimeError(
                    f"Stock inconsistency after successful checkout for order {order_id}: expected {expected_stock}, got {stock_after}"
                )
            if credit_after != expected_credit:
                raise RuntimeError(
                    f"Credit inconsistency after successful checkout for order {order_id}: expected {expected_credit}, got {credit_after}"
                )
        else:
            expected_stock = stock_before
            expected_credit = credit_before

            stock_after = _eventually(lambda: self._find_item_stock(item_id), expected_stock)
            credit_after = _eventually(lambda: self._find_user_credit(user_id), expected_credit)

            if stock_after != expected_stock:
                raise RuntimeError(
                    f"Stock inconsistency after failed checkout for order {order_id}: expected {expected_stock}, got {stock_after}"
                )
            if credit_after != expected_credit:
                raise RuntimeError(
                    f"Credit inconsistency after failed checkout for order {order_id}: expected {expected_credit}, got {credit_after}"
                )

            