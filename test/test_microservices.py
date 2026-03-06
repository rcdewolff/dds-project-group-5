import unittest
import utils as tu


class TestMicroservices(unittest.TestCase):

    # ------------------------------------------------------------------
    # Stock
    # ------------------------------------------------------------------

    def test_stock(self):
        # Create item
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item, f"create_item(5) did not return item_id. Got: {item}")
        item_id: str = item['item_id']

        # Find item
        item = tu.find_item(item_id)
        self.assertEqual(item['price'], 5,
            f"Expected price=5, got {item['price']}. Full item: {item}")
        self.assertEqual(item['stock'], 0,
            f"Expected stock=0 after creation, got {item['stock']}. Full item: {item}")

        # Add stock
        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(tu.status_code_is_success(int(add_stock_response)),
            f"add_stock({item_id}, 50) failed with status {add_stock_response}")

        stock_after_add = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_add, 50,
            f"Expected stock=50 after adding 50, got {stock_after_add}")

        # Over-subtract (should fail)
        over_subtract_response = tu.subtract_stock(item_id, 200)
        self.assertTrue(tu.status_code_is_failure(int(over_subtract_response)),
            f"subtract_stock({item_id}, 200) should have failed but got status {over_subtract_response}")

        # Normal subtract
        subtract_response = tu.subtract_stock(item_id, 15)
        self.assertTrue(tu.status_code_is_success(int(subtract_response)),
            f"subtract_stock({item_id}, 15) failed with status {subtract_response}")

        stock_after_subtract = tu.find_item(item_id)['stock']
        self.assertEqual(stock_after_subtract, 35,
            f"Expected stock=35 after subtracting 15 from 50, got {stock_after_subtract}")

    # ------------------------------------------------------------------
    # Payment
    # ------------------------------------------------------------------

    def test_payment(self):
        # Create user
        user: dict = tu.create_user()
        self.assertIn('user_id', user, f"create_user() did not return user_id. Got: {user}")
        user_id: str = user['user_id']

        # Add credit
        add_credit_response = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(add_credit_response),
            f"add_credit_to_user({user_id}, 15) failed with status {add_credit_response}")

        # Create item and stock
        item: dict = tu.create_item(5)
        self.assertIn('item_id', item, f"create_item(5) did not return item_id. Got: {item}")
        item_id: str = item['item_id']

        add_stock_response = tu.add_stock(item_id, 50)
        self.assertTrue(tu.status_code_is_success(add_stock_response),
            f"add_stock({item_id}, 50) failed with status {add_stock_response}")

        # Create order and add items
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order, f"create_order({user_id}) did not return order_id. Got: {order}")
        order_id: str = order['order_id']

        for i in range(3):
            resp = tu.add_item_to_order(order_id, item_id, 1)
            self.assertTrue(tu.status_code_is_success(resp),
                f"add_item_to_order attempt {i+1} failed with status {resp} "
                f"(order_id={order_id}, item_id={item_id})")

        # Pay
        payment_response = tu.payment_pay(user_id, 10)
        self.assertTrue(tu.status_code_is_success(payment_response),
            f"payment_pay({user_id}, 10) failed with status {payment_response}")

        credit_after_payment = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after_payment, 5,
            f"Expected credit=5 after paying 10 from 15, got {credit_after_payment}")

    # ------------------------------------------------------------------
    # Order / Checkout saga
    # ------------------------------------------------------------------

    def test_order(self):
        # Create user
        user: dict = tu.create_user()
        self.assertIn('user_id', user, f"create_user() did not return user_id. Got: {user}")
        user_id: str = user['user_id']

        # Create order
        order: dict = tu.create_order(user_id)
        self.assertIn('order_id', order, f"create_order({user_id}) did not return order_id. Got: {order}")
        order_id: str = order['order_id']

        # Create item 1 with stock 15
        item1: dict = tu.create_item(5)
        self.assertIn('item_id', item1, f"create_item(5) for item1 failed. Got: {item1}")
        item_id1: str = item1['item_id']
        resp = tu.add_stock(item_id1, 15)
        self.assertTrue(tu.status_code_is_success(resp),
            f"add_stock(item1={item_id1}, 15) failed with status {resp}")

        # Create item 2 with stock 1 (will be exhausted to test failure)
        item2: dict = tu.create_item(5)
        self.assertIn('item_id', item2, f"create_item(5) for item2 failed. Got: {item2}")
        item_id2: str = item2['item_id']
        resp = tu.add_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(resp),
            f"add_stock(item2={item_id2}, 1) failed with status {resp}")

        # Add both items to order
        resp = tu.add_item_to_order(order_id, item_id1, 1)
        self.assertTrue(tu.status_code_is_success(resp),
            f"add_item_to_order(order={order_id}, item1={item_id1}) failed with status {resp}")
        resp = tu.add_item_to_order(order_id, item_id2, 1)
        self.assertTrue(tu.status_code_is_success(resp),
            f"add_item_to_order(order={order_id}, item2={item_id2}) failed with status {resp}")

        # Exhaust item2 stock → checkout should fail (stock unavailable)
        resp = tu.subtract_stock(item_id2, 1)
        self.assertTrue(tu.status_code_is_success(resp),
            f"subtract_stock(item2={item_id2}, 1) failed with status {resp}")

        checkout_resp = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(checkout_resp.status_code),
            f"checkout should have FAILED (item2 out of stock) but got "
            f"status={checkout_resp.status_code}, body={checkout_resp.text}")

        # item1 stock should be unchanged after failed checkout (compensation check)
        stock_item1 = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_item1, 15,
            f"item1 stock should be 15 after failed checkout (compensation), got {stock_item1}")

        # Restock item2 — checkout should still fail (no credit)
        resp = tu.add_stock(item_id2, 15)
        self.assertTrue(tu.status_code_is_success(int(resp)),
            f"add_stock(item2={item_id2}, 15) failed with status {resp}")

        credit = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 0,
            f"Expected credit=0 (no credit added yet), got {credit}")

        checkout_resp = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_failure(checkout_resp.status_code),
            f"checkout should have FAILED (no credit) but got "
            f"status={checkout_resp.status_code}, body={checkout_resp.text}")

        # Add credit — now checkout should succeed
        resp = tu.add_credit_to_user(user_id, 15)
        self.assertTrue(tu.status_code_is_success(int(resp)),
            f"add_credit_to_user({user_id}, 15) failed with status {resp}")

        credit = tu.find_user(user_id)['credit']
        self.assertEqual(credit, 15,
            f"Expected credit=15 after adding 15, got {credit}")

        stock_item1 = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_item1, 15,
            f"Expected item1 stock=15 before successful checkout, got {stock_item1}")

        # Final checkout — should succeed
        checkout_resp = tu.checkout_order(order_id)
        self.assertTrue(tu.status_code_is_success(checkout_resp.status_code),
            f"Final checkout FAILED unexpectedly. "
            f"status={checkout_resp.status_code}, body={checkout_resp.text}")

        # Verify stock was decremented
        stock_after = tu.find_item(item_id1)['stock']
        self.assertEqual(stock_after, 14,
            f"Expected item1 stock=14 after checkout (15-1), got {stock_after}")

        # Verify credit was charged
        credit_after = tu.find_user(user_id)['credit']
        self.assertEqual(credit_after, 5,
            f"Expected credit=5 after paying 10 from 15, got {credit_after}")


if __name__ == '__main__':
    unittest.main(verbosity=2)