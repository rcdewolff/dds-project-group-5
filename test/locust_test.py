import locust

# Create a simple locust test class for my localhost 8000
class MyLocust(locust.HttpUser):

    @locust.task
    def single_user(self):
       
        res = self.client.get("/payment/create-user")
        user_id = res.json().get("user_id")
        print(f"Created user with ID: {user_id}")

        res = self.client.get(f"/orders/create/{user_id}")
        order_id = res.json().get("order_id")
        print(f"Created order with ID: {order_id}")

        res = self.client.post(f"/stock/items")
        for item in res.json():
            item_id = item.get("item_id")
            print(f"Adding item with ID: {item_id} to order")
            self.client.post(f"/orders/addItem/{order_id}/{item_id}/1")

            