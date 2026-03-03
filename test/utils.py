import requests

ORDER_URL = STOCK_URL = PAYMENT_URL = "http://127.0.0.1:8000"


########################################################################################################################
#   STOCK MICROSERVICE FUNCTIONS
########################################################################################################################
def create_item(price: int) -> dict:
    return requests.post(f"{STOCK_URL}/stock/item/create/{price}").json()


def find_item(item_id: str) -> dict:
    return requests.get(f"{STOCK_URL}/stock/find/{item_id}").json()


def add_stock(item_id: str, amount: int) -> int:
    return requests.post(f"{STOCK_URL}/stock/add/{item_id}/{amount}").status_code


def subtract_stock(item_id: str, amount: int) -> int:
    return requests.post(f"{STOCK_URL}/stock/subtract/{item_id}/{amount}").status_code

def get_items() -> list[dict]:
    return requests.get(f"{STOCK_URL}/stock/items").json()

########################################################################################################################
#   PAYMENT MICROSERVICE FUNCTIONS
########################################################################################################################
def payment_pay(user_id: str, amount: int) -> int:
    return requests.post(f"{PAYMENT_URL}/payment/pay/{user_id}/{amount}").status_code


def create_user() -> dict:
    return requests.post(f"{PAYMENT_URL}/payment/create_user").json()


def find_user(user_id: str) -> dict:
    return requests.get(f"{PAYMENT_URL}/payment/find_user/{user_id}").json()


def add_credit_to_user(user_id: str, amount: int) -> int:
    return requests.post(f"{PAYMENT_URL}/payment/add_funds/{user_id}/{amount}").status_code

def get_users() -> list[dict]:
    return requests.get(f"{PAYMENT_URL}/payment/users").json()

########################################################################################################################
#   ORDER MICROSERVICE FUNCTIONS
########################################################################################################################
def create_order(user_id: str) -> dict:
    return requests.post(f"{ORDER_URL}/orders/create/{user_id}").json()


def add_item_to_order(order_id: str, item_id: str, quantity: int) -> int:
    return requests.post(f"{ORDER_URL}/orders/addItem/{order_id}/{item_id}/{quantity}").status_code


def find_order(order_id: str) -> dict:
    return requests.get(f"{ORDER_URL}/orders/find/{order_id}").json()


def checkout_order(order_id: str) -> requests.Response:
    return requests.post(f"{ORDER_URL}/orders/checkout/{order_id}")

def order_test(message: str) -> requests.Response:
    return requests.get(f"{ORDER_URL}/orders/test/{message}")



########################################################################################################################
#   STATUS CHECKS
########################################################################################################################
def status_code_is_success(status_code: int) -> bool:
    return 200 <= status_code < 300


def status_code_is_failure(status_code: int) -> bool:
    return 400 <= status_code < 500



def setup_test_environment(n: int):
    # Create 2 users for testing
    user_ids = []
    item_ids = []

    for _ in range(n):
        user_response = create_user()
        user_ids.append(user_response["user_id"])

    # Add some credit to the users
    for user_id in user_ids:
        add_credit_to_user(user_id, 1000)
    
    # Add some items
    for i in range(n):
        item_response = create_item(10 + i * 5)
        item_ids.append(item_response["item_id"])

    # Add some stock
    for item_id in item_ids:
        add_stock(item_id, 1000)


def print_test_data() -> None:
    
    user_ids = get_users()
    print(user_ids)
    items = get_items()
    print(items)
    for user in user_ids:        
        print(f"User ID: {user['user_id']}, Credit: {user['credit']}")

    for item in items:

        print(f"Item ID: {item['item_id']}, Price: {item['price']}, Stock: {item['stock']}")


def test_checkout_saga():
    
    user: dict = create_user()
    # Add some credit to the user
    add_credit_to_user(user["user_id"], 1000)
    user = find_user(user["user_id"])
    print(f"Testing checkout saga for user: {user['user_id']} with credit: {user['credit']}")
    
    user_id = user["user_id"]
    order_response = create_order(user_id=user_id)
    print(order_response)

    order_id = order_response["order_id"]

    # Create some items and add them to the stock
    for price in [10, 20, 30]:
        item_response = create_item(price)
        item_id = item_response["item_id"]
        add_stock(item_id, 100)
        print(f"Created item with ID: {item_id} and price: {price}")

    item_ids = get_items()
    print(f"Available items: {item_ids}")

    # Add items to the order
    for item in item_ids:
        add_item_to_order(order_id, item["item_id"], 2)
    

    # Checkout the order
    checkout_response = checkout_order(order_id)
    print(f"Checkout response status code: {checkout_response.status_code}")


def test_add_item_to_order():
    # Create a user and an associated order
    user_response = create_user()
    user_id = user_response["user_id"]
    print(f"Created user with ID: {user_id}")

    order_response = create_order(user_id=user_id)
    order_id = order_response["order_id"]
    print(f"Created order with ID: {order_id}")

    # Create an item
    item_response = create_item(price=20)
    item_id = item_response["item_id"]
    print(f"Created item with ID: {item_id}")
    # Add stock for the item
    add_stock(item_id, 50)

    # Add the item to the order
    response = add_item_to_order(order_id, item_id, 2)
    print(f"Add item response status code: {response}")
    
    # Retrieve the order details to verify the item was added
    order_details = find_order(order_id)
    print(f"Order details after adding item: {order_details}")
    # Check if the item_id is in the order details
   

if __name__ == '__main__':
    # setup_test_environment(2)
    # print_test_data()
    test_checkout_saga()

    # test_add_item_to_order()