This README is designed to guide the update of kafka clients in our project.

---

## Kafka Service Integration Guide

Since we are working basically on the same things, I've created this guide to ensure that each of our implementation of the kafka client is (almost entirely) compatible with each `app.py` file. 

Generally speaking, let's try to change the single `app.py` files as little as possible, modify the kafka client instead (in `kafka_client`).



### 1. Modular Architecture

Our Kafka integration is built on a shared client class. Instead of rewriting connection logic, you import the `Client` and pass it two specific arguments:

* **Service Name:** Used to create a unique `group_id`. This ensures that if we scale a service, the load is balanced correctly.
* **Topics:** A list of strings defining exactly which channels this service needs to listen to.


### 2. Handling Concurrency

Flask is designed to handle synchronous HTTP requests. Kafka consumers, however, use a **blocking loop** to wait for new messages. If you run the consumer in the main thread, your API will never start.

**The Solution: Daemon Threads**
We use Python’s `threading` module to run the consumer.

* **Background Execution:** The consumer runs in a separate execution context from the Flask request handlers.
* **Daemon Status:** We set `daemon=True` so that if the Flask process crashes or is stopped, the Kafka thread terminates automatically, preventing "zombie" processes.

### 3. Implementation Pattern

When creating a new version of a consumer or adding it to a new service, follow this template in your `app.py`:

```python
from shared_kafka.kafka_client import Client
import threading

# 1. Initialize globally
kafka_client = Client('your-service-name', ['topic_a', 'topic_b'])

def message_processor():
    """
    This is where you define version-specific logic.
    Modify this function to change how the service reacts to messages.
    """
    for msg in kafka_client.consumer:
        # Business logic goes here
        print(f"Processing {msg.topic}")

# 2. Spin up the background thread before app.run()
threading.Thread(target=message_processor, daemon=True).start()

```

### 4. Setup
If you want to change imports, modify `setup.py` and run the command `pip install -e ./kafka_service`