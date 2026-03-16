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

When creating a new version of a consumer or adding it to a new service, follow this template in your `gunicorn.conf.py`. We use this setting because 
kafka is quite slow at the booting and it is better to start the daemon after
gunicorn started the workers.

```python
# Introduce kafka lazy setup, it waits for gunicorn to be up and then starts the clients
def post_fork(server, worker):
    import threading
    from kafka_service import kafka_client
    import app    
    # Reinitialize kafka client fresh in each worker
    order_kafka = kafka_client.Client(app.service_name, [f'{app.service_name}.request'])
    app.kafka_producer = order_kafka.producer
    app.kafka_consumer = order_kafka.consumer
    
    threading.Thread(target=app.consume_messages, daemon=True).start()

```

### 4. Setup
If you want to change imports, modify `setup.py` and run the command `pip install -e ./services`

---
## Events
#### Domain Events (Past Tense)
These describe things that have already happened within a single service. They are used to keep track and communicate to other services. Each service should handle a domain event internally (retry policy, rollback,...)

Naming Convention: [Entity][Action]ed

Examples: OrderCreated, StockReserved, PaymentFailed, UserRegistered.

Purpose: To inform other services that they might need to react.

#### Command Events (Imperative)

These describe an intent to do something internally. This kind of events shouldn't be transmitted to other services.

Naming Convention: [Action][Entity]

Examples: ReserveStock, ProcessPayment, SendEmail.

Purpose: To trigger a specific action in a downstream service (often used in Sagas).