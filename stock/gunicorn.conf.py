
# Start only one instance of db for both workerss
def on_starting(server):
    from app import init_db
    init_db()

def post_fork(server, worker):
    import threading
    from kafka_service import kafka_client
    from psycopg_pool import ConnectionPool
    import app

    # Reinitialize DB pool
    app.db_pool = ConnectionPool(
        conninfo=app.db_pool.conninfo,
        min_size=1,
        max_size=10,
        reconnect_timeout=30,
        kwargs={"connect_timeout": 10}
    )
    app.event_consumer._db_pool = app.db_pool

    # Reinitialize Kafka
    order_kafka = kafka_client.Client(app.service_name, ['order.events', 'stock.events', 'payment.events'])
    app.kafka_producer = order_kafka.producer
    app.kafka_consumer = order_kafka.consumer
    app.event_consumer._consumer = order_kafka.consumer

    threading.Thread(target=app.consume_messages, daemon=True).start()