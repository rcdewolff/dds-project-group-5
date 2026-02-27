
# Start only one instance of db for both workerss
def on_starting(server):
    from app import init_db
    init_db()

# Introduce kafka lazy setup, it waits for gunicorn to be up and then starts the clients
def post_fork(server, worker):
    import threading
    from services import kafka_client
    import app    
    # Reinitialize kafka client fresh in each worker
    order_kafka = kafka_client.Client(app.service_name, [f'{app.service_name}.request'])
    app.kafka_producer = order_kafka.producer
    app.kafka_consumer = order_kafka.consumer
    
    threading.Thread(
        target=app.consume_messages, 
        args=(order_kafka.consumer,),
        daemon=True).start()