
# Start only one instance of db for both workerss
def on_starting(server):
    from app import init_db
    init_db()


def post_fork(server, worker):
    from services import kafka_client
    import app
    app.db_pool = app.init_db_pool()
    # Producer only — consumer lives in its own process now
    order_kafka = kafka_client.Client(app.service_name, [])
    app.kafka_producer = order_kafka.producer

