import logging

import app
from services import kafka_client


logging.basicConfig(level=logging.INFO)


def main():
    app.db_pool = app.init_db_pool()
    client = kafka_client.Client(app.service_name, [f"{app.service_name}.request"])
    app.consume_messages(client.consumer)


if __name__ == "__main__":
    main()
