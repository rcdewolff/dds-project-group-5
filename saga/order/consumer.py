import os
from services import kafka_client,utils
import redis # type: ignore
from msgspec import json
import logging

logging.basicConfig(level=logging.INFO)

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))

def main():
    print("Starting Order Service consumer...")
    service_name = "order"
    client = kafka_client.Client(service_name, [f'{service_name}.request'])
    consumer = client.consumer
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    for message in consumer:
        print(f"Received Kafka message on topic {message.topic}")
        result = utils.decode_and_type_event(message)
    
        if isinstance(result, utils.Failure):
            logging.error(f"Kafka message dropped: {result.error}")
            continue
            
        event = result.value
        channel = f"{service_name}:saga:{event.saga_id}"
        
        redis_client.publish(channel,message.value)   
        logging.info(f"Forwarded {event.event_type} to Redis channel {channel}")



if __name__ == "__main__":
    main()