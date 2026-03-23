#!/bin/sh
set -eu

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
PARTITIONS="${KAFKA_APP_TOPIC_PARTITIONS:-6}"
REPLICATION_FACTOR="${KAFKA_APP_TOPIC_REPLICATION_FACTOR:-1}"

TOPICS="stock.request payment.request order.request"

printf 'Waiting for Kafka broker at %s\n' "$BOOTSTRAP_SERVER"
until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP_SERVER" >/dev/null 2>&1; do
  sleep 2
done

for topic in $TOPICS; do
  printf 'Ensuring topic %s exists with %s partitions (rf=%s)\n' "$topic" "$PARTITIONS" "$REPLICATION_FACTOR"
  /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR"
done

printf 'Kafka application topic bootstrap completed.\n'
