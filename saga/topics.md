# Topics

- stock.request
- payment.request
- order.request

All application topics are created manually by `scripts/create-kafka-topics.sh` with:

- partitions: 6
- replication factor: 1 (single broker)
- key for saga messages: `correlation_id`
