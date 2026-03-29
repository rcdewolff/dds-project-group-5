# Topics

- stock.request
- payment.request
- order.request
- checkout-commands
- checkout-results

All application topics are created manually by `scripts/create-kafka-topics.sh` with:

- partitions: 6
- replication factor: 1 (single broker)
- key for saga messages: `correlation_id`

`checkout-commands` carries worker-issued checkout commands and is keyed by `correlation_id`.

Minimal event schema for `checkout-commands`:

```json
{
	"event_type": "checkout.command",
	"correlation_id": "<uuid>",
	"order_id": "<order_id>",
	"timestamp": 1735560000.123
}
```

`checkout-results` carries terminal checkout outcomes and is keyed by `correlation_id`.

Minimal event schema for `checkout-results`:

```json
{
	"event_type": "checkout.result",
	"correlation_id": "<uuid>",
	"order_id": "<order_id>",
	"status": "completed|failed|compensated",
	"results": {},
	"error": "optional failure details",
	"timestamp": 1735560000.123
}
```
