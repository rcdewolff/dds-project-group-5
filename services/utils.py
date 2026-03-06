import uuid
from enum import StrEnum
from typing import Generic, TypeVar
from msgspec import Struct
import uuid

from typing import TypeVar, Generic, Type, Any, Optional
from msgspec import json, convert, Struct, ValidationError
import uuid
import datetime
from dataclasses import dataclass
from typing import Generic, TypeVar, Union



# Generic type variables are used for the Result Pattern, that allows us to structure the return types
#  of our operations.

T = TypeVar("T")  # The type of the Success value (e.g., int)
E = TypeVar("E")  # The type of the Error value (e.g., str)

@dataclass(frozen=True)
class Success(Generic[T]):
    value: T
    is_ok: bool = True

@dataclass(frozen=True)
class Failure(Generic[E]):
    error: E
    is_ok: bool = False

# A type alias for convenience, especially in db calls or other simple applications.
CreditResult = Union[Success[int], Failure[str]]

J = TypeVar("J")


class BaseEvent(Struct, Generic[J]):
    """
    A generic event envelope that can wrap any payload type. 
    Useful for Kafka messages where we want a consistent structure but variable payloads.
    """
    event_type: str
    correlation_id: str
    payload: J
    timestamp: float = datetime.datetime.now(datetime.timezone.utc).timestamp()

    @classmethod
    def create(cls, event_type: str, payload: J, corr_id: str = ""):
        return cls(
            event_type=event_type,
            payload=payload,
            correlation_id=corr_id or str(uuid.uuid4())
        )



class OrderCheckoutPayload(Struct):
    """
    Payload for an order checkout event.
    """
    order_id: str
    items: list[tuple[str,int]]
    

class StartPaymentCommandPayload(Struct):
    """
    Payload for a start payment command event.
    """
    order_id: str
    user_id: str
    amount: int


class ReserveStockCommandPayload(Struct):
    """
    Payload for a reserve stock command event.
    """
    order_id: str
    items: list[tuple[str,int]]

class StockUnavailablePayload(Struct):
    """
    Payload for a stock unavailable event.
    """
    order_id: str
    items: list[tuple[str,int]]


class StockReservedPayload(Struct):
    """
    Payload for a stock reserved event.
    """
    order_id: str
    amount: int


class PaymentProcessedPayload(Struct):
    """
    Payload for a payment processed event.
    """
    order_id: str
    user_id: str
    amount: int
    remaining_credit: int

class PaymentFailedPayload(Struct):
    """
    Payload for a payment failed event.
    """
    order_id: str
    user_id: str
    amount: int
    reason: str


class CheckoutPayload(Struct):
    
    order_id: str
    user_id: str




class Commands(StrEnum):
    RESERVE_STOCK = "reserve_stock"
    FREE_STOCK = "free_stock"

    START_PAYMENT = "start_payment"
    ROLLBACK_PAYMENT = "rollback_payment"


class OrderInternalEvent(StrEnum):
    ORDER_CREATED = "order_created"
    ITEM_ADDED = "item_added"
    CHECKOUT_INITIATED = "checkout_initiated"
    ORDER_CANCELLED = "order_cancelled"
    ORDER_COMPLETED = "order_completed"

class PaymentInternalEvent(StrEnum):
    USER_CREATED = "user_created"
    FUNDS_ADDED = "funds_added"
    PAYMENT_RESERVED = "payment_reserved"
    PAYMENT_CONFIRMED = "payment_confirmed"

class StockInternalEvent(StrEnum):
    ITEM_CREATED = "item_created"
    STOCK_INCREMENTED = "stock_incremented"
    STOCK_DECREMENTED = "stock_decremented"
    STOCK_RESERVED = "stock_reserved"


class OrderIntegrationEvent(StrEnum):
    # Triggered by /orders/checkout/{order_id}
    # Sent to Stock and Payment services
    ORDER_PLACED = "integration.order.placed"
    
    # Sent when the entire saga completes
    ORDER_COMPLETED = "integration.order.ready"

class PaymentIntegrationEvent(StrEnum):
    # Sent to Order service to confirm billing success
    PAYMENT_SUCCEEDED = "integration.payment.succeeded"
    PAYMENT_FAILED = "integration.payment.failed"

class StockIntegrationEvent(StrEnum):
    # Sent to Order service after stock is successfully subtracted
    STOCK_ALLOCATED = "integration.stock.allocated"
    STOCK_UNAVAILABLE = "integration.stock.unavailable"





PAYLOAD_REGISTRY: dict[str, type] = {
    Commands.RESERVE_STOCK: OrderCheckoutPayload,
    Commands.START_PAYMENT: StartPaymentCommandPayload,
    StockIntegrationEvent.STOCK_ALLOCATED: StockReservedPayload,
    StockIntegrationEvent.STOCK_UNAVAILABLE: StockUnavailablePayload,
    PaymentIntegrationEvent.PAYMENT_SUCCEEDED: PaymentProcessedPayload,
    PaymentIntegrationEvent.PAYMENT_FAILED: PaymentFailedPayload
}

DecodeResult = Union[Success[BaseEvent[Any]], Failure[str]]



def decode_and_type_event(record: Any) -> DecodeResult:
    """
    Decodes a raw Kafka message into a specific BaseEvent[PayloadStruct].
    Returns None if the event type is unknown or validation fails.
    """
    try:

        raw_bytes = record.value
        print(f"Decoding raw bytes: {raw_bytes}")
        # Decode the envelope with a dict payload
        envelope = json.decode(raw_bytes, type=BaseEvent[dict])
        
        # Registry Lookup
        payload_cls = PAYLOAD_REGISTRY.get(envelope.event_type)
        if not payload_cls:
            print(f"Event decoding: unknown event type: {envelope.event_type}")
            return Failure(error=f"UNKNOWN_EVENT_TYPE:{envelope.event_type}")

        # Convert dict to specific Struct
        typed_payload = convert(envelope.payload, payload_cls)
        
        # Return new instance with the typed payload
        return Success(
            value = BaseEvent (
                event_type=envelope.event_type,
                correlation_id=envelope.correlation_id,
                payload=typed_payload,
                timestamp=envelope.timestamp
            )
        )
    
    except ValidationError as e:
        print(f"Validation failed: {e}")
        return Failure(error=f"Validation failed: {e}")
    except Exception as e:
        print(f"Decoding error: {e}")
        return Failure(error=f"Decoding error: {e}")
    

class RedisMessageWrapper:
    """
    A wrapper for Redis messages to ensure consistent handling of byte strings.
    """
    def __init__(self, data):
        self.value = data.encode() if isinstance(data, str) else data
