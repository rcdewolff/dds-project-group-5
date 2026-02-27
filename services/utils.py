import uuid
from enum import StrEnum
from typing import Generic, TypeVar
from msgspec import Struct
import uuid

from typing import Generic, TypeVar, Optional
from msgspec import Struct
import uuid
import datetime

T = TypeVar("T", bound=Struct)

class BaseEvent(Struct, Generic[T]):
    event_type: str
    correlation_id: str
    payload: T
    timestamp: float = datetime.datetime.now(datetime.timezone.utc).timestamp()

    @classmethod
    def create(cls, event_type: str, payload: T, corr_id: str = ""):
        return cls(
            event_type=event_type,
            payload=payload,
            correlation_id=corr_id or str(uuid.uuid4())
        )

# Children classes

class InternalEvent(BaseEvent[T]):
    """Optimized for the Append-Only Log."""
    version: int = 1  # Crucial for replaying old events after code changes
    aggregate_id: str = "" # e.g., order_id to group events in the log

class ExternalEvent(BaseEvent[T]):
    """Optimized for the Message Broker (Kafka/RabbitMQ)."""
    source_service: str = "order-service"
    schema_version: str = "v1" # Helps consumers handle breaking changes

class OrderCheckoutPayload(Struct):
    order_id: str
    items: list[tuple[str,int]] 
    

class CartUpdatePayload(Struct):
    item_id: str
    quantity: int

class StockUpdatePayload(Struct):
    item_id: str
    quantity: int

class BalanceUpdatePayload(Struct):
    user_id: str
    amount: int

class UserCreatePayload(Struct):
    user_id: str

class OrderCreatePayload(Struct):
    order_id: str

class ItemCreatePayload(Struct): 
    pass

class StockPayload(Struct):
    """
    Possible types for this events: CREATE_ITEM, DELETE_ITEM, RELEASE_STOCK, STOCK_PROCESSED
    """
    order_id: str
    item_id: str


class PaymentCheckoutPayload(Struct):
    order_id: str
    price: float

class CheckoutPayload(Struct):
    """
    Possible types: DEPOSIT, PAY, REFUND_CREDIT, PAYMENT_PROCESSED
    """
    order_id: str
    user_id: str


# class OrderEventType(StrEnum):
#     # Domain Events (Past Tense)
#     CREATED = "ORDER_CREATED"
#     CANCELLED = "ORDER_CANCELLED"
#     PAID = "ORDER_PAID"
#     PAYMENT_CANCELED = "PAYMENT_FAILED"

    # Command Events (Imperative)
    # INITIATE_CHECKOUT = "INITIATE_CHECKOUT"
    # TRIGGER_COMPENSATION = "TRIGGER_STOCK"
    # ADD_ITEM = "ADD_ITEM"

# class StockEventType(StrEnum):
#     SUBTRACTED = "ITEM_SUBTRACTED"
#     SUBTRACTION_FAILED = "STOCK_SUBTRACTION_FAILED"
#     ADDITION_FAILED = "STOCK_ADDITION_FAILED"
#     RESTORED = "STOCK_RESTORED"
#     ADDED = "ITEM_ADDED"

# class PaymentEventType(StrEnum):
#     INITIATED = "PAYMENT_INITIATED"
#     FAILED = "PAYMENT_FAILED"
#     REVERTED = "PAYMENT_REVERTED"
    

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