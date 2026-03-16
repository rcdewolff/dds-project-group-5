import time
from typing import Any, Optional
from datetime import datetime
import uuid
from msgspec import Struct, to_builtins


class BaseEvent(Struct):
    event_type: str        
    payload: dict[str, Any] 
    correlation_id: str

    @staticmethod
    def create(event_type: str, payload: Struct, corr_id: str = ""):
        """Helper to ensure fresh timestamps and ID propagation"""
        return BaseEvent(
            event_type=event_type,
            payload=to_builtins(payload),
            correlation_id=corr_id or str(uuid.uuid4())
        )


class OrderPayload(Struct):
    """
    Possible types for this events: ADD_ITEM, REMOVE_ITEM, ORDER_FAILED 
    """
    order_id: str
    item_id: str
    quantity: int

class StockPayload(Struct):
    """
    Possible types for this events: CREATE_ITEM, DELETE_ITEM, RELEASE_STOCK, STOCK_PROCESSED
    """
    order_id: str
    item_id: str

class CheckoutPayload(Struct):
    """
    Possible types: DEPOSIT, PAY, REFUND_CREDIT, PAYMENT_PROCESSED
    """
    order_id: str
    user_id: str