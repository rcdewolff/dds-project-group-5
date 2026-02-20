from typing import Optional
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel


class ItemAdded():
    order_id: UUID
    item_id: UUID
    quantity: int


class ItemRemoved():
    order_id: UUID
    item_id: UUID
    quantity: int


class StockSubtracted():
    order_id: UUID
    item_id: UUID


class CheckoutInitiated():
    order_id: UUID


class CheckoutCompleted():
    order_id: UUID


class CheckoutCancelled():
    order_id: UUID


class SequenceNumber():
    sequence_number: int