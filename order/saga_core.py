# saga_core.py
# Core definitions for Saga pattern implementation
from dataclasses import dataclass, field
from typing import Callable, Any, Optional, Dict, List
from enum import Enum
from abc import ABC, abstractmethod
import uuid
from datetime import datetime

class SagaStatus(Enum):
    """Possible states for a saga step"""
    PENDING = "pending"  # Not started
    RUNNING = "running"  # In progress
    COMPLETED = "completed"  # All steps succeeded
    COMPENSATING = "compensating"  # Rolling back
    FAILED = "failed"  # Completed with failure
    COMPENSATED = "compensated"  # Successfully rolled back


class SimpleSagaStep(Enum):
    """
    Represents the three steps of the checkout saga in execution order.
    Progression: CHECKOUT → STOCK_RESERVATION → PAYMENT
    """
    CHECKOUT = "ORDER_CHECKOUT_PHASE"               # Step 1: validate and initialize order
    STOCK_RESERVATION = "STOCK_RESERVATION_PHASE"  # Step 2: reserve items in stock service
    PAYMENT = "PAYMENT_PHASE"                       # Step 3: charge payment service

@dataclass
class SimpleSagaContext:
    saga_id: str                        # = correlation_id
    step: SimpleSagaStep                # current phase
    status: SagaStatus                  # PENDING, RUNNING, COMPENSATING etc.
    data: Dict[str, Any]                # order_id, user_id, items — immutable input
    results: Dict[str, Any] 
    
    def set_result(self, step: str, result: Any):
        self.results[step] = result

    def advance(self):
        if self.step == SimpleSagaStep.CHECKOUT:
            self.step = SimpleSagaStep.STOCK_RESERVATION
        elif self.step == SimpleSagaStep.STOCK_RESERVATION:
            self.step = SimpleSagaStep.PAYMENT
        else:
            raise Exception("No further steps to advance to.")
    
    def get_result(self, step: str) -> Optional[Any]:
        return self.results.get(step)
    


# The coordinator logic lives here, not in a Callable
class CheckoutSagaOrchestrator:
    def start(self, context: SimpleSagaContext):
        
        pass
        # emit RESERVE_STOCK command to Kafka
        # subscribe to Redis, wait for reply
        # on success: advance(), emit START_PAYMENT
        # on failure: emit compensation commands

    def compensate(self, context: SimpleSagaContext):
        pass
        # walk back completed_steps in reverse
        # emit FREE_STOCK, ROLLBACK_PAYMENT etc.


