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


class SagaStep(Enum):
    """
    Represents the three steps of the checkout saga in execution order.
    Progression: CHECKOUT → STOCK_RESERVATION → PAYMENT
    """
    CHECKOUT = "ORDER_CHECKOUT_PHASE"               # Step 1: validate and initialize order
    STOCK_RESERVATION = "STOCK_RESERVATION_PHASE"  # Step 2: reserve items in stock service
    PAYMENT = "PAYMENT_PHASE"                       # Step 3: charge payment service

@dataclass
class SagaContext:
    """
    Context for managing the state of a simple saga.
    """
    saga_id: str
    step: SagaStep
    status: SagaStatus
    order_id: str
    user_id: str
    items: List[tuple[str, int]]
    results: Dict[SagaStep, Any] = field(default_factory=dict)  # ← key by step, not string
    
    def set_result(self, step: SagaStep, result: Any):
        self.results[step] = result

    def advance(self):
        """
        Advance to the next step in the saga.
        Raises an exception if there are no further steps.
        """
        if self.step == SagaStep.CHECKOUT:
            self.step = SagaStep.STOCK_RESERVATION


        elif self.step == SagaStep.STOCK_RESERVATION:
            self.step = SagaStep.PAYMENT
        
        else:
            raise Exception("No further steps to advance to.")
    
    def get_result(self, step: SagaStep) -> Optional[Any]:
        return self.results.get(step)
    



