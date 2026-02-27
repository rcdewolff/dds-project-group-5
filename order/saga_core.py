# saga_core.py
# Core definitions for Saga pattern implementation
from dataclasses import dataclass, field
from typing import Callable, Any, Optional, Dict, List
from enum import Enum
from abc import ABC, abstractmethod
import uuid
from datetime import datetime

class SagaStatus(Enum):
    """Possible states for a saga execution"""
    PENDING = "pending"  # Not started
    RUNNING = "running"  # In progress
    COMPLETED = "completed"  # All steps succeeded
    COMPENSATING = "compensating"  # Rolling back
    FAILED = "failed"  # Completed with failure
    COMPENSATED = "compensated"  # Successfully rolled back

@dataclass
class SagaStep:
    """
    Represents a single step in a saga.

    Each step has:
    - name: Identifier for the step
    - action: Function to execute the step
    - compensation: Function to undo the step if later steps fail
    """
    name: str
    action: Callable[[Dict[str, Any]], Any]  # Execute the step
    compensation: Callable[[Dict[str, Any]], Any]  # Undo the step
    timeout_seconds: float = 30.0  # Max time for step execution

@dataclass
class SagaContext:
    """
    Holds state during saga execution.

    The context is passed to each step and accumulates results.
    This allows steps to access data from previous steps.
    """
    saga_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    data: Dict[str, Any] = field(default_factory=dict)  # Input data
    results: Dict[str, Any] = field(default_factory=dict)  # Step outputs
    completed_steps: List[str] = field(default_factory=list)  # For compensation
    started_at: datetime = field(default_factory=datetime.now)
    status: SagaStatus = SagaStatus.PENDING

    def set_result(self, step_name: str, result: Any):
        """Store the result of a step"""
        self.results[step_name] = result
        self.completed_steps.append(step_name)

    def get_result(self, step_name: str) -> Optional[Any]:
        """Retrieve the result of a previous step"""
        return self.results.get(step_name)




# simple_saga_context.py
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from enum import Enum
import uuid
from datetime import datetime

# Imported from saga_core.py
from saga_core import SagaStatus


class SimpleSagaStep(Enum):
    """
    Represents the three phases of the checkout saga in execution order.
    Progression: CHECKOUT → STOCK_RESERVATION → PAYMENT
    """
    CHECKOUT = "ORDER_CHECKOUT_PHASE"               # Step 1: validate and initialize order
    STOCK_RESERVATION = "STOCK_RESERVATION_PHASE"  # Step 2: reserve items in stock service
    PAYMENT = "PAYMENT_PHASE"                       # Step 3: charge payment service


@dataclass
class SimpleSagaContext:
    """
    Simple saga state machine for the checkout saga across three services:
        1. Order checkout (Order Service)
        2. Stock reservation (Stock Service)
        3. Payment (Payment Service)

    Tracks current phase, overall status, original input data,
    and intermediate results needed for compensation.
    """

    # --- Identity ---
    saga_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=datetime.now)

    # --- State machine ---
    step: SimpleSagaStep = SimpleSagaStep.CHECKOUT
    status: SagaStatus = SagaStatus.PENDING

    # --- Immutable input data (set once at saga creation, never modified) ---
    # Carries everything the saga needs so each phase is self-contained.
    # Example keys: order_id, user_id, product_id, quantity, amount
    data: Dict[str, Any] = field(default_factory=dict)

    # --- Mutable intermediate results (grows as phases complete) ---
    # Populated by each phase so that compensation has the IDs it needs to undo work.
    # Example keys:
    #   "stock_reservation": {"reservation_id": "res-456"}
    #   "payment":           {"charge_id": "chg-789"}
    results: Dict[str, Any] = field(default_factory=dict)

    def set_result(self, phase_name: str, result: Any) -> None:
        """Store the outcome of a completed phase for use in later phases or compensation."""
        self.results[phase_name] = result

    def get_result(self, phase_name: str) -> Optional[Any]:
        """Retrieve the stored outcome of a previous phase."""
        return self.results.get(phase_name)

    def advance(self) -> None:
        """
        Move to the next step in the saga sequence.
        Raises StopIteration if already at the last step.
        """
        steps = list(SimpleSagaStep)
        current_index = steps.index(self.step)
        if current_index + 1 >= len(steps):
            raise StopIteration("Saga is already at the final step.")
        self.step = steps[current_index + 1]