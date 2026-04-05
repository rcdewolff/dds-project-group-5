# Two-Phase Commit Protocol Implementation

## Overview

This document describes the implementation of the Two-Phase Commit (2PC) protocol for the distributed order checkout system. The 2PC protocol ensures atomicity across multiple microservices (Order, Stock, and Payment) when processing a checkout transaction. Either all services commit the transaction successfully, or all abort, maintaining system-wide consistency.

## System Architecture

The system consists of four main components:

1. **Coordinator Service** - Orchestrates the 2PC protocol
2. **Order Service** - Manages orders and initiates checkout
3. **Stock Service** - Manages inventory and stock reservations
4. **Payment Service** - Manages user credits and payment processing

### High-Level Flow

```
Client → Order Service → Coordinator → [Stock Service, Payment Service]
                            ↓
                    2PC Protocol Execution
                            ↓
                    Success/Failure Response
```

## Orchestrator Implementation

The orchestrator is implemented in [`coordinator/app.py`](coordinator/app.py) as the `Orchestrator` class.
It is intentionally service-agnostic: concrete participants are supplied by the caller as `Participant` objects, and any domain side-effect (for this project: marking an order as paid) is injected from Order via `on_commit_decided`.

### Key Components

#### Transaction States

```python
class TxStatus(str, Enum):
    INITIATED = "INITIATED"
    PREPARED  = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED   = "ABORTED"
```

#### Participant Model

Each participant (service) in the transaction is represented by a `Participant` dataclass:

```python
@dataclass
class Participant:
    name:         str
    prepare_url:  str
    commit_url:   str
    abort_url:    str
    prepare_body: dict[str, Any]
```

This encapsulates all the information needed to communicate with each service during the 2PC protocol.

### The 2PC Protocol Flow

The orchestrator's `run()` method implements the full 2PC protocol:

#### Phase 1: Prepare Phase

1. **Transaction Initialization**
   - Generate a unique `transaction_id` (UUID)
   - Persist transaction with `INITIATED` status in the database
   - If persistence fails, return failure immediately

2. **Prepare Requests**
   - Iterate through all participants
   - Send HTTP POST request to each participant's `prepare_url`
   - Include transaction-specific data in the request body
   - If ANY participant fails to prepare:
     - Abort all previously prepared participants
     - Update transaction status to `ABORTED`
     - Return failure result

3. **All Prepared**
   - If all participants successfully prepare
   - Update transaction status to `PREPARED`
   - Proceed to Phase 2

#### Phase 2: Commit Phase

1. **Commit Requests**
   - Iterate through all participants
   - Send HTTP POST request to each participant's `commit_url`
   - If ANY participant fails to commit:
     - Abort remaining participants (that haven't committed yet)
     - Update transaction status to `ABORTED`
     - Return failure result

2. **All Committed**
   - If all participants successfully commit
   - Update transaction status to `COMMITTED`
   - Return success result with transaction ID

### Error Handling

The coordinator includes robust error handling:

- **Network Failures**: Catches `RequestException` and treats as prepare/commit failure
- **Timeout Handling**: Configurable timeout (default 10 seconds) for each request
- **Partial Failures**: Tracks which participants have prepared/committed for proper cleanup
- **Database Persistence**: All state changes are persisted to handle coordinator crashes

### Abort Protocol

When a failure occurs, the coordinator's `_abort_all()` method:
- Sends abort requests to all participants that had successfully prepared
- Continues even if abort requests fail (best-effort cleanup)
- Logs errors for debugging

## Participant Implementations

### Stock Service

Location: [`stock/app.py`](stock/app.py)

#### Database Schema

The stock service maintains three tables for 2PC:

```sql
-- Main inventory
CREATE TABLE items (
    item_id TEXT PRIMARY KEY,
    stock   INTEGER NOT NULL,
    price   INTEGER NOT NULL
)

-- Transaction tracking
CREATE TABLE stock_transactions (
    transaction_id TEXT PRIMARY KEY,
    order_id       TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'PREPARED',
    items_reserved JSONB NOT NULL,
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

-- Individual stock reservations
CREATE TABLE stock_reservations (
    reservation_id TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    item_id        TEXT NOT NULL,
    quantity       INTEGER NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES stock_transactions(transaction_id)
)
```

#### 2PC Endpoints

**Prepare Endpoint** (`POST /prepare/<transaction_id>`)
- Validates that all requested items exist
- Checks that sufficient stock is available for each item
- Creates a transaction record with status `PREPARED`
- Creates reservation records for each item (without modifying actual stock)
- Returns 200 OK if all checks pass
- Returns 400 error if:
  - Items don't exist
  - Insufficient stock available
  - Database errors occur

**Commit Endpoint** (`POST /commit/<transaction_id>`)
- Retrieves all reservations for the transaction
- Deducts reserved quantities from actual stock: `stock = stock - quantity`
- Updates transaction status to `COMMITTED`
- Returns 200 OK on success

**Abort Endpoint** (`POST /abort/<transaction_id>`)
- Deletes all reservation records (via CASCADE due to foreign key)
- Updates transaction status to `ABORTED`
- No changes are made to actual stock levels
- Returns 200 OK on success

### Payment Service

Location: [`payment/app.py`](payment/app.py)

#### Database Schema

The payment service maintains three tables for 2PC:

```sql
-- User accounts
CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    credit  INTEGER NOT NULL
)

-- Transaction tracking
CREATE TABLE payment_transactions (
    transaction_id TEXT PRIMARY KEY,
    order_id       TEXT NOT NULL,
    user_id        TEXT NOT NULL,
    amount         INTEGER NOT NULL,
    status         TEXT NOT NULL DEFAULT 'PREPARED',
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)

-- Credit holds (pending charges)
CREATE TABLE credit_holds (
    hold_id        TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    user_id        TEXT NOT NULL,
    amount         INTEGER NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES payment_transactions(transaction_id)
)
```

#### 2PC Endpoints

**Prepare Endpoint** (`POST /prepare/<transaction_id>`)
- Validates that the user exists
- Checks that the user has sufficient credit
- Creates a transaction record with status `PREPARED`
- Creates a credit hold record (without deducting actual credit)
- Returns 200 OK if all checks pass
- Returns 400 error if:
  - User doesn't exist
  - Insufficient credit
  - Database errors occur

**Commit Endpoint** (`POST /commit/<transaction_id>`)
- Retrieves the transaction record
- Deducts the amount from user's credit: `credit = credit - amount`
- Updates transaction status to `COMMITTED`
- Returns 200 OK on success

**Abort Endpoint** (`POST /abort/<transaction_id>`)
- Deletes the credit hold record (via CASCADE)
- Updates transaction status to `ABORTED`
- No changes are made to actual credit balance
- Returns 200 OK on success

## Checkout Flow Implementation

Location: [`order/app.py`](order/app.py) - `/checkout/<order_id>` endpoint

### Step-by-Step Process

1. **Retrieve Order Details**
   ```python
   order_entry = get_order_from_db(order_id)
   ```
   - Fetches order items, user_id, and total cost

2. **Aggregate Items**
   ```python
   items_quantities: dict[str, int] = defaultdict(int)
   for item_id, quantity in order_entry.items:
       items_quantities[item_id] += quantity
   ```
   - Consolidates duplicate items (if the same item was added multiple times)

3. **Configure Participants**
   
   **Stock Participant:**
   ```python
   stock_participant = Participant(
       name="stock",
       prepare_url=f"{GATEWAY_URL}/stock/prepare/{{transaction_id}}",
       commit_url=f"{GATEWAY_URL}/stock/commit/{{transaction_id}}",
       abort_url=f"{GATEWAY_URL}/stock/abort/{{transaction_id}}",
       prepare_body={'order_id': order_id, 'items': items_list},
   )
   ```

   **Payment Participant:**
   ```python
   payment_participant = Participant(
       name="payment",
       prepare_url=f"{GATEWAY_URL}/payment/prepare/{{transaction_id}}",
       commit_url=f"{GATEWAY_URL}/payment/commit/{{transaction_id}}",
       abort_url=f"{GATEWAY_URL}/payment/abort/{{transaction_id}}",
       prepare_body={'order_id': order_id, 'user_id': order_entry.user_id,
                     'amount': order_entry.total_cost},
   )
   ```

4. **Execute 2PC Protocol**
   ```python
    result = orchestrator.run(business_id=order_id,
                                      participants=[stock_participant, payment_participant])
   ```

5. **Handle Result**
   
   **On Failure:**
   - Publish `ORDER_FAILED` event to Kafka
   - Return HTTP 400 error with error message

   **On Success:**
   - Update order's `paid` status to `TRUE` in database
   - Publish `CHECKOUT_SUCCESS` event to Kafka
   - Return HTTP 200 with transaction_id and correlation_id

## Database Persistence for Durability

The orchestrator persists transaction state to a PostgreSQL database:

```sql
CREATE TABLE orchestrator_transactions (
    transaction_id TEXT PRIMARY KEY,
    business_id    TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'INITIATED',
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

### State Persistence Strategy

- **UPSERT Pattern**: Uses `ON CONFLICT ... DO UPDATE` to update existing records
- **Every Phase Transition**: Status is persisted when moving between states
- **Failure Recovery**: In case of coordinator crash, the database contains the last known state
- **Logging**: Comprehensive debug and error logging for observability

## Key Design Decisions

### 1. **URL Template Pattern**
The coordinator uses template URLs with `{transaction_id}` that are formatted at runtime:
```python
def url(self, template: str, transaction_id: str) -> str:
    return template.format(transaction_id=transaction_id)
```
This allows flexible URL construction while keeping the transaction ID consistent.

### 2. **Reservation Pattern**
Both Stock and Payment services use a "reservation" pattern:
- **Prepare Phase**: Create reservations/holds without modifying actual resources
- **Commit Phase**: Apply the reserved changes to actual resources
- **Abort Phase**: Delete reservations without affecting actual resources

This ensures that:
- Resources are effectively locked during the transaction
- The system can safely rollback if needed
- No inconsistent state is visible to other transactions

### 3. **Foreign Key Cascades**
The database schema uses `ON DELETE CASCADE` for reservation/hold tables:
```sql
FOREIGN KEY (transaction_id) 
    REFERENCES stock_transactions(transaction_id) ON DELETE CASCADE
```
This simplifies cleanup during abort operations.

### 4. **Best-Effort Abort**
The coordinator's abort mechanism continues even if individual abort requests fail:
```python
def _abort(self, transaction_id: str, p: Participant) -> None:
    try:
        requests.post(p.url(p.abort_url, transaction_id), timeout=self.timeout)
    except requests.exceptions.RequestException as exc:
        logger.error("ABORT error  participant=%s  exc=%s", p.name, exc)
        # Continues despite error
```
This ensures that as many participants as possible are notified of the abort.

### 5. **Idempotent Operations**
The prepare/commit/abort endpoints are designed to be idempotent:
- Multiple prepare calls with the same transaction_id will use the existing record
- Commit/abort operations can be safely retried

### 6. **Event Sourcing Integration**
The system integrates 2PC with Kafka event streaming:
- `ORDER_FAILED` events are published when 2PC fails
- `CHECKOUT_SUCCESS` events are published when 2PC succeeds
- Correlation IDs enable distributed tracing

## Limitations and Trade-offs

### Current Limitations

1. **Blocking Protocol**: The coordinator blocks while waiting for all participants
   - Impact: Reduced throughput for concurrent checkouts
   - Mitigation: Could implement async/parallel prepare requests

2. **No Timeout Recovery**: If a participant hangs, the transaction waits until timeout
   - Impact: Resources held longer than necessary
   - Mitigation: Aggressive timeouts (10 seconds) configured

3. **Single Coordinator**: No coordinator redundancy
   - Impact: Single point of failure
   - Mitigation: Database persistence enables crash recovery

4. **No Participant Recovery Protocol**: If a participant crashes, manual intervention may be needed
   - Impact: Orphaned prepared transactions
   - Mitigation: Could implement background cleanup jobs

### Trade-offs

**Consistency vs Availability**: This implementation prioritizes consistency over availability:
- Pro: Strong consistency guarantees - no partial checkouts
- Con: Lower availability - any participant failure causes checkout failure
- Alternative: Saga pattern with eventual consistency

**Simplicity vs Features**: The implementation is deliberately simple:
- Pro: Easy to understand, debug, and maintain
- Con: Missing advanced features like:
  - Nested transactions
  - Read-only optimization
  - Presumed abort/commit
  - Three-phase commit

**Synchronous vs Asynchronous**: Uses synchronous HTTP requests:
- Pro: Simple control flow, easier error handling
- Con: Lower performance compared to async implementations
- Alternative: Message-based 2PC with message queues

## Testing the Implementation

The system includes test cases in [`test/test_microservices.py`](test/test_microservices.py):

### Test Scenarios
- **Success Case**: Valid checkout with sufficient stock and credit
- **Insufficient Stock**: Checkout fails if stock is unavailable
- **Insufficient Credit**: Checkout fails if user has insufficient funds
- **Rollback**: Verify that failed checkouts don't modify state

### Running Tests
```bash
python -m pytest test/test_microservices.py
```

## Monitoring and Debugging

### Logging

The implementation includes comprehensive logging at key points:

```python
logger.debug("2PC START  tx=%s  order=%s", transaction_id, order_id)
logger.debug("2PC PREPARED  tx=%s", transaction_id)
logger.debug("2PC COMMITTED  tx=%s", transaction_id)
logger.warning("2PC ABORT  tx=%s  participant=%s", transaction_id, p.name)
logger.error("2PC COMMIT_FAIL  tx=%s  participant=%s", transaction_id, p.name)
```

### Transaction Tracking

Query the database to track transaction states:

```sql
-- Check transaction status
SELECT * FROM order_transactions WHERE transaction_id = '<uuid>';

-- Check stock reservations
SELECT * FROM stock_reservations WHERE transaction_id = '<uuid>';

-- Check payment holds
SELECT * FROM credit_holds WHERE transaction_id = '<uuid>';
```

## Conclusion

This 2PC implementation provides strong consistency guarantees for distributed transactions across multiple microservices. The coordinator-based approach with prepare-commit-abort semantics ensures that checkout operations are atomic - either all services complete the transaction or none do.

The implementation balances simplicity with correctness, using well-established patterns like resource reservations, idempotent operations, and persistent state management. While there are trade-offs in terms of performance and availability, the system provides a solid foundation for a distributed order processing system with ACID guarantees.
