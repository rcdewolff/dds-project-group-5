# 2-Phase Commit Protocol Implementation

## Overview

This document describes the implementation of the 2-Phase Commit (2PC) protocol for the checkout method in the distributed order-payment-stock microservices system.

## What is 2-Phase Commit?

2-phase commit is a distributed transaction protocol that ensures atomicity across multiple services:

1. **Phase 1 (Prepare)**: The coordinator asks all participants if they can commit their changes
2. **Phase 2 (Commit/Abort)**: 
   - If all participants agree, the coordinator instructs them to commit
   - If any participant disagrees, the coordinator instructs all to abort

This ensures that either all changes are committed or all are rolled back, preventing inconsistent states.

## Architecture Changes

### 1. Stock Service (`stock/app.py`)

#### New Tables

**`stock_transactions`**: Tracks 2PC transactions
```sql
CREATE TABLE stock_transactions (
    transaction_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PREPARED',
    items_reserved JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

**`stock_reservations`**: Holds reserved items during prepare phase (without actually deducting stock)
```sql
CREATE TABLE stock_reservations (
    reservation_id TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    item_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES stock_transactions(transaction_id) ON DELETE CASCADE
)
```

#### New Endpoints

- **`POST /stock/prepare/<transaction_id>`**
  - Input: `{'order_id': str, 'items': [{'item_id': str, 'quantity': int}]}`
  - Checks if all items have sufficient stock
  - Creates reservation records (doesn't deduct stock yet)
  - Returns: `{'status': 'PREPARED', 'transaction_id': str}` if successful, 400 if out of stock

- **`POST /stock/commit/<transaction_id>`**
  - Deducts the reserved stock from the items table
  - Updates transaction status to COMMITTED
  - Returns: `{'status': 'COMMITTED', 'transaction_id': str}`

- **`POST /stock/abort/<transaction_id>`**
  - Deletes reservation records (no actual changes were made)
  - Updates transaction status to ABORTED
  - Returns: `{'status': 'ABORTED', 'transaction_id': str}`

### 2. Payment Service (`payment/app.py`)

#### New Tables

**`payment_transactions`**: Tracks 2PC payment transactions
```sql
CREATE TABLE payment_transactions (
    transaction_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    amount INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'PREPARED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

**`credit_holds`**: Holds user credit during prepare phase (without actually deducting credit)
```sql
CREATE TABLE credit_holds (
    hold_id TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    amount INTEGER NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES payment_transactions(transaction_id) ON DELETE CASCADE
)
```

#### New Endpoints

- **`POST /payment/prepare/<transaction_id>`**
  - Input: `{'order_id': str, 'user_id': str, 'amount': int}`
  - Checks if user has sufficient credit
  - Creates hold record (doesn't deduct credit yet)
  - Returns: `{'status': 'PREPARED', 'transaction_id': str}` if successful, 400 if insufficient credit

- **`POST /payment/commit/<transaction_id>`**
  - Deducts the held credit from the user's credit
  - Updates transaction status to COMMITTED
  - Returns: `{'status': 'COMMITTED', 'transaction_id': str}`

- **`POST /payment/abort/<transaction_id>`**
  - Deletes hold records (no actual changes were made)
  - Updates transaction status to ABORTED
  - Returns: `{'status': 'ABORTED', 'transaction_id': str}`

### 3. Order Service (`order/app.py`)

#### New Table

**`order_transactions`**: Tracks coordinator's 2PC state machine
```sql
CREATE TABLE order_transactions (
    transaction_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'INITIATED',
    stock_transaction_id TEXT,
    payment_transaction_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

Transaction statuses: INITIATED → PREPARED → COMMITTED/ABORTED

#### Updated Checkout Logic

The new `checkout/<order_id>` endpoint implements full 2PC protocol:

```python
POST /checkout/<order_id>

Response (200 OK):
{
    'status': 'success',
    'order_id': str,
    'transaction_id': str,
    'message': 'Checkout successful'
}

Response (400 Error):
- 'Out of stock on item_id: {item_id}'
- 'User out of credit or payment service error'
- Other validation errors
```

## Checkout Flow with 2PC

### Phase 1: PREPARE
1. Generate unique transaction ID
2. Create transaction record in order service (status: INITIATED)
3. Send prepare request to stock service
   - Stock service verifies availability and creates reservations
   - Returns success/failure
4. Send prepare request to payment service
   - Payment service verifies credit and creates hold
   - Returns success/failure
5. If either service disagrees:
   - Send abort request to both services
   - Return error to client
   - Transaction marked as ABORTED
6. If both agree:
   - Update transaction status to PREPARED

### Phase 2: COMMIT
1. Send commit request to stock service
   - Stock service deducts the reserved items
   - Returns success/failure
2. Send commit request to payment service
   - Payment service deducts the held credit
   - Returns success/failure
3. Mark order as paid in order service
4. Update transaction status to COMMITTED
5. Return success to client

## Benefits of 2PC

1. **Atomicity**: Either all changes are applied or none
2. **Consistency**: No partial updates across services
3. **Handles Service Failures**: 
   - If a service fails during prepare, others are rolled back
   - Reservations are lightweight and can be cleaned up
4. **No Manual Rollback Needed**: Old code relied on manual rollback endpoints; 2PC handles it automatically

## Error Handling

- **Stock Prepare Fails**: Abort both, return out-of-stock error
- **Payment Prepare Fails**: Abort stock if it succeeded, return insufficient credit error
- **Stock Commit Fails**: Abort payment, return critical error
- **Payment Commit Fails**: All changes already made, return error (log for manual investigation)

## Testing Recommendations

1. **Success Case**: Create order with available stock and sufficient user credit
   ```bash
   POST /checkout/<order_id>
   # Should return 200 with transaction_id
   ```

2. **Out of Stock**: Create order requiring more stock than available
   ```bash
   POST /checkout/<order_id>
   # Should return 400 with out-of-stock error
   # Database should show ABORTED transaction
   ```

3. **Insufficient Credit**: Create order with cost exceeding user credit
   ```bash
   POST /checkout/<order_id>
   # Should return 400 with insufficient credit error
   # Database should show ABORTED transaction
   # Stock reservations should be cleaned up
   ```

4. **Concurrent Transactions**: Run multiple checkouts simultaneously
   - Each transaction should have unique transaction_id
   - Reservations should be properly isolated

## Database Query Examples

### Check Transaction Status
```sql
SELECT * FROM order_transactions WHERE order_id = '<order_id>';
```

### View Stock Reservations (before commit)
```sql
SELECT * FROM stock_reservations WHERE transaction_id = '<transaction_id>';
```

### View Payment Holds (before commit)
```sql
SELECT * FROM credit_holds WHERE transaction_id = '<transaction_id>';
```

## Migration Notes

The old `/subtract` and `/add` endpoints in stock service still exist and work independently. They don't use the 2PC protocol. For production use, consider:

1. Deprecating old endpoints
2. Updating all checkout logic to use 2PC
3. Adding cleanup jobs to remove stale transactions/reservations after timeout period

## Future Enhancements

1. **Timeout Handling**: Automatically abort transactions that exceed timeout
2. **Compensation Logic**: Add compensation transactions for distributed rollback
3. **Event Log**: Add event sourcing for audit trail
4. **Idempotency**: Add idempotency keys to handle duplicate requests
5. **Monitoring**: Add metrics for prepare/commit success rates
