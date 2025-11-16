# EnhancedPutSQL Processor Behavior

## Property Visibility Based on Master Switch

### When `Enable Validation = false` (Default)
```
┌─────────────────────────────────────────┐
│      EnhancedPutSQL Properties          │
├─────────────────────────────────────────┤
│ ✓ Enable Validation: false              │
│                                         │
│ === Standard PutSQL Properties ===      │
│ ✓ JDBC Connection Pool                  │
│ ✓ SQL Statement                         │
│ ✓ Database Session AutoCommit           │
│ ✓ Support Fragmented Transactions       │
│ ✓ Transaction Timeout                   │
│ ✓ Batch Size                            │
│ ✓ Obtain Generated Keys                 │
│ ✓ Rollback On Failure                   │
└─────────────────────────────────────────┘
```
**Behavior**: Works exactly like standard PutSQL processor

### When `Enable Validation = true`
```
┌─────────────────────────────────────────┐
│      EnhancedPutSQL Properties          │
├─────────────────────────────────────────┤
│ ✓ Enable Validation: true               │
│                                         │
│ === Validation Properties (NEW) ===     │
│ ✓ Operation ID: ${operation.id}         │
│ ✓ SSE Engine DBCP Service               │
│ ✓ Source DBCP Service                   │
│ ✓ Target DBCP Service                   │
│ ✓ Validation Mode: STRICT               │
│                                         │
│ === Standard PutSQL Properties ===      │
│ ✓ JDBC Connection Pool                  │
│ ✓ SQL Statement                         │
│ ✓ Database Session AutoCommit           │
│ ✓ Support Fragmented Transactions       │
│ ✓ Transaction Timeout                   │
│ ✓ Batch Size                            │
│ ✓ Obtain Generated Keys                 │
│ ✓ Rollback On Failure                   │
└─────────────────────────────────────────┘
```
**Behavior**: Validates environment before executing SQL

## The Three DBCP Services Explained

When validation is enabled, you configure **4 DBCP services** total:

### 1. SSE Engine DBCP Service (Validation Only)
```
Purpose: Query environment configurations
Connects to: SSE Engine database (metadata)
Used for: Looking up expected source/target DB details
```

### 2. Source DBCP Service (Validation Only)
```
Purpose: Validate source database
Connects to: Your actual source database
Used for: Comparing actual connection against expected
```

### 3. Target DBCP Service (Validation Only)
```
Purpose: Validate target database
Connects to: Your actual target database
Used for: Comparing actual connection against expected
```

### 4. JDBC Connection Pool (Standard PutSQL - Required)
```
Purpose: Execute SQL statements
Connects to: The database where SQL will be executed
Used for: Actual SQL execution (usually same as Target DBCP)
```

## Execution Flow

### Without Validation (Enable Validation = false)
```
┌──────────┐
│ FlowFile │
└────┬─────┘
     │
     v
┌─────────────────┐
│ EnhancedPutSQL  │
│  (Standard Mode)│
└────┬───────────┘
     │
     v
┌──────────────────┐
│ Execute SQL      │
│ (JDBC Conn Pool) │
└────┬─────────────┘
     │
     ├──────> success
     ├──────> failure
     └──────> retry
```

### With Validation - STRICT Mode (Enable Validation = true)
```
┌──────────┐
│ FlowFile │
└────┬─────┘
     │
     v
┌─────────────────────────────┐
│ EnhancedPutSQL              │
│  (Validation Mode)          │
└────┬────────────────────────┘
     │
     v
┌─────────────────────────────┐
│ Step 1: Get Operation ID    │
│ from FlowFile attributes    │
└────┬────────────────────────┘
     │
     v
┌──────────────────────────────┐
│ Step 2: Query SSE Engine     │
│ (SSE Engine DBCP Service)    │
│ - Get expected source DB     │
│ - Get expected target DB     │
└────┬─────────────────────────┘
     │
     v
┌──────────────────────────────┐
│ Step 3: Extract Actual URLs  │
│ - From Source DBCP Service   │
│ - From Target DBCP Service   │
└────┬─────────────────────────┘
     │
     v
┌──────────────────────────────┐
│ Step 4: Compare URLs         │
│ Actual vs Expected           │
└────┬─────────────────────────┘
     │
     ├─── Match? ──┐
     │             │
     NO           YES
     │             │
     v             v
┌─────────────┐  ┌──────────────────┐
│ Add failure │  │ Add success      │
│ attributes  │  │ attributes       │
└────┬────────┘  └────┬─────────────┘
     │                │
     v                v
validation_failed   ┌──────────────────┐
                    │ Execute SQL      │
                    │ (JDBC Conn Pool) │
                    └────┬─────────────┘
                         │
                         ├──────> success
                         ├──────> failure
                         └──────> retry
```

### With Validation - WARNING Mode
```
Same as STRICT, but when validation fails:
- Log WARNING message
- Continue to Execute SQL (do NOT route to validation_failed)
```

## FlowFile Attributes Added

### On Validation Success
```yaml
validation.passed: "true"
validation.environment.id: "12345"
validation.environment.name: "Production"
validation.timestamp: "1700000000000"
```

### On Validation Failure (STRICT)
```yaml
validation.passed: "false"
validation.error: "Controller service mismatch for environment 'Production' (ID: 12345). Source match: false, Target match: true."
validation.timestamp: "1700000000000"
```

## URL Comparison Logic

The processor compares JDBC URLs by:
1. Extracting actual URL from DBCP metadata
2. Building expected URL from SSE Engine configuration
3. Normalizing both URLs (remove query params, trailing slashes, lowercase)
4. Case-insensitive comparison

### Example Comparison
```
SSE Engine Config:
  source_db_type: "mysql"
  source_host: "mysql-prod.example.com"
  source_port: 3306
  source_database: "app_db"

Expected URL (built):
  jdbc:mysql://mysql-prod.example.com:3306/app_db

Actual URL (from Source DBCP):
  jdbc:mysql://mysql-prod.example.com:3306/app_db?useSSL=true

Normalized URLs:
  Expected: jdbc:mysql://mysql-prod.example.com:3306/app_db
  Actual:   jdbc:mysql://mysql-prod.example.com:3306/app_db

Result: ✓ MATCH
```

## Use Cases

### Use Case 1: Development/Testing (No Validation)
```
Enable Validation: false
→ Quick testing without validation overhead
→ Backward compatible with existing flows
```

### Use Case 2: Production (STRICT Validation)
```
Enable Validation: true
Validation Mode: STRICT
→ Prevent SQL execution against wrong databases
→ Enforce environment safety
→ Route mismatches to validation_failed for review
```

### Use Case 3: Migration/Transition (WARNING Validation)
```
Enable Validation: true
Validation Mode: WARNING
→ Monitor for environment mismatches
→ Log warnings for review
→ Continue operation (no disruption)
→ Useful during migration phase
```

## Configuration Tips

### Tip 1: Operation ID from FlowFile
Most common pattern:
```
Operation ID: ${operation.id}
```
Assumes upstream processors set `operation.id` attribute.

### Tip 2: DBCP Service Reuse
The "Target DBCP Service" (validation) and "JDBC Connection Pool" (execution) 
are often the same service:
```
Target DBCP Service: My_Target_DB
JDBC Connection Pool: My_Target_DB  (same service)
```

### Tip 3: Monitoring
Set up monitoring for:
- `validation_failed` relationship (alerts)
- FlowFiles with `validation.passed = "false"`
- Log messages containing "Environment validation warning"

## Summary

| Aspect | Without Validation | With Validation |
|--------|-------------------|-----------------|
| Properties | Standard PutSQL only | Standard + 5 validation |
| DBCP Services | 1 (for SQL execution) | 4 (1 engine + 2 validate + 1 execute) |
| Execution | Direct SQL execution | Validate → Execute |
| Relationships | 3 (success/failure/retry) | 4 (+ validation_failed) |
| Use Case | Development, Testing | Production, Safety |
| Backward Compatible | N/A | Yes (when disabled) |

