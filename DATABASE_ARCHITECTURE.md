# Database Architecture for EnhancedPutSQL

## Database Types Overview

The EnhancedPutSQL processor is designed to work with a **mixed database environment**:

### SSE Engine Database (Metadata Store)
- **Type**: MySQL
- **Purpose**: Stores environment configurations and operation metadata
- **Connection**: Via "SSE Engine DBCP Service" controller service
- **Contains**: 
  - Operations table
  - Environment configurations
  - Database connection details for source/target systems

### Source Database
- **Type**: Oracle (typically)
- **Purpose**: The actual source database being validated
- **Connection**: Via "Source DBCP Service" controller service
- **Usage**: Validation compares this DBCP's connection details against expected configuration

### Target Database
- **Type**: Oracle (typically)
- **Purpose**: The actual target database being validated and where SQL is executed
- **Connection**: Via "Target DBCP Service" AND "JDBC Connection Pool" controller services
- **Usage**: 
  - Validated against expected configuration
  - Used for SQL statement execution

## Controller Service Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  NiFi Controller Services                │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  1. SSE Engine DBCP (MySQL)                              │
│     └─> jdbc:mysql://engine-host:3306/sse_engine        │
│                                                           │
│  2. Source DBCP (Oracle)                                 │
│     └─> jdbc:oracle:thin:@source-host:1521/sourcedb     │
│                                                           │
│  3. Target DBCP (Oracle)                                 │
│     └─> jdbc:oracle:thin:@target-host:1521/targetdb     │
│                                                           │
│  4. JDBC Connection Pool (Oracle - same as Target)       │
│     └─> jdbc:oracle:thin:@target-host:1521/targetdb     │
│                                                           │
└─────────────────────────────────────────────────────────┘
```

## How the Processor Uses Each DBCP

### 1. SSE Engine DBCP Service (MySQL)
```sql
-- Queries the engine database to get expected configurations
SELECT DISTINCT
    o.id as operation_id,
    o.environment_id,
    e.name as environment_name,
    sc.database_type as source_db_type,    -- Returns "oracle"
    sc.host as source_host,                -- e.g., "source-host"
    sc.port as source_port,                -- e.g., 1521
    sc.db_name as source_database,         -- e.g., "sourcedb"
    tc.database_type as target_db_type,    -- Returns "oracle"
    tc.host as target_host,                -- e.g., "target-host"
    tc.port as target_port,                -- e.g., 1521
    tc.db_name as target_database          -- e.g., "targetdb"
FROM operations o
INNER JOIN environment_configurations e ON o.environment_id = e.id
INNER JOIN database_configurations sc ON e.source_config_id = sc.id
INNER JOIN database_configurations tc ON e.target_config_id = tc.id
WHERE o.id = ?
```

### 2. Source DBCP Service (Oracle)
```java
// Processor extracts actual connection URL from the controller service
Connection conn = sourceDbcp.getConnection();
String actualSourceUrl = conn.getMetaData().getURL();
// Returns: jdbc:oracle:thin:@source-host:1521/sourcedb
```

### 3. Target DBCP Service (Oracle)
```java
// Processor extracts actual connection URL from the controller service
Connection conn = targetDbcp.getConnection();
String actualTargetUrl = conn.getMetaData().getURL();
// Returns: jdbc:oracle:thin:@target-host:1521/targetdb
```

### 4. JDBC Connection Pool (Oracle)
```java
// Used by standard PutSQL logic to execute SQL statements
// This is typically the same controller service as Target DBCP
// Only used AFTER validation passes
```

## Validation Flow with Mixed Databases

```
Step 1: Query MySQL (SSE Engine)
  ┌──────────────────────────────┐
  │ SSE Engine DBCP (MySQL)      │
  │ jdbc:mysql://...             │
  └────────┬─────────────────────┘
           │
           v
  Get expected Oracle configurations:
    - source_db_type: "oracle"
    - source_host: "source-host"
    - source_port: 1521
    - target_db_type: "oracle"
    - target_host: "target-host"
    - target_port: 1521

Step 2: Build Expected Oracle URLs
  Expected Source: jdbc:oracle:thin:@source-host:1521/sourcedb
  Expected Target: jdbc:oracle:thin:@target-host:1521/targetdb

Step 3: Extract Actual Oracle URLs
  ┌──────────────────────────────┐
  │ Source DBCP (Oracle)         │
  │ jdbc:oracle:thin:@...        │
  └────────┬─────────────────────┘
           │
           v
  Actual Source: jdbc:oracle:thin:@source-host:1521/sourcedb

  ┌──────────────────────────────┐
  │ Target DBCP (Oracle)         │
  │ jdbc:oracle:thin:@...        │
  └────────┬─────────────────────┘
           │
           v
  Actual Target: jdbc:oracle:thin:@target-host:1521/targetdb

Step 4: Compare URLs
  Source Match? YES ✓
  Target Match? YES ✓

Step 5: Execute SQL on Oracle
  ┌──────────────────────────────┐
  │ JDBC Connection Pool (Oracle)│
  │ jdbc:oracle:thin:@...        │
  └────────┬─────────────────────┘
           │
           v
  Execute: INSERT INTO table VALUES (...)
```

## Key Points

### 1. Database Type Independence
The processor **doesn't care about database types** because:
- All connections are managed by NiFi's DBCP Controller Services
- Controller Services abstract the database-specific details
- The processor just compares JDBC URLs (strings)

### 2. URL Building Logic
The processor supports multiple database types:
```java
switch (dbType.toLowerCase()) {
    case "oracle":   return "jdbc:oracle:thin:@%s:%d/%s";
    case "mysql":    return "jdbc:mysql://%s:%d/%s";
    case "postgresql": return "jdbc:postgresql://%s:%d/%s";
    case "sqlserver": return "jdbc:sqlserver://%s:%d;databaseName=%s";
}
```

### 3. Mixed Environment is Normal
Typical production setup:
- **Engine**: MySQL (lightweight, fast queries for metadata)
- **Source/Target**: Oracle (enterprise data systems)
- **Processor**: Handles both seamlessly via controller services

## Configuration Example

### Controller Services Configuration

```yaml
# 1. SSE Engine DBCP - MySQL
Name: SSE_Engine_MySQL_DBCP
Type: DBCPConnectionPool
Properties:
  - Database Connection URL: jdbc:mysql://mysql-engine:3306/sse_engine
  - Database Driver Class Name: com.mysql.cj.jdbc.Driver
  - Database User: engine_user
  - Password: ********

# 2. Source Oracle DBCP
Name: Source_Oracle_DBCP
Type: DBCPConnectionPool
Properties:
  - Database Connection URL: jdbc:oracle:thin:@oracle-source:1521/SOURCEDB
  - Database Driver Class Name: oracle.jdbc.OracleDriver
  - Database User: source_user
  - Password: ********

# 3. Target Oracle DBCP
Name: Target_Oracle_DBCP
Type: DBCPConnectionPool
Properties:
  - Database Connection URL: jdbc:oracle:thin:@oracle-target:1521/TARGETDB
  - Database Driver Class Name: oracle.jdbc.OracleDriver
  - Database User: target_user
  - Password: ********
```

### Processor Configuration

```yaml
EnhancedPutSQL Properties:
  # Validation enabled
  - Enable Validation: true
  
  # Validation properties
  - Operation ID: ${operation.id}
  - SSE Engine DBCP Service: SSE_Engine_MySQL_DBCP     # MySQL
  - Source DBCP Service: Source_Oracle_DBCP            # Oracle
  - Target DBCP Service: Target_Oracle_DBCP            # Oracle
  - Validation Mode: STRICT
  
  # Standard PutSQL properties
  - JDBC Connection Pool: Target_Oracle_DBCP           # Oracle (reuse)
  - SQL Statement: ${sql.statement}
  - Rollback On Failure: true
```

## SSE Engine Database Schema (MySQL)

```sql
-- MySQL tables storing Oracle connection details
CREATE TABLE operations (
    id VARCHAR(255) PRIMARY KEY,
    environment_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE environment_configurations (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    source_config_id BIGINT,
    target_config_id BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE database_configurations (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    database_type VARCHAR(50),      -- "oracle", "mysql", "postgresql", etc.
    host VARCHAR(255),
    port INT,
    db_name VARCHAR(255),
    deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Example data
INSERT INTO database_configurations VALUES
(1, 'oracle', 'oracle-source.prod.example.com', 1521, 'SOURCEDB', FALSE, NOW()),
(2, 'oracle', 'oracle-target.prod.example.com', 1521, 'TARGETDB', FALSE, NOW());

INSERT INTO environment_configurations VALUES
(1, 'Production', 1, 2, NOW());

INSERT INTO operations VALUES
('OP-12345', 1, NOW());
```

## Benefits of This Architecture

### 1. Centralized Metadata (MySQL)
- Fast queries for environment configuration
- Easy to update connection details
- Single source of truth

### 2. Flexible Data Systems (Oracle)
- Use enterprise-grade databases for actual data
- Support for complex transactions
- Leverage existing Oracle infrastructure

### 3. Separation of Concerns
- Engine DB: Configuration and orchestration
- Source/Target DBs: Business data
- Clear boundaries and responsibilities

### 4. NiFi Controller Services Handle Everything
- Driver loading (MySQL JDBC vs Oracle JDBC)
- Connection pooling
- Transaction management
- The processor just validates and executes

## Summary

| Component | Database Type | Purpose | JDBC URL Pattern |
|-----------|--------------|---------|------------------|
| SSE Engine | MySQL | Metadata/Config | `jdbc:mysql://host:3306/db` |
| Source | Oracle | Data Source | `jdbc:oracle:thin:@host:1521/db` |
| Target | Oracle | Data Target | `jdbc:oracle:thin:@host:1521/db` |
| Execution | Oracle (same as Target) | SQL Execution | `jdbc:oracle:thin:@host:1521/db` |

The processor is **database-agnostic** - it works with any database supported by NiFi's DBCP Controller Services. The mixed MySQL/Oracle environment is handled transparently through the controller service abstraction.

