# Enhanced PutSQL Bundle for NiFi 2.1.0

A custom Apache NiFi processor bundle that extends the standard PutSQL functionality with multi-database validation and routing capabilities.

## Features

- **Multi-Database Validation**: Validates source and target database configurations before processing
- **Operation Routing**: Ensures data integrity through operation-based validation
- **Advanced Transaction Management**: Supports fragmented transactions and batch processing
- **Comprehensive Error Handling**: Detailed error reporting and retry mechanisms
- **Generated Key Support**: Captures auto-generated primary keys from INSERT operations

## Requirements

- **Java**: 11 or higher
- **Apache NiFi**: 2.1.0
- **Maven**: 3.6 or higher
- **MySQL Connector**: 8.0.33 (provided)

## Building the Bundle

### Windows
```bash
build.bat
```

### Linux/Mac
```bash
chmod +x build.sh
./build.sh
```

### Manual Build
```bash
# Clean and compile
mvn clean compile package -DskipTests

# Run tests
mvn test

# Create NAR package
mvn package -pl nifi-enhanced-putsql-nar -am
```

## Deployment

### Prerequisites
- **NiFi 2.1.0** installed and running
- **Java 11** or higher
- **MySQL database** (if using MySQL connector)

### Step 1: Deploy the NAR File
1. **Copy the NAR file** to your NiFi installation:
   ```bash
   # Windows
   copy nifi-enhanced-putsql-nar\target\nifi-enhanced-putsql-nar-2.1.0.nar C:\nifi-2.1.0\lib\
   
   # Linux/Mac
   cp nifi-enhanced-putsql-nar/target/nifi-enhanced-putsql-nar-2.1.0.nar /path/to/nifi/lib/
   ```

2. **Restart NiFi**:
   ```bash
   # Windows
   C:\nifi-2.1.0\bin\nifi.bat stop
   C:\nifi-2.1.0\bin\nifi.bat start
   
   # Linux/Mac
   ./bin/nifi.sh stop
   ./bin/nifi.sh start
   ```

### Step 2: Verify Installation
1. **Open NiFi UI** (typically http://localhost:8080/nifi)
2. **Create a new processor** by dragging from the processor palette
3. **Search for "EnhancedPutSQL"** in the processor search
4. **The processor should appear** in the processor palette

### Step 3: Configure DBCP Services
Before using the processor, you need to configure the required DBCP services:

1. **Go to Controller Services** in NiFi
2. **Create DBCPConnectionPool services** for:
   - Source Database (if using source validation)
   - Target Database (if using target validation)  
   - Engine Database (for actual SQL execution)
3. **Configure each service** with appropriate database connection details
4. **Enable the services** before using the processor

## Configuration

### Required Properties

- **Source Database Connection Pool**: DBCP service for source database
- **Target Database Connection Pool**: DBCP service for target database  
- **Engine Database Connection Pool**: DBCP service for operation validation
- **Operation ID**: Unique identifier for the migration operation
- **JDBC Connection Pool**: DBCP service for SQL execution

### Optional Properties

- **SQL Statement**: Static SQL or from FlowFile content
- **Batch Size**: Number of FlowFiles per transaction (default: 100)
- **Auto Commit**: Database autocommit mode (default: false)
- **Support Fragmented Transactions**: Multi-part transaction support (default: true)
- **Obtain Generated Keys**: Capture auto-generated keys (default: false)
- **Rollback On Failure**: Transaction rollback behavior (default: false)

## Database Schema Requirements

The processor expects the following tables in your engine database:

### Operations Table
```sql
CREATE TABLE sse_engine.operations (
    operation_id VARCHAR(255) PRIMARY KEY,
    environment_id VARCHAR(255)
);
```

### Environment Configurations Table
```sql
CREATE TABLE sse_engine.environment_configurations (
    environment_id VARCHAR(255) PRIMARY KEY,
    source_config_id VARCHAR(255),
    target_config_id VARCHAR(255)
);
```

### Database Configurations Table
```sql
CREATE TABLE sse_engine.database_configurations (
    config_id VARCHAR(255) PRIMARY KEY,
    url VARCHAR(500),
    driver_name VARCHAR(255)
);
```

### Data Migration Records Table
```sql
CREATE TABLE sse_engine.data_migration_records (
    operation_id VARCHAR(255),
    queue_status VARCHAR(50)
);
```

## Usage Example

1. **Configure DBCP Services**: Set up your source, target, and engine database connection pools
2. **Add Processor**: Drag the EnhancedPutSQL processor to your canvas
3. **Configure Properties**: Set all required properties with appropriate values
4. **Connect FlowFiles**: Connect FlowFiles containing SQL statements and parameters
5. **Monitor Results**: Check success, retry, and failure relationships

## FlowFile Attributes

### Input Attributes
- `sql.args.N.type`: JDBC type for parameter N
- `sql.args.N.value`: Value for parameter N
- `sql.args.N.format`: Format for parameter N (optional)
- `fragment.identifier`: Transaction identifier (for fragmented transactions)
- `fragment.index`: Fragment index (for fragmented transactions)
- `fragment.count`: Total fragment count (for fragmented transactions)

### Output Attributes
- `sql.generated.key`: Auto-generated key (if enabled)
- `error.message`: Error message (on failure)
- `error.code`: SQL error code (on failure)
- `error.sql.state`: SQL state (on failure)

## Troubleshooting

### Common Issues

1. **Processor Not Appearing**: Ensure NAR file is in the correct lib directory and NiFi is restarted
2. **NoClassDefFoundError: org/apache/nifi/dbcp/DBCPService**: This error indicates the DBCP service is not available in NiFi. The NAR file should be ~32KB in size. Ensure NiFi 2.1.0 has the DBCP service bundle installed.
3. **Validation Failures**: Check that all DBCP services are properly configured and accessible
4. **Database Connection Issues**: Verify database URLs, credentials, and network connectivity
5. **Java Version Issues**: Ensure Java 11+ is being used
6. **ClassNotFoundException**: Ensure all required dependencies are included in the NAR file

### Logs

Check NiFi logs for detailed error messages:
- `logs/nifi-app.log`: Application-level errors
- `logs/nifi-user.log`: User-specific errors

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review NiFi logs for error details
3. Ensure all dependencies are properly configured

## License

This project is licensed under the Apache License 2.0.
