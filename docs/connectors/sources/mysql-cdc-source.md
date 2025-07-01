# MySQL CDC Setup

This application implements MySQL CDC using MySQL binary log replication with **Quix Streams StatefulSource** for exactly-once processing and automatic recovery.

## Key Features

- **Quix Streams StatefulSource**: Built on Quix Streams' robust stateful source framework
- **Automatic State Management**: Integrated state store for binlog position and snapshot tracking
- **Exactly-Once Processing**: No data loss during application restarts or failures  
- **Initial Snapshot**: Optionally capture existing data before starting CDC
- **Automatic Recovery**: Seamlessly resume processing after interruptions
- **Change Buffering**: Batches changes for efficient Kafka publishing
- **Built-in Reliability**: Leverages Quix Streams' production-ready state management

## Prerequisites

1. MySQL version <=8.0

2. **MySQL Configuration**: Your MySQL server must have binary logging enabled with ROW format:
   ```ini
   # Add to MySQL configuration file (my.cnf or my.ini)
   [mysqld]
   server-id = 1
   log_bin = /var/log/mysql/mysql-bin.log
   binlog_expire_logs_seconds = 864000
   max_binlog_size = 100M
   binlog-format = ROW
   binlog_row_metadata = FULL
   binlog_row_image = FULL
   ```

3. **MySQL User Permissions**: The MySQL user needs REPLICATION SLAVE and REPLICATION CLIENT privileges:
   ```sql
   -- Create replication user
   CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'secure_password';
   
   -- Grant replication privileges for CDC
   GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
   
   -- Grant select for initial snapshot (if using snapshot feature)
   GRANT SELECT ON your_database.your_table TO 'cdc_user'@'%';
   
   FLUSH PRIVILEGES;
   ```

## Environment Variables

Set the following environment variables:

### Required MySQL Connection
- `MYSQL_HOST` - MySQL server hostname (e.g., localhost)
- `MYSQL_PORT` - MySQL server port (default: 3306)
- `MYSQL_USER` - MySQL username
- `MYSQL_PASSWORD` - MySQL password
- `MYSQL_DATABASE` - MySQL database name
- `MYSQL_SCHEMA` - MySQL database name (same as MYSQL_DATABASE)
- `MYSQL_TABLE` - Table name to monitor for changes

### Optional Configuration
- `MYSQL_SNAPSHOT_HOST` - MySQL host for initial snapshot (defaults to MYSQL_HOST). Use this to snapshot from a read replica
- `INITIAL_SNAPSHOT` - Set to "true" to perform initial snapshot (default: false)
- `SNAPSHOT_BATCH_SIZE` - Rows per snapshot batch (default: 1000)
- `FORCE_SNAPSHOT` - Set to "true" to force re-snapshot (default: false)

### Kafka Output
- `output` - Kafka topic name for publishing changes

## Example .env file

```env
# MySQL Connection
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=cdc_user
MYSQL_PASSWORD=secure_password
MYSQL_DATABASE=your_database
MYSQL_SCHEMA=your_database
MYSQL_TABLE=your_table

# Optional: Use read replica for initial snapshot
MYSQL_SNAPSHOT_HOST=replica.mysql.example.com

# Initial Snapshot Configuration
INITIAL_SNAPSHOT=true
SNAPSHOT_BATCH_SIZE=1000
FORCE_SNAPSHOT=false

# Kafka Output
output=cdc-changes-topic
```

## Quix Streams StatefulSource Architecture

The application uses Quix Streams' `StatefulSource` class which provides:

### Built-in State Management:
- **Automatic Persistence**: State is automatically saved to the configured state store
- **Exactly-Once Guarantees**: Built-in mechanisms ensure no data loss or duplication
- **Transactional Processing**: State changes are committed atomically with message production
- **Fault Tolerance**: Automatic recovery from failures with consistent state

### State Storage:
The StatefulSource manages two types of state:
1. **Binlog Position**: `binlog_position_{schema}_{table}`
   ```json
   {
     "log_file": "mysql-bin.000123",
     "log_pos": 45678,
     "timestamp": 1704067200.0
   }
   ```

2. **Snapshot Completion**: `snapshot_completed_{schema}_{table}`
   ```json
   {
     "completed_at": 1704067200.0,
     "schema": "database_name",
     "table": "table_name",
     "timestamp": "2024-01-01 12:00:00 UTC"
   }
   ```

### Benefits:
- ✅ **Production-Ready**: Built on Quix Streams' proven architecture
- ✅ **No Manual State Management**: Automatic state persistence and recovery
- ✅ **Exactly-Once Processing**: Guaranteed delivery semantics
- ✅ **Simplified Operations**: Reduced complexity compared to manual state management
- ✅ **Scalable**: Can be easily deployed and scaled in production environments

## Initial Snapshot

Capture existing table data before starting real-time CDC:

### Configuration:
```env
INITIAL_SNAPSHOT=true
SNAPSHOT_BATCH_SIZE=1000
MYSQL_SNAPSHOT_HOST=replica.mysql.example.com  # Optional
```

### Features:
- **Batched Processing**: Configurable batch sizes to handle large tables
- **Memory Efficient**: Processes data in chunks to avoid memory issues
- **Read Replica Support**: Use `MYSQL_SNAPSHOT_HOST` to snapshot from replica
- **Completion Tracking**: Marks snapshot completion in StatefulSource state store
- **Force Re-snapshot**: Use `FORCE_SNAPSHOT=true` to re-run if needed

### Snapshot Process:
1. Connects to snapshot host (or main host if not specified)
2. Processes table data in batches
3. Sends records with `"kind": "snapshot_insert"`
4. Marks completion in StatefulSource state store
5. Proceeds to real-time CDC

## Dependencies

Install the required Python packages:
```bash
pip install -r requirements.txt
```

The key dependencies are:
- `quixstreams` - Quix Streams library with StatefulSource support
- `pymysql` - MySQL database connector
- `mysql-replication` - MySQL binary log replication library

## Change Data Format

The MySQL CDC produces change events in the following format:

### Snapshot Insert Event
```json
{
  "kind": "snapshot_insert",
  "schema": "database_name",
  "table": "table_name", 
  "columnnames": ["col1", "col2"],
  "columnvalues": ["value1", "value2"],
  "oldkeys": {}
}
```

### INSERT Event
```json
{
  "kind": "insert",
  "schema": "database_name",
  "table": "table_name", 
  "columnnames": ["col1", "col2"],
  "columnvalues": ["value1", "value2"],
  "oldkeys": {}
}
```

### UPDATE Event
```json
{
  "kind": "update",
  "schema": "database_name", 
  "table": "table_name",
  "columnnames": ["col1", "col2"],
  "columnvalues": ["new_value1", "new_value2"],
  "oldkeys": {
    "keynames": ["col1", "col2"],
    "keyvalues": ["old_value1", "old_value2"]
  }
}
```

### DELETE Event
```json
{
  "kind": "delete",
  "schema": "database_name",
  "table": "table_name", 
  "columnnames": [],
  "columnvalues": [],
  "oldkeys": {
    "keynames": ["col1", "col2"],
    "keyvalues": ["deleted_value1", "deleted_value2"]
  }
}
```

## Running the Application

1. **Configure MySQL** with binary logging enabled
2. **Set environment variables** (see example above)
3. **Run the application**:
   ```bash
   python main.py
   ```

### Application Flow:
1. **StatefulSource Initialization**: Quix Streams creates the MySQL CDC source
2. **State Recovery**: Automatically loads saved binlog position and snapshot status
3. **Initial Snapshot** (if enabled and not completed):
   - Connects to snapshot host
   - Processes existing data in batches
   - Sends snapshot events to Kafka
   - Marks completion in state store
4. **Real-time CDC**:
   - Connects to MySQL binlog stream
   - Resumes from saved position (or current if first run)
   - Monitors specified table for changes
   - Buffers changes and publishes to Kafka every 500ms
   - Automatically commits state after successful delivery
5. **Automatic Recovery**: On restart, StatefulSource handles state recovery

### Monitoring:
- Check application logs for binlog position updates
- Monitor Quix Streams state store for position and snapshot data
- Verify Kafka topic for change events
- Use MySQL's `SHOW MASTER STATUS` to compare positions

## Troubleshooting

### Common Issues:

1. **Binary logging not enabled**:
   - Error: "Binary logging must be enabled for CDC"
   - Solution: Enable binlog in MySQL configuration and restart

2. **Insufficient privileges**:
   - Error: Access denied
   - Solution: Grant REPLICATION SLAVE, REPLICATION CLIENT privileges

3. **StatefulSource state issues**:
   - StatefulSource automatically handles state recovery
   - Check Quix Streams configuration and state store connectivity
   - Review application logs for state-related errors

4. **Snapshot issues**:
   - Check `MYSQL_SNAPSHOT_HOST` connectivity
   - Verify SELECT privileges on target table
   - Review batch size for memory constraints

### Best Practices:
- Use read replicas for initial snapshots on large tables
- Configure appropriate Quix Streams state store settings
- Set appropriate `SNAPSHOT_BATCH_SIZE` based on available memory
- Monitor Quix Streams metrics for source performance
- Ensure proper Kafka connectivity for reliable message delivery 


## Testing Locally

You can test your application using a locally emulated MySQL host via Docker
with all correct settings by:

1. Execute the following in terminal (just copy+paste) to run MySQL with the 
correct settings and set of test credentials:

```bash
TMPDIR=$(mktemp -d $HOME/mysql-cdc.XXXXXX)

cat > "$TMPDIR/custom-mysql.cnf" <<EOF
[mysqld]
server-id = 1
log_bin = /var/lib/mysql/mysql-bin.log
binlog_expire_logs_seconds = 864000
max_binlog_size = 100M
binlog-format = ROW
binlog_row_metadata = FULL
binlog_row_image = FULL
EOF

cat > "$TMPDIR/init-user.sql" <<EOF
CREATE DATABASE IF NOT EXISTS test_database;
USE test_database;
CREATE TABLE IF NOT EXISTS test_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'cdc_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON test_database.test_table TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
EOF

docker run --rm -d \
  --name mysql-cdc \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root_password \
  -e MYSQL_DATABASE=test_database \
  -v "$TMPDIR/custom-mysql.cnf":/etc/mysql/conf.d/custom.cnf:ro \
  -v "$TMPDIR/init-user.sql":/docker-entrypoint-initdb.d/init-user.sql:ro \
  mysql:8.0

echo "sleeping then deleting temp dir" 
sleep 30
rm -rf $TMPDIR
```

2. Connect Using:

```python
from quixstreams.sources.community.mysql_cdc import MySqlCdcSource

src = MySqlCdcSource(
    host="localhost",
    port=3306,
    user="cdc_user",
    password="cdc_password",
    database="test_database",
    table="test_table",
)
```
