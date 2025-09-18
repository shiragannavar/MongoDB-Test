# MongoDB to DataStax HCD Migration

This folder contains standalone migration scripts to transfer subscriber data from MongoDB to DataStax HCD.

## Files

- `mongo_to_hcd_migration.py` - Main migration script
- `migration.log` - Migration log file (created during execution)
- `README.md` - This documentation

## Features

- **Batch Processing**: Processes data in configurable batches (default: 50 documents)
- **Single-threaded**: Uses only 1 thread for reliable, sequential processing
- **Detailed Logging**: Comprehensive logs with progress tracking
- **Error Handling**: Continues processing even if individual documents fail
- **Progress Tracking**: Shows real-time progress and statistics
- **Data Integrity**: Preserves complete document structure during migration

## Usage

### Prerequisites

1. Ensure your `.env` file in the parent directory contains:
   ```
   # MongoDB Configuration
   MONGODB_URI=mongodb://localhost:27017/
   MONGODB_DATABASE=vil_dxl_dds
   
   # HCD Configuration
   HCD_API_ENDPOINT=http://localhost:8181
   HCD_USERNAME=your_username
   HCD_PASSWORD=your_password
   HCD_KEYSPACE=default_keyspace
   ```

2. Install required dependencies (from parent directory):
   ```bash
   pip install pymongo astrapy python-dotenv
   ```

### Run Migration

```bash
# From the main project directory
cd migration
python mongo_to_hcd_migration.py
```

### Sample Output

```
2025-09-18 17:15:00 - INFO - 🚀 MongoDB to DataStax HCD Migration Script Initialized
2025-09-18 17:15:00 - INFO - 📦 Batch Size: 50
2025-09-18 17:15:00 - INFO - 🔌 Connecting to MongoDB...
2025-09-18 17:15:00 - INFO - ✅ MongoDB connection established successfully
2025-09-18 17:15:00 - INFO - 📊 Found 10 subscriber records in MongoDB
2025-09-18 17:15:01 - INFO - 🔌 Connecting to DataStax HCD...
2025-09-18 17:15:01 - INFO - ✅ DataStax HCD connection established successfully
2025-09-18 17:15:01 - INFO - 🔄 Starting migration process...
2025-09-18 17:15:01 - INFO - 📊 Total documents to migrate: 10
2025-09-18 17:15:01 - INFO - 🔢 Total batches: 1
2025-09-18 17:15:01 - INFO - ============================================================
2025-09-18 17:15:01 - INFO - 🔄 Processing Batch 1/1
2025-09-18 17:15:01 - INFO - ============================================================
2025-09-18 17:15:01 - INFO - 📖 Reading batch from MongoDB (skip: 0, limit: 50)
2025-09-18 17:15:01 - INFO - ✍️  Writing BATCH_001 to DataStax HCD (10 documents)
2025-09-18 17:15:02 - INFO - ✅ BATCH_001 completed: 10/10 documents written successfully
2025-09-18 17:15:02 - INFO - ============================================================
2025-09-18 17:15:02 - INFO - 🎉 MIGRATION COMPLETED
2025-09-18 17:15:02 - INFO - ============================================================
2025-09-18 17:15:02 - INFO - 📊 Total Documents: 10
2025-09-18 17:15:02 - INFO - ✅ Successfully Migrated: 10
2025-09-18 17:15:02 - INFO - ❌ Errors: 0
2025-09-18 17:15:02 - INFO - ⏱️  Duration: 1.23 seconds
2025-09-18 17:15:02 - INFO - 🚀 Average Speed: 8.13 docs/second
```

## Configuration

### Batch Size
Modify the batch size in the `main()` function:
```python
migrator = MongoToHCDMigrator(batch_size=100)  # Change from default 50
```

### Logging
Logs are written to both console and `migration.log` file. The log level can be adjusted in the script.

## Error Handling

- **Connection Failures**: Script stops if it cannot connect to either database
- **Document Errors**: Individual document failures are logged but don't stop the migration
- **Batch Failures**: Failed batches are logged with detailed error information
- **Interruption**: Graceful handling of Ctrl+C interruption

## Data Integrity

- **Complete Structure**: Migrates entire document structure including nested objects
- **Field Preservation**: All fields are preserved exactly as they exist in MongoDB
- **ID Handling**: MongoDB `_id` fields are removed to avoid conflicts in HCD
- **Data Types**: All data types are preserved during migration
