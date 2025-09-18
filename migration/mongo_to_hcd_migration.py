#!/usr/bin/env python3
"""
MongoDB to DataStax HCD Migration Script

This script reads subscriber data from MongoDB in batches and writes them to DataStax HCD.
Uses multi-threaded processing with insert_many for optimal performance and detailed logging.
"""

import os
import sys
import time
from datetime import datetime
from typing import List, Dict, Any
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
import pymongo
from astrapy import DataAPIClient
from astrapy.authentication import UsernamePasswordTokenProvider
from astrapy.constants import Environment

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MongoToHCDMigrator:
    def __init__(self, batch_size: int = 100, max_threads: int = 10):
        """
        Initialize the migrator with batch processing capability
        
        Args:
            batch_size: Number of documents to process in each batch
            max_threads: Maximum number of threads for parallel processing
        """
        self.batch_size = batch_size
        self.max_threads = max_threads
        self.mongodb_client = None
        self.mongodb_db = None
        self.hcd_client = None
        self.hcd_db = None
        self.hcd_collection = None
        self._lock = threading.Lock()
        
        logger.info("🚀 MongoDB to DataStax HCD Migration Script Initialized")
        logger.info(f"📦 Batch Size: {batch_size}")
        logger.info(f"🧵 Max Threads: {max_threads}")
    
    def connect_mongodb(self):
        """Connect to MongoDB database"""
        try:
            logger.info("🔌 Connecting to MongoDB...")
            
            mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
            mongodb_database = os.getenv('MONGODB_DATABASE', 'vil_dxl_dds')
            
            logger.info(f"   📍 URI: {mongodb_uri}")
            logger.info(f"   🗄️  Database: {mongodb_database}")
            
            self.mongodb_client = pymongo.MongoClient(mongodb_uri)
            self.mongodb_db = self.mongodb_client[mongodb_database]
            
            # Test connection
            self.mongodb_client.admin.command('ping')
            
            logger.info("✅ MongoDB connection established successfully")
            
            # Show a sample record for verification
            try:
                sample_record = self.mongodb_db.subscribers.find_one()
                if sample_record:
                    logger.info("📄 Sample subscriber record:")
                    logger.info(f"   🆔 Hash MSISDN: {sample_record.get('hashMsisdn', 'N/A')[:20]}...")
                    logger.info(f"   📡 Provider: {sample_record.get('provider', 'N/A')}")
                    logger.info(f"   📍 Circle ID: {sample_record.get('circleID', 'N/A')}")
                    logger.info(f"   📊 Status: {sample_record.get('status', 'N/A')}")
                    logger.info(f"   📅 Storage Date: {sample_record.get('dateofStorage', 'N/A')[:10]}...")
                    
                    # Show products count if available
                    products = sample_record.get('subscribedProductOffering', {}).get('product', [])
                    services = sample_record.get('subscribedProductOffering', {}).get('services', [])
                    logger.info(f"   📦 Products: {len(products)}, Services: {len(services)}")
                else:
                    logger.warning("⚠️  No subscriber records found in MongoDB")
            except Exception as e:
                logger.warning(f"⚠️  Could not fetch sample record: {str(e)}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {str(e)}")
            return False
    
    def connect_hcd(self):
        """Connect to DataStax HCD database"""
        try:
            logger.info("🔌 Connecting to DataStax HCD...")
            
            api_endpoint = os.getenv('HCD_API_ENDPOINT')
            username = os.getenv('HCD_USERNAME')
            password = os.getenv('HCD_PASSWORD')
            keyspace = os.getenv('HCD_KEYSPACE', 'default_keyspace')
            
            logger.info(f"   📍 Endpoint: {api_endpoint}")
            logger.info(f"   👤 Username: {username}")
            logger.info(f"   🔑 Keyspace: {keyspace}")
            
            if not all([api_endpoint, username, password]):
                raise ValueError("HCD configuration incomplete. Check HCD_API_ENDPOINT, HCD_USERNAME, and HCD_PASSWORD")
            
            # Create proper token provider for authentication
            token_provider = UsernamePasswordTokenProvider(username, password)
            
            self.hcd_client = DataAPIClient(environment=Environment.HCD)
            database = self.hcd_client.get_database(api_endpoint, token=token_provider)
            
            # Ensure keyspace exists
            try:
                database.get_database_admin().create_keyspace(keyspace)
                logger.info(f"📁 Created keyspace: {keyspace}")
            except Exception as e:
                logger.info(f"📁 Keyspace already exists or creation failed: {keyspace} - {str(e)}")
            
            # Get collection with keyspace (following the sample code pattern)
            try:
                self.hcd_collection = database.create_collection("subscribers", keyspace=keyspace)
                logger.info("📋 Created 'subscribers' collection in HCD")
            except Exception:
                self.hcd_collection = database.get_collection("subscribers", keyspace=keyspace)
                logger.info("📋 Using existing 'subscribers' collection in HCD")
            
            logger.info("✅ DataStax HCD connection established successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to DataStax HCD: {str(e)}")
            return False
    
    def read_mongodb_batch(self, skip: int, limit: int) -> List[Dict[str, Any]]:
        """
        Read a batch of documents from MongoDB
        
        Args:
            skip: Number of documents to skip
            limit: Number of documents to read
            
        Returns:
            List of subscriber documents
        """
        try:
            logger.info(f"📖 Reading batch from MongoDB (skip: {skip}, limit: {limit})")
            
            cursor = self.mongodb_db.subscribers.find().skip(skip).limit(limit)
            batch = list(cursor)
            
            logger.info(f"   📄 Retrieved {len(batch)} documents")
            
            # Remove MongoDB-specific _id fields
            for doc in batch:
                if '_id' in doc:
                    del doc['_id']
            
            return batch
            
        except Exception as e:
            logger.error(f"❌ Failed to read batch from MongoDB: {str(e)}")
            return []
    
    def write_hcd_batch(self, batch: List[Dict[str, Any]], batch_number: int) -> int:
        """
        Write a batch of documents to DataStax HCD using insert_many
        
        Args:
            batch: List of documents to write
            batch_number: Current batch number for logging
            
        Returns:
            Number of successfully written documents
        """
        try:
            batch_name = f"BATCH_{batch_number:03d}"
            thread_id = threading.current_thread().name
            
            with self._lock:
                logger.info(f"✍️  Writing {batch_name} to DataStax HCD ({len(batch)} documents) [Thread: {thread_id}]")
            
            # Use insert_many for better performance
            try:
                result = self.hcd_collection.insert_many(batch)
                success_count = len(result.inserted_ids) if hasattr(result, 'inserted_ids') else len(batch)
                
                # Silent success for insert_many - only log batch completion
                with self._lock:
                    logger.info(f"✅ {batch_name} completed: {success_count}/{len(batch)} documents written successfully [Thread: {thread_id}]")
                
                return success_count
                
            except Exception as batch_error:
                # Fallback to individual inserts if insert_many fails
                with self._lock:
                    logger.warning(f"⚠️  {batch_name}: insert_many failed, falling back to individual inserts: {str(batch_error)}")
                
                success_count = 0
                for i, doc in enumerate(batch):
                    try:
                        self.hcd_collection.insert_one(doc)
                        success_count += 1
                        # Silent success - no logging for successful inserts
                    except Exception as doc_error:
                        hash_msisdn = doc.get('hashMsisdn', 'unknown')
                        with self._lock:
                            logger.error(f"   ❌ {batch_name}: Failed to write document {hash_msisdn[:16]}...: {str(doc_error)}")
                            logger.error(f"   📄 Failed MongoDB Record: {doc}")
                
                with self._lock:
                    logger.info(f"✅ {batch_name} completed: {success_count}/{len(batch)} documents written successfully [Thread: {thread_id}]")
                
                return success_count
            
        except Exception as e:
            with self._lock:
                logger.error(f"❌ Failed to write batch to HCD: {str(e)}")
            return 0
    
    def _process_single_batch(self, batch_num: int, skip: int, current_batch_size: int) -> tuple:
        """
        Process a single batch in a separate thread
        
        Args:
            batch_num: Batch number for logging
            skip: Number of documents to skip
            current_batch_size: Size of current batch
            
        Returns:
            Tuple of (migrated_count, error_count)
        """
        try:
            with self._lock:
                logger.info(f"\n{'='*60}")
                logger.info(f"🔄 Processing Batch {batch_num} [Thread: {threading.current_thread().name}]")
                logger.info(f"{'='*60}")
            
            # Read batch from MongoDB
            batch = self.read_mongodb_batch(skip, current_batch_size)
            
            if not batch:
                with self._lock:
                    logger.warning(f"⚠️  Batch {batch_num} is empty, skipping...")
                return 0, 0
            
            # Write batch to HCD
            migrated_count = self.write_hcd_batch(batch, batch_num)
            error_count = len(batch) - migrated_count
            
            return migrated_count, error_count
            
        except Exception as e:
            with self._lock:
                logger.error(f"❌ Error processing batch {batch_num}: {str(e)}")
            return 0, current_batch_size
    
    def migrate_data(self):
        """
        Main migration process - reads from MongoDB in batches and writes to HCD
        """
        logger.info("🔄 Starting migration process...")
        
        # Connect to both databases
        if not self.connect_mongodb():
            logger.error("💥 Migration aborted: MongoDB connection failed")
            return False
        
        if not self.connect_hcd():
            logger.error("💥 Migration aborted: HCD connection failed")
            return False
        
        # Process first 10,000 documents
        max_docs = 10000
        logger.info(f"📊 Processing first {max_docs} documents from MongoDB")
        logger.info(f"📦 Processing in batches of {self.batch_size}")
        
        total_batches = (max_docs + self.batch_size - 1) // self.batch_size
        logger.info(f"🔢 Total batches: {total_batches}")
        
        # Migration statistics
        total_migrated = 0
        total_errors = 0
        start_time = time.time()
        
        # Prepare batch tasks for threading
        batch_tasks = []
        for batch_num in range(1, total_batches + 1):
            skip = (batch_num - 1) * self.batch_size
            
            # Calculate remaining documents to process
            remaining_docs = max_docs - skip
            if remaining_docs <= 0:
                break
                
            # Adjust batch size for last batch
            current_batch_size = min(self.batch_size, remaining_docs)
            batch_tasks.append((batch_num, skip, current_batch_size))
        
        logger.info(f"🧵 Using {min(self.max_threads, len(batch_tasks))} threads for parallel processing")
        
        # Process batches using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.max_threads, thread_name_prefix="HCD-Worker") as executor:
            # Submit all batch tasks
            future_to_batch = {}
            for batch_num, skip, current_batch_size in batch_tasks:
                future = executor.submit(self._process_single_batch, batch_num, skip, current_batch_size)
                future_to_batch[future] = batch_num
            
            # Process completed batches
            completed_batches = 0
            for future in as_completed(future_to_batch):
                batch_num = future_to_batch[future]
                try:
                    migrated_count, error_count = future.result()
                    total_migrated += migrated_count
                    total_errors += error_count
                    completed_batches += 1
                    
                    # Progress update
                    progress = (completed_batches / len(batch_tasks)) * 100
                    logger.info(f"📈 Progress: {progress:.1f}% ({completed_batches}/{len(batch_tasks)} batches completed)")
                    
                except Exception as e:
                    logger.error(f"❌ Batch {batch_num} failed with error: {str(e)}")
                    total_errors += self.batch_size
        
        # Final statistics
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info(f"\n{'='*60}")
        logger.info("🎉 MIGRATION COMPLETED")
        logger.info(f"{'='*60}")
        logger.info(f"📊 Total Documents Processed: {max_docs}")
        logger.info(f"✅ Successfully Migrated: {total_migrated}")
        logger.info(f"❌ Errors: {total_errors}")
        logger.info(f"⏱️  Duration: {duration:.2f} seconds")
        logger.info(f"🚀 Average Speed: {total_migrated/duration:.2f} docs/second")
        
        if total_errors == 0:
            logger.info("🎯 Migration completed successfully with no errors!")
        else:
            logger.warning(f"⚠️  Migration completed with {total_errors} errors. Check logs for details.")
        
        return total_errors == 0
    
    def cleanup(self):
        """Close database connections"""
        try:
            if self.mongodb_client:
                self.mongodb_client.close()
                logger.info("🔌 MongoDB connection closed")
        except Exception as e:
            logger.error(f"❌ Error closing MongoDB connection: {str(e)}")

def main():
    """Main function to run the migration"""
    logger.info("🌟 Starting MongoDB to DataStax HCD Migration")
    logger.info(f"📅 Migration started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize migrator with batch size of 100 and 10 threads
    migrator = MongoToHCDMigrator(batch_size=100, max_threads=10)
    
    try:
        # Run migration
        success = migrator.migrate_data()
        
        if success:
            logger.info("🎉 Migration completed successfully!")
            return 0
        else:
            logger.error("💥 Migration failed!")
            return 1
            
    except KeyboardInterrupt:
        logger.warning("⚠️  Migration interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"💥 Unexpected error during migration: {str(e)}")
        return 1
    finally:
        migrator.cleanup()

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
