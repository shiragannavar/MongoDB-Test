#!/usr/bin/env python3
"""
MongoDB to DataStax HCD Migration Script

This script reads subscriber data from MongoDB in batches and writes them to DataStax HCD.
Uses single-threaded processing with detailed logging for each batch operation.
"""

import os
import sys
import time
from datetime import datetime
from typing import List, Dict, Any
import logging

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
import pymongo
from astrapy import DataAPIClient
from astrapy.constants import Environment

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('migration/migration.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MongoToHCDMigrator:
    def __init__(self, batch_size: int = 100):
        """
        Initialize the migrator with batch processing capability
        
        Args:
            batch_size: Number of documents to process in each batch
        """
        self.batch_size = batch_size
        self.mongodb_client = None
        self.mongodb_db = None
        self.hcd_client = None
        self.hcd_db = None
        self.hcd_collection = None
        
        logger.info("🚀 MongoDB to DataStax HCD Migration Script Initialized")
        logger.info(f"📦 Batch Size: {batch_size}")
    
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
            
            # Create token for authentication
            token = f"{username}:{password}"
            
            self.hcd_client = DataAPIClient(environment=Environment.HCD)
            database = self.hcd_client.get_database(api_endpoint, token=token)
            
            # Ensure keyspace exists
            try:
                database.get_database_admin().create_keyspace(keyspace)
                logger.info(f"📁 Created keyspace: {keyspace}")
            except Exception:
                logger.info(f"📁 Keyspace already exists: {keyspace}")
            
            # Get database with keyspace
            self.hcd_db = database.get_database_admin().get_database(keyspace=keyspace)
            
            # Create or get subscribers collection
            try:
                self.hcd_collection = self.hcd_db.create_collection("subscribers")
                logger.info("📋 Created 'subscribers' collection in HCD")
            except Exception:
                self.hcd_collection = self.hcd_db.get_collection("subscribers")
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
        Write a batch of documents to DataStax HCD
        
        Args:
            batch: List of documents to write
            batch_number: Current batch number for logging
            
        Returns:
            Number of successfully written documents
        """
        try:
            batch_name = f"BATCH_{batch_number:03d}"
            logger.info(f"✍️  Writing {batch_name} to DataStax HCD ({len(batch)} documents)")
            
            success_count = 0
            
            for i, doc in enumerate(batch):
                try:
                    self.hcd_collection.insert_one(doc)
                    success_count += 1
                    
                    # Log progress for every 10 documents
                    if (i + 1) % 10 == 0 or (i + 1) == len(batch):
                        logger.info(f"   📝 {batch_name}: {i + 1}/{len(batch)} documents written")
                        
                except Exception as doc_error:
                    hash_msisdn = doc.get('hashMsisdn', 'unknown')
                    logger.error(f"   ❌ {batch_name}: Failed to write document {hash_msisdn[:16]}...: {str(doc_error)}")
            
            logger.info(f"✅ {batch_name} completed: {success_count}/{len(batch)} documents written successfully")
            return success_count
            
        except Exception as e:
            logger.error(f"❌ Failed to write batch to HCD: {str(e)}")
            return 0
    
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
        
        # Process only first 100 documents
        max_docs = 100
        logger.info(f"📊 Processing first {max_docs} documents from MongoDB")
        logger.info(f"📦 Processing in batches of {self.batch_size}")
        
        total_batches = (max_docs + self.batch_size - 1) // self.batch_size
        logger.info(f"🔢 Total batches: {total_batches}")
        
        # Migration statistics
        total_migrated = 0
        total_errors = 0
        start_time = time.time()
        
        # Process each batch
        for batch_num in range(1, total_batches + 1):
            skip = (batch_num - 1) * self.batch_size
            
            # Calculate remaining documents to process
            remaining_docs = max_docs - skip
            if remaining_docs <= 0:
                break
                
            # Adjust batch size for last batch
            current_batch_size = min(self.batch_size, remaining_docs)
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🔄 Processing Batch {batch_num}/{total_batches}")
            logger.info(f"{'='*60}")
            
            # Read batch from MongoDB
            batch = self.read_mongodb_batch(skip, current_batch_size)
            
            if not batch:
                logger.warning(f"⚠️  Batch {batch_num} is empty, skipping...")
                continue
            
            # Write batch to HCD
            migrated_count = self.write_hcd_batch(batch, batch_num)
            
            total_migrated += migrated_count
            total_errors += (len(batch) - migrated_count)
            
            # Progress update
            progress = (batch_num / total_batches) * 100
            logger.info(f"📈 Progress: {progress:.1f}% ({batch_num}/{total_batches} batches)")
            
            # Small delay between batches to avoid overwhelming the system
            time.sleep(0.5)
        
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
    
    # Initialize migrator with batch size of 50
    migrator = MongoToHCDMigrator(batch_size=50)
    
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
