#!/usr/bin/env python3
"""
Migration Verification Script

This script verifies that the migration from MongoDB to DataStax HCD was successful
by comparing document counts and sample data between both databases.
"""

import os
import sys
import logging
from typing import Dict, Any

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
import pymongo
from astrapy import DataAPIClient
from astrapy.constants import Environment

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MigrationVerifier:
    def __init__(self):
        """Initialize the migration verifier"""
        self.mongodb_client = None
        self.mongodb_db = None
        self.hcd_client = None
        self.hcd_db = None
        self.hcd_collection = None
        
        logger.info("🔍 Migration Verification Script Initialized")
    
    def connect_mongodb(self) -> bool:
        """Connect to MongoDB"""
        try:
            mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
            mongodb_database = os.getenv('MONGODB_DATABASE', 'vil_dxl_dds')
            
            self.mongodb_client = pymongo.MongoClient(mongodb_uri)
            self.mongodb_db = self.mongodb_client[mongodb_database]
            
            # Test connection
            self.mongodb_client.admin.command('ping')
            logger.info("✅ Connected to MongoDB")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to MongoDB: {str(e)}")
            return False
    
    def connect_hcd(self) -> bool:
        """Connect to DataStax HCD"""
        try:
            api_endpoint = os.getenv('HCD_API_ENDPOINT')
            username = os.getenv('HCD_USERNAME')
            password = os.getenv('HCD_PASSWORD')
            keyspace = os.getenv('HCD_KEYSPACE', 'default_keyspace')
            
            if not all([api_endpoint, username, password]):
                raise ValueError("HCD configuration incomplete")
            
            token = f"{username}:{password}"
            self.hcd_client = DataAPIClient(environment=Environment.HCD)
            database = self.hcd_client.get_database(api_endpoint, token=token)
            self.hcd_db = database.get_database_admin().get_database(keyspace=keyspace)
            self.hcd_collection = self.hcd_db.get_collection("subscribers")
            
            logger.info("✅ Connected to DataStax HCD")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to DataStax HCD: {str(e)}")
            return False
    
    def verify_counts(self) -> bool:
        """Verify document counts match between MongoDB and HCD"""
        try:
            mongo_count = self.mongodb_db.subscribers.count_documents({})
            hcd_count = len(list(self.hcd_collection.find({})))
            
            logger.info(f"📊 MongoDB subscribers: {mongo_count}")
            logger.info(f"📊 HCD subscribers: {hcd_count}")
            
            if mongo_count == hcd_count:
                logger.info("✅ Document counts match!")
                return True
            else:
                logger.error(f"❌ Document count mismatch: MongoDB={mongo_count}, HCD={hcd_count}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error verifying counts: {str(e)}")
            return False
    
    def verify_sample_data(self) -> bool:
        """Verify sample documents exist in both databases"""
        try:
            # Get a sample document from MongoDB
            mongo_sample = self.mongodb_db.subscribers.find_one({})
            if not mongo_sample:
                logger.warning("⚠️  No documents found in MongoDB")
                return True
            
            # Remove MongoDB _id for comparison
            mongo_hash = mongo_sample.get('hashMsisdn')
            if '_id' in mongo_sample:
                del mongo_sample['_id']
            
            # Find corresponding document in HCD
            hcd_sample = self.hcd_collection.find_one({"hashMsisdn": mongo_hash})
            
            if hcd_sample:
                # Remove HCD _id for comparison
                if '_id' in hcd_sample:
                    del hcd_sample['_id']
                
                # Compare key fields
                key_fields = ['provider', 'subscriptionType', 'status', 'circleID']
                matches = 0
                
                for field in key_fields:
                    if mongo_sample.get(field) == hcd_sample.get(field):
                        matches += 1
                    else:
                        logger.warning(f"⚠️  Field mismatch '{field}': MongoDB='{mongo_sample.get(field)}' vs HCD='{hcd_sample.get(field)}'")
                
                if matches == len(key_fields):
                    logger.info(f"✅ Sample document verification passed (hash: {mongo_hash[:16]}...)")
                    return True
                else:
                    logger.error(f"❌ Sample document verification failed: {matches}/{len(key_fields)} fields match")
                    return False
            else:
                logger.error(f"❌ Sample document not found in HCD (hash: {mongo_hash[:16]}...)")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error verifying sample data: {str(e)}")
            return False
    
    def verify_migration(self) -> bool:
        """Run complete migration verification"""
        logger.info("🔍 Starting migration verification...")
        
        # Connect to databases
        if not self.connect_mongodb() or not self.connect_hcd():
            return False
        
        # Verify counts
        counts_match = self.verify_counts()
        
        # Verify sample data
        data_matches = self.verify_sample_data()
        
        # Final result
        if counts_match and data_matches:
            logger.info("🎉 Migration verification PASSED!")
            logger.info("✅ All data successfully migrated from MongoDB to DataStax HCD")
            return True
        else:
            logger.error("💥 Migration verification FAILED!")
            logger.error("❌ Issues detected in the migration")
            return False
    
    def cleanup(self):
        """Close connections"""
        try:
            if self.mongodb_client:
                self.mongodb_client.close()
        except Exception:
            pass

def main():
    """Main verification function"""
    verifier = MigrationVerifier()
    
    try:
        success = verifier.verify_migration()
        return 0 if success else 1
    except Exception as e:
        logger.error(f"💥 Verification failed with error: {str(e)}")
        return 1
    finally:
        verifier.cleanup()

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
