#!/usr/bin/env python3
"""
Script to rename collections in MongoDB
- Rename 'subscribers' to 'subscriber_profiles'
- Rename 'telecom_plans' to 'subscribers'
"""

import os
from database import DatabaseManager
from dotenv import load_dotenv

load_dotenv()

def main():
    """Rename collections in MongoDB"""
    # Ensure we're using MongoDB
    os.environ['DATABASE_TYPE'] = 'mongodb'
    
    # Initialize database manager
    db_manager = DatabaseManager()
    db = db_manager.database
    
    print("🔄 Renaming collections in vil_dxl_dds database...")
    
    try:
        # Check if collections exist
        collections = db.list_collection_names()
        print(f"📋 Current collections: {collections}")
        
        # Rename subscribers to subscriber_profiles
        if 'subscribers' in collections:
            db.subscribers.rename('subscriber_profiles')
            print("✅ Renamed 'subscribers' to 'subscriber_profiles'")
        else:
            print("⚠️  'subscribers' collection not found")
        
        # Rename telecom_plans to subscribers
        if 'telecom_plans' in collections:
            db.telecom_plans.rename('subscribers')
            print("✅ Renamed 'telecom_plans' to 'subscribers'")
        else:
            print("⚠️  'telecom_plans' collection not found")
        
        # Show final collections
        final_collections = db.list_collection_names()
        print(f"\n📋 Final collections: {final_collections}")
        
        # Show counts
        for collection_name in final_collections:
            count = db[collection_name].count_documents({})
            print(f"   - {collection_name}: {count} documents")
        
        print("\n🎉 Collection renaming complete!")
        
    except Exception as e:
        print(f"❌ Error renaming collections: {str(e)}")

if __name__ == "__main__":
    main()
