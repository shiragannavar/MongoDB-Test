#!/usr/bin/env python3
"""
MongoDB Connection Test Script
"""

import os
from pymongo import MongoClient
from dotenv import load_dotenv
import sys

load_dotenv()

def test_mongodb_connection():
    """Test MongoDB Atlas connection"""
    print("🔍 Testing MongoDB Connection...")
    
    # Get connection details
    uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
    database_name = os.getenv('MONGODB_DATABASE', 'user_profiles')
    
    print(f"📡 URI: {uri[:50]}...")  # Show first 50 chars for security
    print(f"🗄️  Database: {database_name}")
    
    try:
        # Test connection
        print("\n⏳ Connecting to MongoDB...")
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        
        # Test server info
        server_info = client.server_info()
        print(f"✅ Connected successfully!")
        print(f"📊 MongoDB Version: {server_info.get('version', 'Unknown')}")
        
        # Test database access
        db = client[database_name]
        collections = db.list_collection_names()
        print(f"📁 Collections in '{database_name}': {collections}")
        
        # Test users collection
        users_collection = db.users
        user_count = users_collection.count_documents({})
        print(f"👥 Users in collection: {user_count}")
        
        if user_count > 0:
            sample_user = users_collection.find_one()
            print(f"📄 Sample user: {sample_user.get('name', 'N/A')} ({sample_user.get('email', 'N/A')})")
        
        client.close()
        return True
        
    except Exception as e:
        print(f"❌ Connection failed: {str(e)}")
        
        # Specific error handling
        if "authentication failed" in str(e).lower():
            print("🔐 Authentication issue - check username/password in connection string")
        elif "connection refused" in str(e).lower():
            print("🚫 Connection refused - check network/firewall settings")
        elif "timeout" in str(e).lower():
            print("⏰ Connection timeout - check network connectivity")
        elif "dns" in str(e).lower():
            print("🌐 DNS resolution issue - check cluster hostname")
        
        return False

def check_environment():
    """Check environment variables"""
    print("\n🔧 Environment Check:")
    
    required_vars = ['MONGODB_URI', 'MONGODB_DATABASE']
    for var in required_vars:
        value = os.getenv(var)
        if value:
            if var == 'MONGODB_URI':
                # Mask sensitive parts
                masked = value[:20] + "***" + value[-10:] if len(value) > 30 else "***"
                print(f"✅ {var}: {masked}")
            else:
                print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: Not set")

if __name__ == "__main__":
    print("🧪 MongoDB Connection Diagnostics")
    print("=" * 40)
    
    check_environment()
    success = test_mongodb_connection()
    
    if success:
        print("\n🎉 MongoDB connection is working!")
        sys.exit(0)
    else:
        print("\n💡 Troubleshooting Tips:")
        print("1. Check MongoDB Atlas cluster is running")
        print("2. Verify IP whitelist includes your current IP")
        print("3. Confirm database user credentials")
        print("4. Test with MongoDB Compass")
        sys.exit(1)
