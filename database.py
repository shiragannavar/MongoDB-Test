import os
from typing import Dict, List, Optional, Any
from pymongo import MongoClient
from astrapy import DataAPIClient
from astrapy.authentication import UsernamePasswordTokenProvider
from astrapy.constants import Environment
from dotenv import load_dotenv

load_dotenv()

class DatabaseManager:
    def __init__(self):
        self.db_type = os.getenv('DATABASE_TYPE', 'mongodb')
        self.collection = None
        self._setup_connection()
    
    def _setup_connection(self):
        """Setup database connection based on configuration"""
        if self.db_type == 'mongodb':
            self._setup_mongodb()
        elif self.db_type == 'hcd':
            self._setup_hcd()
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def _setup_mongodb(self):
        """Setup MongoDB connection"""
        uri = os.getenv('MONGODB_URI', 'mongodb://localhost:27017/')
        database_name = os.getenv('MONGODB_DATABASE', 'vil_dxl_dds')
        
        self.client = MongoClient(uri)
        self.database = self.client[database_name]
        self.collection = self.database.users
    
    def _setup_hcd(self):
        """Setup HCD Data API connection"""
        api_endpoint = os.getenv('HCD_API_ENDPOINT')
        username = os.getenv('HCD_USERNAME')
        password = os.getenv('HCD_PASSWORD')
        keyspace = os.getenv('HCD_KEYSPACE', 'default_keyspace')
        
        if not all([api_endpoint, username, password]):
            raise ValueError("HCD configuration incomplete. Check HCD_API_ENDPOINT, HCD_USERNAME, and HCD_PASSWORD")
        
        token = UsernamePasswordTokenProvider(username, password)
        client = DataAPIClient(environment=Environment.HCD)
        database = client.get_database(api_endpoint, token=token)
        
        # Ensure keyspace exists
        try:
            database.get_database_admin().create_keyspace(keyspace)
        except Exception:
            # Keyspace might already exist
            pass
        
        # Get database with keyspace
        self.database = database.get_database_admin().get_database(keyspace=keyspace)
        
        # Create or get the users collection
        try:
            self.collection = self.database.create_collection("users")
        except Exception:
            # Collection might already exist
            self.collection = self.database.get_collection("users")
    
    def create_user(self, user_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new user"""
        # Ensure we have a UUID string as _id for both databases
        if '_id' not in user_data or not user_data['_id']:
            import uuid
            user_data['_id'] = str(uuid.uuid4())
        
        # Same API for both MongoDB and HCD
        result = self.collection.insert_one(user_data)
        return user_data
    
    def get_all_users(self) -> List[Dict[str, Any]]:
        """Get all users"""
        # Same API for both MongoDB and HCD
        cursor = self.collection.find({})
        return list(cursor)
    
    def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get user by ID"""
        # Same API for both MongoDB and HCD
        return self.collection.find_one({"_id": user_id})
    
    def delete_user(self, user_id: str) -> bool:
        """Delete user by ID"""
        # Same API for both MongoDB and HCD
        result = self.collection.delete_one({"_id": user_id})
        return result.deleted_count > 0
    
    def sync_mongodb_to_hcd(self) -> Dict[str, Any]:
        """Sync all MongoDB records to DataStax HCD"""
        if self.db_type != 'mongodb':
            return {'success': False, 'message': 'Can only sync from MongoDB to HCD'}
        
        try:
            # Get all users from MongoDB
            mongodb_users = self.get_all_users()
            
            if not mongodb_users:
                return {'success': True, 'message': 'No users to sync', 'synced_count': 0}
            
            # Create HCD connection
            hcd_manager = DatabaseManager()
            hcd_manager.db_type = 'hcd'
            hcd_manager._setup_hcd()
            
            synced_count = 0
            errors = []
            
            for user in mongodb_users:
                try:
                    # Remove MongoDB-specific _id if it exists and create new one
                    if '_id' in user:
                        del user['_id']
                    
                    # Create user in HCD
                    hcd_manager.create_user(user)
                    synced_count += 1
                except Exception as e:
                    errors.append(f"Error syncing user {user.get('name', 'unknown')}: {str(e)}")
            
            message = f'Successfully synced {synced_count} users to DataStax HCD'
            if errors:
                message += f'. {len(errors)} errors occurred.'
            
            return {
                'success': True,
                'message': message,
                'synced_count': synced_count,
                'errors': errors
            }
            
        except Exception as e:
            return {'success': False, 'message': f'Sync failed: {str(e)}'}
    
    def sync_subscribers_to_hcd(self) -> Dict[str, Any]:
        """Sync all MongoDB subscriber records to DataStax HCD"""
        if self.db_type != 'mongodb':
            return {'success': False, 'message': 'Can only sync from MongoDB to HCD'}
        
        try:
            # Get all subscribers from MongoDB
            from telecom_data_handler import TelecomDataHandler
            mongodb_handler = TelecomDataHandler()
            mongodb_subscribers = mongodb_handler.get_all_subscribers(limit=100)  # Limit to 100 for migration
            
            if not mongodb_subscribers:
                return {'success': True, 'message': 'No subscribers to sync', 'synced_count': 0}
            
            # Create HCD telecom handler
            hcd_manager = DatabaseManager()
            hcd_manager.db_type = 'hcd'
            hcd_manager._setup_hcd()
            
            # Create HCD subscribers collection
            try:
                hcd_subscribers_collection = hcd_manager.database.create_collection("subscribers")
            except Exception:
                hcd_subscribers_collection = hcd_manager.database.get_collection("subscribers")
            
            synced_count = 0
            errors = []
            
            for subscriber in mongodb_subscribers:
                try:
                    # Remove MongoDB-specific _id if it exists
                    if '_id' in subscriber:
                        del subscriber['_id']
                    
                    # Insert subscriber into HCD
                    hcd_subscribers_collection.insert_one(subscriber)
                    synced_count += 1
                except Exception as e:
                    hash_msisdn = subscriber.get('hashMsisdn', 'unknown')
                    errors.append(f"Error syncing subscriber {hash_msisdn[:16]}...: {str(e)}")
            
            message = f'Successfully synced {synced_count} subscribers to DataStax HCD'
            if errors:
                message += f'. {len(errors)} errors occurred.'
            
            return {
                'success': True,
                'message': message,
                'synced_count': synced_count,
                'errors': errors
            }
            
        except Exception as e:
            return {'success': False, 'message': f'Subscriber sync failed: {str(e)}'}
    
    def get_database_info(self) -> Dict[str, str]:
        """Get information about current database connection"""
        return {
            "type": self.db_type,
            "status": "connected"
        }
