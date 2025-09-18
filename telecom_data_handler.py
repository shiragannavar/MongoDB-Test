#!/usr/bin/env python3
"""
Telecom Subscriber Data Handler
Handles telecom subscriber records with encrypted personal data
"""

import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from database import DatabaseManager

class TelecomDataHandler:
    def __init__(self):
        self.db_manager = DatabaseManager()
        # Use subscribers collection for telecom data
        if hasattr(self.db_manager, 'database'):
            self.subscribers_collection = self.db_manager.database.subscribers
            self.plans_collection = self.db_manager.database.telecom_plans
        else:
            # For HCD, create collections
            try:
                self.subscribers_collection = self.db_manager.database.create_collection("subscribers")
                self.plans_collection = self.db_manager.database.create_collection("telecom_plans")
            except Exception:
                self.subscribers_collection = self.db_manager.database.get_collection("subscribers")
                self.plans_collection = self.db_manager.database.get_collection("telecom_plans")
        
        # Keep backward compatibility
        self.collection = self.subscribers_collection
    
    def insert_subscriber(self, subscriber_data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a new subscriber record"""
        # Validate required fields
        required_fields = ['msisdn', 'hashMsisdn', 'provider', 'subscriptionType']
        for field in required_fields:
            if field not in subscriber_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Add metadata
        if 'dateofStorage' not in subscriber_data:
            subscriber_data['dateofStorage'] = datetime.utcnow().isoformat()
        
        result = self.collection.insert_one(subscriber_data)
        return subscriber_data
    
    def get_all_subscribers(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all subscribers with pagination"""
        cursor = self.collection.find().limit(limit)
        subscribers = []
        for doc in cursor:
            # Convert ObjectId to string for JSON serialization
            if '_id' in doc and hasattr(doc['_id'], '$oid'):
                doc['_id'] = doc['_id']['$oid']
            elif '_id' in doc:
                doc['_id'] = str(doc['_id'])
            subscribers.append(doc)
        return subscribers
    
    def find_subscriber_by_hash(self, hash_msisdn: str) -> Optional[Dict[str, Any]]:
        """Find subscriber by hashed MSISDN"""
        doc = self.collection.find_one({"hashMsisdn": hash_msisdn})
        if doc and '_id' in doc:
            doc['_id'] = str(doc['_id'])
        return doc
    
    def find_subscribers_by_provider(self, provider: str) -> List[Dict[str, Any]]:
        """Find all subscribers by provider"""
        cursor = self.collection.find({"provider": provider})
        subscribers = []
        for doc in cursor:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
            subscribers.append(doc)
        return subscribers
    
    def get_active_subscribers(self) -> List[Dict[str, Any]]:
        """Get all active subscribers"""
        cursor = self.collection.find({"status": "A", "activeMsisdn": "Y"})
        subscribers = []
        for doc in cursor:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
            subscribers.append(doc)
        return subscribers
    
    def get_subscriber_products(self, hash_msisdn: str) -> List[Dict[str, Any]]:
        """Get all products for a specific subscriber"""
        subscriber = self.find_subscriber_by_hash(hash_msisdn)
        if subscriber and 'subscribedProductOffering' in subscriber:
            return subscriber['subscribedProductOffering'].get('product', [])
        return []
    
    def get_subscriber_services(self, hash_msisdn: str) -> List[Dict[str, Any]]:
        """Get all services for a specific subscriber"""
        subscriber = self.find_subscriber_by_hash(hash_msisdn)
        if subscriber and 'subscribedProductOffering' in subscriber:
            return subscriber['subscribedProductOffering'].get('services', [])
        return []
    
    def update_subscriber_status(self, hash_msisdn: str, status: str) -> bool:
        """Update subscriber status"""
        result = self.collection.update_one(
            {"hashMsisdn": hash_msisdn},
            {"$set": {"status": status}}
        )
        return result.modified_count > 0
    
    def delete_subscriber(self, hash_msisdn: str) -> bool:
        """Delete a subscriber by hashed MSISDN"""
        result = self.collection.delete_one({"hashMsisdn": hash_msisdn})
        return result.deleted_count > 0
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics for both collections"""
        try:
            # Subscribers stats
            total_subscribers = self.subscribers_collection.count_documents({})
            active_subscribers = self.subscribers_collection.count_documents({"status": "A"})
            
            # Provider distribution for subscribers
            provider_pipeline = [
                {"$group": {"_id": "$provider", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            provider_distribution = list(self.subscribers_collection.aggregate(provider_pipeline))
            
            # Subscription type distribution
            subscription_pipeline = [
                {"$group": {"_id": "$subscriptionType", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            subscription_distribution = list(self.subscribers_collection.aggregate(subscription_pipeline))
            
            # Plans stats
            total_plans = self.plans_collection.count_documents({})
            active_plans = self.plans_collection.count_documents({"isActive": True})
            popular_plans = self.plans_collection.count_documents({"isPopular": True})
            
            # Plan provider distribution
            plan_provider_pipeline = [
                {"$group": {"_id": "$provider", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            plan_provider_distribution = list(self.plans_collection.aggregate(plan_provider_pipeline))
            
            # Plan type distribution
            plan_type_pipeline = [
                {"$group": {"_id": "$planType", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            plan_type_distribution = list(self.plans_collection.aggregate(plan_type_pipeline))
            
            return {
                "subscribers": {
                    "total_subscribers": total_subscribers,
                    "active_subscribers": active_subscribers,
                    "provider_distribution": provider_distribution,
                    "subscription_distribution": subscription_distribution
                },
                "plans": {
                    "total_plans": total_plans,
                    "active_plans": active_plans,
                    "popular_plans": popular_plans,
                    "provider_distribution": plan_provider_distribution,
                    "plan_type_distribution": plan_type_distribution
                },
                # Backward compatibility
                "total_subscribers": total_subscribers,
                "active_subscribers": active_subscribers,
                "provider_distribution": provider_distribution,
                "subscription_distribution": subscription_distribution
            }
        except Exception as e:
            print(f"Error getting database stats: {str(e)}")
            return {
                "subscribers": {
                    "total_subscribers": 0,
                    "active_subscribers": 0,
                    "provider_distribution": [],
                    "subscription_distribution": []
                },
                "plans": {
                    "total_plans": 0,
                    "active_plans": 0,
                    "popular_plans": 0,
                    "provider_distribution": [],
                    "plan_type_distribution": []
                },
                "total_subscribers": 0,
                "active_subscribers": 0,
                "provider_distribution": [],
                "subscription_distribution": []
            }
    
    # Plans-related methods
    def get_all_plans(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get all telecom plans with optional limit"""
        try:
            cursor = self.plans_collection.find().limit(limit)
            return list(cursor)
        except Exception as e:
            print(f"Error fetching plans: {str(e)}")
            return []
    
    def get_plan_by_id(self, plan_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific plan by ID"""
        try:
            return self.plans_collection.find_one({"planId": plan_id})
        except Exception as e:
            print(f"Error fetching plan {plan_id}: {str(e)}")
            return None
    
    def insert_plan(self, plan_data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a new telecom plan"""
        # Validate required fields
        required_fields = ['planId', 'planName', 'provider', 'price']
        for field in required_fields:
            if field not in plan_data:
                raise ValueError(f"Missing required field: {field}")
        
        # Add metadata
        plan_data['createdAt'] = datetime.utcnow().isoformat()
        plan_data['updatedAt'] = datetime.utcnow().isoformat()
        
        result = self.plans_collection.insert_one(plan_data)
        return {"inserted_id": str(result.inserted_id), "success": True}
    
    def update_plan_status(self, plan_id: str, is_active: bool) -> bool:
        """Update plan active status"""
        result = self.plans_collection.update_one(
            {"planId": plan_id},
            {"$set": {"isActive": is_active, "updatedAt": datetime.utcnow().isoformat()}}
        )
        return result.modified_count > 0
    
    def delete_plan(self, plan_id: str) -> bool:
        """Delete a plan by ID"""
        result = self.plans_collection.delete_one({"planId": plan_id})
        return result.deleted_count > 0

def create_sample_subscriber() -> Dict[str, Any]:
    """Create a sample subscriber record based on the provided structure"""
    return {
        "msisdn": "/Pft0RgfQZ0PAMWrp4rxBg==",
        "birthDate": "V6QTOfE8Y5jDEQXroi+Pnw==",
        "circleID": "0008",
        "dateofStorage": datetime.utcnow().isoformat(),
        "familyName": "Hp4dCQR4FtDcDUcxeMR0Eg==",
        "givenName": "PTn6dYTnZolBiskY4AaVRg==",
        "middleName": "",
        "provider": "VF",
        "subscriptionType": "PR",
        "contactMedium": {
            "alternateNumber": "3P6QmlN5f7HcHnREInfDCw==",
            "emailAddress": "tdeFDM7p1TrzrVBOJ/IZo95EGzLA/as5kCjnedK0XZGcqX6zzUPZe7m126BXHuGU",
            "postalAddress": {
                "addressType": "SubscriberPermanentAddress",
                "street1": "GMmKrbtdUUNpGR7s3j06YMBV9fySkUq9jU2sBjec9sc=",
                "street2": "4GFbiN7eT5PWe27w/ZBzKCML9UQ6zw5I+5kfdAmXi5YRcaAtvLaZEoM4uZLPcCKM",
                "city": "mWDi5dDs6gtKsrcaM/DBpCjJ+pMF+WfcgG67691FCy8=",
                "stateorprovince": "Ojs0S+hFbQ1zibFxwOVksA==",
                "postcode": "fs4v9gTncZUTjVW5rmFRgg==",
                "country": "Qc4ZqoDu4sumSFh8vcFmuQ=="
            }
        },
        "subscribedProductOffering": {
            "services": [
                {
                    "id": "7871",
                    "serviceType": "N",
                    "name": "VOLTE",
                    "description": "VOLTE",
                    "state": "A",
                    "category": "N",
                    "startDate": "2019-12-18T00:00:00",
                    "endDate": "2020-12-17T00:00:00"
                }
            ],
            "product": [
                {
                    "id": "7664",
                    "productType": "D",
                    "type": "D",
                    "name": "RI3GV84HDR0D1P5G",
                    "description": "RI3GV84HDR0D1P5G",
                    "status": "A",
                    "startDate": "2020-03-13T20:49:15",
                    "terminationDate": "2020-06-05T20:49:15"
                }
            ]
        },
        "hashMsisdn": "4FA5653D6CEA757C643BBA898A68E1A3BE6A1820CAB98BB6FA18C667674CBF6A",
        "preferredLanguage": "",
        "emailVerifiedDate": "",
        "status": "A",
        "encryptedWithNew": "Y",
        "fatherName": "Fname",
        "nationality": "ooo",
        "firstRechargeDate": "2018-12-18T18:38:54",
        "activeMsisdn": "Y",
        "lastLoginChannel": "VF-CON-APP",
        "lastLoginDate": "2021-07-30T18:04:27",
        "gstCustomerType": "",
        "gstNumber": "",
        "gstRegistrationDate": "17-12-2018 17:55:02",
        "gstRegistrationType": ""
    }

if __name__ == "__main__":
    # Test the telecom data handler
    handler = TelecomDataHandler()
    
    # Create and insert sample subscriber
    sample_subscriber = create_sample_subscriber()
    print("Inserting sample subscriber...")
    result = handler.insert_subscriber(sample_subscriber)
    print(f"Inserted subscriber with hash: {result['hashMsisdn']}")
    
    # Get stats
    stats = handler.get_database_stats()
    print(f"Database stats: {json.dumps(stats, indent=2)}")
