#!/usr/bin/env python3
"""
Script to insert 10 dummy subscriber records into MongoDB subscribers collection
"""

import os
import hashlib
import base64
from datetime import datetime, timedelta
import random
from database import DatabaseManager
from dotenv import load_dotenv

load_dotenv()

# Sample data for generating realistic telecom subscribers
PROVIDERS = ["VF", "JIO", "AIRTEL", "BSNL", "IDEA"]
SUBSCRIPTION_TYPES = ["PREPAID", "POSTPAID", "HYBRID"]
CIRCLE_IDS = ["0001", "0002", "0003", "0004", "0005", "0006", "0007", "0008", "0009", "0010"]
LOGIN_CHANNELS = ["VF-CON-APP", "JIO-APP", "AIRTEL-APP", "WEB-PORTAL", "USSD", "SMS"]
NATIONALITIES = ["IND", "USA", "GBR", "AUS", "CAN"]

PRODUCT_NAMES = [
    "RI3GV84HDR0D1P5G", "RI3GV0WR0D0G_84", "RI3GV84HDR0D2G", "CLUB_4G", 
    "II3GV84DR0D1G", "II3GV28DR0D1P4G", "II3GV84DR0D1P4G", "IC",
    "DATA_PACK_1GB", "VOICE_UNLIMITED", "SMS_PACK_100", "ROAMING_PACK"
]

SERVICE_NAMES = ["VOLTE", "VoWiFi", "5G", "4G", "SMS", "DATA", "ROAMING", "ISD"]

def generate_encrypted_field():
    """Generate a base64 encoded dummy encrypted field"""
    random_data = os.urandom(16)
    return base64.b64encode(random_data).decode('utf-8')

def generate_hash_msisdn():
    """Generate a realistic hash MSISDN"""
    # Generate a random phone number and hash it
    phone = f"91{random.randint(7000000000, 9999999999)}"
    return hashlib.sha256(phone.encode()).hexdigest().upper()

def generate_services():
    """Generate random services for subscriber"""
    num_services = random.randint(1, 3)
    services = []
    
    for i in range(num_services):
        start_date = datetime.now() - timedelta(days=random.randint(30, 365))
        end_date = start_date + timedelta(days=random.randint(30, 180))
        
        service = {
            "id": str(random.randint(7000, 8999)),
            "serviceType": "N",
            "name": random.choice(SERVICE_NAMES),
            "description": random.choice(SERVICE_NAMES),
            "state": random.choice(["A", "I"]),
            "category": "N",
            "startDate": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "endDate": end_date.strftime("%Y-%m-%dT%H:%M:%S")
        }
        services.append(service)
    
    return services

def generate_products():
    """Generate random products for subscriber matching sample structure"""
    num_products = random.randint(3, 8)  # Match sample which has 8 products
    products = []
    
    for i in range(num_products):
        start_date = datetime.now() - timedelta(days=random.randint(30, 1095))
        end_date = start_date + timedelta(days=random.randint(30, 365))
        
        product_name = random.choice(PRODUCT_NAMES)
        
        product = {
            "id": str(random.randint(3000, 9999)),
            "productType": "D",
            "type": "D",
            "name": product_name,
            "description": product_name if product_name != "IC" else "Incoming Calls",
            "status": "A",  # Most products in sample are active
            "startDate": start_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "terminationDate": end_date.strftime("%Y-%m-%dT%H:%M:%S") if random.choice([True, False, False]) else ""  # Most don't have termination date
        }
        products.append(product)
    
    return products

def generate_subscriber_data():
    """Generate realistic telecom subscriber data matching the exact sample structure"""
    hash_msisdn = generate_hash_msisdn()
    provider = random.choice(PROVIDERS)
    
    # Generate dates in the exact format from sample
    first_recharge = datetime.now() - timedelta(days=random.randint(365, 1095))
    last_login = datetime.now() - timedelta(days=random.randint(1, 30))
    storage_date = datetime.now() - timedelta(days=random.randint(1, 180))
    gst_reg_date = first_recharge + timedelta(days=random.randint(1, 30))
    
    return {
        "msisdn": generate_encrypted_field(),
        "birthDate": generate_encrypted_field(),
        "circleID": random.choice(CIRCLE_IDS),
        "dateofStorage": storage_date.strftime("%Y-%m-%dT%H:%M:%S"),
        "familyName": generate_encrypted_field(),
        "givenName": generate_encrypted_field(),
        "middleName": "",
        "provider": provider,
        "subscriptionType": "PR" if random.choice([True, False]) else "PO",  # Use PR/PO format
        "contactMedium": {
            "alternateNumber": generate_encrypted_field(),
            "emailAddress": generate_encrypted_field(),
            "postalAddress": {
                "addressType": "SubscriberPermanentAddress",
                "street1": generate_encrypted_field(),
                "street2": generate_encrypted_field(),
                "city": generate_encrypted_field(),
                "stateorprovince": generate_encrypted_field(),
                "postcode": generate_encrypted_field(),
                "country": generate_encrypted_field()
            }
        },
        "subscribedProductOffering": {
            "services": generate_services(),
            "product": generate_products()
        },
        "hashMsisdn": hash_msisdn,
        "preferredLanguage": "",
        "emailVerifiedDate": "",
        "status": random.choice(["A", "I"]),
        "encryptedWithNew": "Y",
        "fatherName": "Fname",
        "nationality": random.choice(["IND", "ooo", "USA"]),
        "firstRechargeDate": first_recharge.strftime("%Y-%m-%dT%H:%M:%S"),
        "activeMsisdn": random.choice(["Y", "N"]),
        "lastLoginChannel": random.choice(LOGIN_CHANNELS),
        "lastLoginDate": last_login.strftime("%Y-%m-%dT%H:%M:%S"),
        "gstCustomerType": "",
        "gstNumber": "",
        "gstRegistrationDate": gst_reg_date.strftime("%d-%m-%Y %H:%M:%S"),
        "gstRegistrationType": ""
    }

def main():
    """Insert 10 dummy subscriber records into MongoDB"""
    # Ensure we're using MongoDB
    os.environ['DATABASE_TYPE'] = 'mongodb'
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    # Get the subscribers collection
    subscribers_collection = db_manager.database.subscribers
    
    # Clear existing data to regenerate with correct structure
    print("🗑️  Clearing existing subscriber records...")
    delete_result = subscribers_collection.delete_many({})
    print(f"   Deleted {delete_result.deleted_count} existing records")
    
    print("🚀 Inserting 10 dummy subscriber records into MongoDB...")
    print(f"📊 Database: vil_dxl_dds")
    print(f"📋 Collection: subscribers")
    
    inserted_count = 0
    errors = []
    
    for i in range(10):
        try:
            subscriber_data = generate_subscriber_data()
            result = subscribers_collection.insert_one(subscriber_data)
            inserted_count += 1
            print(f"✅ {i+1:2d}. Provider: {subscriber_data['provider']} | Hash: {subscriber_data['hashMsisdn'][:16]}... | Status: {subscriber_data['status']}")
        except Exception as e:
            errors.append(f"Record {i+1}: {str(e)}")
            print(f"❌ {i+1:2d}. Error: {str(e)}")
    
    print(f"\n📈 Summary:")
    print(f"   ✅ Successfully inserted: {inserted_count} subscriber records")
    if errors:
        print(f"   ❌ Errors: {len(errors)}")
        for error in errors[:3]:  # Show first 3 errors
            print(f"      - {error}")
    
    # Get final stats
    try:
        total_subscribers = subscribers_collection.count_documents({})
        active_subscribers = subscribers_collection.count_documents({"status": "A"})
        
        # Provider distribution
        provider_pipeline = [
            {"$group": {"_id": "$provider", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        provider_dist = list(subscribers_collection.aggregate(provider_pipeline))
        
        print(f"\n📊 Database Statistics:")
        print(f"   📱 Total Subscribers: {total_subscribers}")
        print(f"   ✅ Active Subscribers: {active_subscribers}")
        print(f"   📡 Provider Distribution:")
        for provider in provider_dist:
            print(f"      - {provider['_id']}: {provider['count']} subscribers")
    except Exception as e:
        print(f"   ⚠️  Could not fetch stats: {str(e)}")
    
    print(f"\n🎉 Dummy subscriber records insertion complete!")
    print(f"💡 Visit http://localhost:5001 to view the subscriber records")

if __name__ == "__main__":
    main()
