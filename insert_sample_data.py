#!/usr/bin/env python3
"""
Script to insert 25 sample user records into MongoDB
"""

import os
import uuid
from datetime import datetime, timedelta
import random
from database import DatabaseManager
from dotenv import load_dotenv

load_dotenv()

# Sample data for generating realistic users
FIRST_NAMES = [
    "Adarsh", "Priya", "Rahul", "Sneha", "Vikram", "Anita", "Rohan", "Kavya", 
    "Arjun", "Meera", "Sanjay", "Pooja", "Kiran", "Divya", "Amit", "Riya",
    "Suresh", "Nisha", "Rajesh", "Shreya", "Manish", "Asha", "Deepak", "Neha", "Vishal"
]

LAST_NAMES = [
    "Sharma", "Patel", "Singh", "Kumar", "Gupta", "Agarwal", "Jain", "Reddy",
    "Nair", "Iyer", "Chopra", "Malhotra", "Verma", "Rao", "Mehta", "Shah",
    "Bansal", "Sinha", "Mishra", "Pandey", "Tiwari", "Saxena", "Joshi", "Bhatt", "Desai"
]

CITIES = [
    "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Kolkata", "Pune", "Ahmedabad",
    "Jaipur", "Surat", "Lucknow", "Kanpur", "Nagpur", "Indore", "Thane", "Bhopal",
    "Visakhapatnam", "Pimpri-Chinchwad", "Patna", "Vadodara", "Ghaziabad", "Ludhiana",
    "Agra", "Nashik", "Faridabad"
]

DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "company.com"]

def generate_user_data():
    """Generate realistic user data"""
    first_name = random.choice(FIRST_NAMES)
    last_name = random.choice(LAST_NAMES)
    
    # Generate email
    email_prefix = f"{first_name.lower()}.{last_name.lower()}"
    domain = random.choice(DOMAINS)
    email = f"{email_prefix}@{domain}"
    
    # Generate age between 18 and 65
    age = random.randint(18, 65)
    
    # Random city
    city = random.choice(CITIES)
    
    # Random created date in the last 2 years
    days_ago = random.randint(1, 730)
    created_at = (datetime.now() - timedelta(days=days_ago)).isoformat()
    
    return {
        "_id": str(uuid.uuid4()),
        "name": f"{first_name} {last_name}",
        "email": email,
        "age": age,
        "city": city,
        "created_at": created_at
    }

def main():
    """Insert 25 sample records into MongoDB"""
    # Ensure we're using MongoDB
    os.environ['DATABASE_TYPE'] = 'mongodb'
    
    # Initialize database manager
    db_manager = DatabaseManager()
    
    print("🚀 Inserting 25 sample records into MongoDB...")
    print(f"📊 Database: {db_manager.db_type.upper()}")
    
    inserted_count = 0
    errors = []
    
    for i in range(25):
        try:
            user_data = generate_user_data()
            result = db_manager.create_user(user_data)
            inserted_count += 1
            print(f"✅ {i+1:2d}. {user_data['name']} ({user_data['email']})")
        except Exception as e:
            errors.append(f"Record {i+1}: {str(e)}")
            print(f"❌ {i+1:2d}. Error: {str(e)}")
    
    print(f"\n📈 Summary:")
    print(f"   ✅ Successfully inserted: {inserted_count} records")
    if errors:
        print(f"   ❌ Errors: {len(errors)}")
        for error in errors[:3]:  # Show first 3 errors
            print(f"      - {error}")
    
    print(f"\n🎉 Sample data insertion complete!")
    print(f"💡 Visit http://localhost:5001 to view the records")

if __name__ == "__main__":
    main()
