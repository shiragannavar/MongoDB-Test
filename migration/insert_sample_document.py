#!/usr/bin/env python3
"""
Sample Document Insertion Script for DataStax HCD

This script inserts a sample subscriber document into DataStax HCD for testing purposes.
Uses the same authentication and connection pattern as the main migration script.
"""

import os
import sys
import logging
from datetime import datetime

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
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
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SampleDocumentInserter:
    def __init__(self):
        """Initialize the sample document inserter"""
        self.hcd_client = None
        self.hcd_collection = None
        
        logger.info("🚀 Sample Document Inserter Initialized")
    
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
    
    def get_sample_document(self):
        """Return the sample subscriber document"""
        return {
            "msisdn": "9970709349",
            "birthDate": "Lt2FqtuN4Xp5kAgvzxwLYA==",
            "status": "A",
            "circleID": "0015",
            "dateofStorage": "2020-03-17T19:50:34",
            "familyName": "Uej3ziyw55WZpTm836OZbg==",
            "givenName": "lvDCPxKhM/mppnJ//Q8icg==",
            "middleName": "",
            "provider": "VODAFONE",
            "subscriptionType": "POSTPAID",
            "hashMsisdn": "9010C99CA6247F5B0EF606AB8A9C6F1BF38E0F65A788FA02B47DD50569C963FA",
            "subscribedProductOffering": {
                "product": [
                    {
                        "id": "26111350",
                        "isIRPackActive": "true",
                        "productType": "P",
                        "terminationDate": "",
                        "type": "S",
                        "name": "Ebill-not to Print1",
                        "description": "Ebill-not to Print1",
                        "status": "A",
                        "startDate": "2025-01-10T15:27:47",
                        "irPackExpiryDateTime": "2025-05-10T15:27:47",
                        "irPackDeletionDateTime": "2025-05-10T15:27:47",
                        "irPackActivationDateTime": "2025-03-10T15:27:47",
                        "agreement_no": "404919010"
                    }
                ]
            },
            "creditLimitType": "T",
            "creditSegment": "Silver",
            "creditStatus": "LR",
            "depositBalance": None,
            "contactMedium": {
                "emailAddress": "l0rXpsiokPRVkFO4sMNeAzC99BAUUgLamAxdOh0iE8Y=",
                "postalAddress": {
                    "addressType": "SubscriberPermanentAddress",
                    "city": "Z82e03NGF5LoMOoeg8BrWw==",
                    "country": "B998pejnM9Oy38SQw1ihzw==",
                    "postcode": "pnDw3t2xb2DWMp0sbkkgCA==",
                    "stateorprovince": "k7nKb5X4jaFhXyhTDzi/bg==",
                    "street1": "vkqVYPxoFpN0fVFgArX35klZFB7B3tSNkD0MxfzRQoo=",
                    "street2": "Zwp9swaM/p5ZMrJ8oHp8XR6dwjlY2N7WDelSbYbn1mgDAA7VJ3v6DTGPW+O2t9wUeAVR6RyjI/S7/woFLqU2JQ==",
                    "street3": "Zc11Do+f2Sxfe8i5lD83nmeAQZniruV42OxG5BSr6vk="
                },
                "BAalternateNumber": "Sgq+5cnpB5wQGmu6ozDSow==",
                "SUBemailAddress": "",
                "alternateNumber": "qtBCj+3vy/yTdgzq86H7Tg=="
            },
            "cycleCode": "1606",
            "gender": "",
            "prgCode": "LL",
            "prgDescription": "IB Individual",
            "statusReasonCode": "10204",
            "statusReasonDate": "2023-04-13T09:36:20",
            "subscriptionId": "164752149",
            "billingArrangement": "INDIVIDUAL",
            "dobVerified": "",
            "fatherName": "Gopal",
            "gstCustomerType": "",
            "gstNumber": "",
            "gstRegistrationDate": "29-04-2025 07:56:21",
            "gstRegistrationType": "",
            "nationality": "Indian",
            "tariffRental": "250",
            "billingAccountNo": "48808779",
            "faID": "48806727",
            "paymentMethod": "CA",
            "pcn": "48880269",
            "invoiceAmount": "100",
            "invoiceCreationDate": "30-05-2025 07:56:21",
            "customerID": "163289102",
            "engagedParty": "9 PLUS  9",
            "installedDate": "2019-07-22T13:52:37",
            "state": "Collection Suspension",
            "outstandingBalance": "0",
            "emailVerifiedStatus": "Y",
            "invoices": [
                {
                    "billingInvoiceNo": "UPI2208572254693",
                    "invoiceCreationDate": "2025-08-07T00:00:00",
                    "invoiceAmount": "1048"
                },
                {
                    "billingInvoiceNo": "UPI2208572254693",
                    "invoiceCreationDate": "2025-08-07T00:00:00",
                    "invoiceAmount": "1048"
                },
                {
                    "billingInvoiceNo": "UPI2308572187029",
                    "invoiceCreationDate": "2025-08-07T00:00:00",
                    "invoiceAmount": "399"
                }
            ]
        }
    
    def insert_sample_document(self):
        """Insert the sample document into HCD"""
        try:
            logger.info("📄 Preparing sample subscriber document...")
            
            # Get the sample document
            sample_doc = self.get_sample_document()
            
            logger.info("📊 Sample Document Details:")
            logger.info(f"   🆔 Hash MSISDN: {sample_doc['hashMsisdn'][:20]}...")
            logger.info(f"   📡 Provider: {sample_doc['provider']}")
            logger.info(f"   📍 Circle ID: {sample_doc['circleID']}")
            logger.info(f"   📊 Status: {sample_doc['status']}")
            logger.info(f"   📅 Storage Date: {sample_doc['dateofStorage']}")
            logger.info(f"   💳 Subscription Type: {sample_doc['subscriptionType']}")
            
            # Insert the document
            logger.info("✍️  Inserting sample document into DataStax HCD...")
            result = self.hcd_collection.insert_one(sample_doc)
            
            if hasattr(result, 'inserted_id'):
                logger.info(f"✅ Sample document inserted successfully!")
                logger.info(f"   🆔 Inserted ID: {result.inserted_id}")
            else:
                logger.info("✅ Sample document inserted successfully!")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to insert sample document: {str(e)}")
            logger.error(f"📄 Failed Document: {sample_doc}")
            return False
    
    def verify_insertion(self):
        """Verify the document was inserted correctly"""
        try:
            logger.info("🔍 Verifying document insertion...")
            
            # Search for the document by hashMsisdn
            sample_hash = "9010C99CA6247F5B0EF606AB8A9C6F1BF38E0F65A788FA02B47DD50569C963FA"
            found_doc = self.hcd_collection.find_one({"hashMsisdn": sample_hash})
            
            if found_doc:
                logger.info("✅ Document verification successful!")
                logger.info(f"   🆔 Found Hash MSISDN: {found_doc.get('hashMsisdn', 'N/A')[:20]}...")
                logger.info(f"   📡 Provider: {found_doc.get('provider', 'N/A')}")
                logger.info(f"   📍 Circle ID: {found_doc.get('circleID', 'N/A')}")
                logger.info(f"   💳 Subscription Type: {found_doc.get('subscriptionType', 'N/A')}")
                return True
            else:
                logger.warning("⚠️  Document not found during verification")
                return False
                
        except Exception as e:
            logger.error(f"❌ Verification failed: {str(e)}")
            return False

def main():
    """Main function to insert sample document"""
    logger.info("🌟 Starting Sample Document Insertion")
    logger.info(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize inserter
    inserter = SampleDocumentInserter()
    
    try:
        # Connect to HCD
        if not inserter.connect_hcd():
            logger.error("💥 Insertion aborted: HCD connection failed")
            return 1
        
        # Insert sample document
        if not inserter.insert_sample_document():
            logger.error("💥 Sample document insertion failed")
            return 1
        
        # Verify insertion
        if not inserter.verify_insertion():
            logger.warning("⚠️  Document verification failed, but insertion may have succeeded")
        
        logger.info("🎉 Sample document insertion completed successfully!")
        return 0
        
    except KeyboardInterrupt:
        logger.warning("⚠️  Insertion interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"💥 Unexpected error during insertion: {str(e)}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
