from pymongo import MongoClient
from datetime import datetime
import json
import apache_beam as beam
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SendToMongoDb(beam.DoFn):
    def __init__(self):
        self.client = None
        self.collection = None

    def setup(self):
        """Setup MongoDB connection"""
        connection_string = "mongodb+srv://RohitSandanshiv:Rohit@cluster-gcp.ejsb3.mongodb.net/"
        self.client = MongoClient(connection_string)
        db = self.client["mongoDb_Dataflow"]
        self.collection = db["purchase"]
        logging.info("MongoDB connection established.")

    def process(self, element):
        """Process each Pub/Sub message and insert/update MongoDB"""
        data = json.loads(element)
        consumerId = data.pop("consumerId")
        print(f"Processing consumerId: {consumerId}")

        # Fetch existing consumer record
        existing_record = self.collection.find_one({"consumerId": consumerId})

        if not existing_record:
            # New consumer: Insert a new document
            insert_data = {
                "consumerId": consumerId,
                "purchase_history": [data],  # Add first purchase
                "purchased_count": 1,
                "total_purchased_amount": data.get("final_price", 0),
                "frequent_buyer": 0,
                "Insertion_Timestamp": datetime.utcnow(),
                "Update Timestamp": datetime.utcnow(),
            }
            self.collection.insert_one(insert_data)
            yield f"Inserted new consumerId: {consumerId}"
        else:
            # Existing consumer: Update purchase history
            new_purchase_count = existing_record.get("purchased_count", 0) + 1
            new_total_purchase_amount = existing_record.get("total_purchased_amount", 0) + data.get("final_price", 0)
            is_frequent_buyer = 1 if new_purchase_count > 5 else 0

            update_query = {"consumerId": consumerId}
            update_data = {
                "$push": {"purchase_history": data},  # Append new purchase
                "$set": {
                    "purchased_count": new_purchase_count,
                    "total_purchased_amount": new_total_purchase_amount,
                    "frequent_buyer": is_frequent_buyer,
                    "Update Timestamp": datetime.utcnow(),
                },
            }

            self.collection.update_one(update_query, update_data)
            yield f"Updated consumerId: {consumerId}"
