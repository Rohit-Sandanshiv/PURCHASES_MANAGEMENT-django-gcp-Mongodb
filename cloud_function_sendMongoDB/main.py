import base64
import functions_framework
from pymongo import MongoClient
import logging
import json
from send_mongodb import send_mongodb


logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    try:
        # Decode Pub/Sub message
        data = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
        json_data = json.loads(data)  # Convert JSON string to dictionary
        
        logger.info(f"Received Data: {json_data}")

        # process into MongoDB
        result = send_mongodb(json_data)
        if result:
            logger.info('Function Executed successfully...')
        else:
            logger.info('SOmething unexpected came up...')


    except Exception as e:
        logger.error(f"Error processing Pub/Sub message: {e}")
