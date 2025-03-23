from django.http import JsonResponse
from google.cloud import pubsub_v1
import json
from django.views.decorators.csrf import csrf_exempt
import logging

logging.basicConfig(level=logging.INFO)  # Set logging level
logger = logging.getLogger(__name__)

purchase_mapping = ['consumerId', 'productId', 'productName', 'quantity',
                    'item_price', 'discount', 'tax', 'final_price']

mandatory_fields = ['consumerId', 'productId', 'productName', 'quantity', 'item_price']

topic_name = 'data_ingestion'
projectId = 'project-gcp-pipeline'


@csrf_exempt
def purchases(request):
    if request.method == "POST":
        try:
            logger.info("Function started...")
            # Parse JSON data from request body
            data = json.loads(request.body)
            mandatory_flag = True

            logger.info(data)

            for field in mandatory_fields:
                if data.get(field) is None:
                    mandatory_flag = False

            if not mandatory_flag:
                return JsonResponse({"error": "Missing Mandatory fields"}, status=400)

            if not data.get('tax') or int(data.get('tax')) < 0:
                data['tax'] = 18

            if not data.get('discount') or int(data.get('discount')) < 0:
                data['discount'] = 0

            total_price = abs(int(data.get('quantity'))) * abs(int(data.get('item_price')))
            total_discounted_price = total_price - total_price * int(data.get('discount')) / 100
            total_taxed_price = total_discounted_price + int(data.get('tax')) * total_discounted_price / 100
            data['final_price'] = total_taxed_price
            logger.info(f"total_taxed_price is {total_taxed_price}")

            # Process the purchase (dummy logic for now)
            response_data = {
                "consumerId": data["consumerId"],
                "productId": data["productId"],
                "quantity": data["quantity"],
                "productName": data["productName"],
                "item_price": data["item_price"],
                "discount": data["discount"],
                "tax": data["tax"],
                "final_price": data["final_price"]
            }

            try:
                publisher = pubsub_v1.PublisherClient()
                topic_path = publisher.topic_path(projectId, topic_name)
                message_data = json.dumps(response_data).encode("utf-8")
                future = publisher.publish(topic_path, message_data)
                logger.info(f"topic_name {topic_name}")
                logger.info(f"topic_path {topic_path}")

                message_id = future.result()  # Wait for message to be published
                logger.info(f"Message published to Pub/Sub: {message_id}")
                return JsonResponse({"message": "Published to Pubsub", "data": response_data}, status=201)
            except Exception as e:
                logger.error(f"Error publishing to Pub/Sub: {e}")
                return JsonResponse({"error": "Failed to publish to Pub/Sub"}, status=500)

        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON"}, status=400)

    return JsonResponse({"error": "Only POST requests are allowed"}, status=405)
