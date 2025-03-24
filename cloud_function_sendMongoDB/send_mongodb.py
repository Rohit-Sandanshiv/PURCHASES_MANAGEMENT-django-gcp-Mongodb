from pymongo import MongoClient
from datetime import datetime

connection_string = "mongodb+srv://RohitSandanshiv:Rohit@cluster-gcp.ejsb3.mongodb.net/"
client = MongoClient(connection_string)
db = client["mongoDb_functions"]
collection = db["purchase"]


def send_mongodb(data):

    consumerId = data.pop("consumerId")
    print(consumerId)
    result = collection.find({"consumerId": consumerId})
    output = list(result)
    print(output)
    if len(output) == 0:
        insert_data = {"consumerId": consumerId,
                    "purchase_history": [data],
                    "purchased_count": 1,
                    "total_purchased_amount": data.get('final_price'),
                    "frequent_buyer": 0,
                    "Insertion_Timestamp": datetime.utcnow(),
                    "Update Timestamp": datetime.utcnow()
                    }
        id = collection.insert_one(insert_data).inserted_id
        print(f'inserted data {insert_data} with document id {id}')
        return 1
    else:
        purchase_history = output[0].get("purchase_history")
        purchase_history.append(data)
        new_purchase_count = output[0].get("purchased_count") + 1
        new_total_purchase_amount = output[0].get("total_purchased_amount") + data.get('final_price')
        is_frequent_buyer = 0
        if new_purchase_count > 5:
            is_frequent_buyer = 1
        filter_query = {"consumerId": consumerId}
        update_data = {
            "$set": {
            "purchase_history": purchase_history,
            "purchased_count": new_purchase_count,
            "total_purchased_amount": new_total_purchase_amount,
            "frequent_buyer": is_frequent_buyer,
            "Update Timestamp": datetime.utcnow()
        }
        }
        collection.update_one(filter_query, update_data)
        print(f'updated data {update_data}...')
        return 1

