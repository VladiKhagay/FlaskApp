import json
import os
import pymongo


class MongoDBManager:
    mongo_client = None
    mongo_db = None
    mongo_collection = None

    def __init__(self):
        self.mongo_client = pymongo.MongoClient(
            os.environ['MONGO_DB_URL'])
        self.mongo_db = self.mongo_client['cloud_based']
        self.mongo_collection = self.mongo_db["wishlist"]

    def get_collection_items(self):
        result = None
        if self.mongo_client is not None and self.mongo_db is not None and self.mongo_collection is not None:
            self.mongo_collection.find().sort("name")
            result = [dict(instanceId=dict(domain=row.get("instance_id").split('@@')[0],
                                            id=row.get("instance_id").split('@@')[1]),
                           active=row.get("active"),
                           createdBy=dict(
                                   userId=dict(domain=row.get("created_by_domain"), email=row.get("created_by_email"))),
                           createdTimestamp=row.get("created_timestamp"),
                           instanceAttributes=row.get("instance_attributes"),
                           location = dict(lat=row.get("lat"), lng=row.get("lng")),
                           name=row.get("name"),
                           type=row.get("type")) for row in self.mongo_collection.find().sort("name")]

        return result
