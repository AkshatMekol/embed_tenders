from pymongo import MongoClient
from bson.objectid import ObjectId
from utils.config import MONGO_URI, DB_NAME, VECTOR_COLLECTION, TENDERS_COLLECTION

mongo = MongoClient(MONGO_URI)
db = mongo[DB_NAME]

vector_collection = db[VECTOR_COLLECTION]
tenders_collection = db[TENDERS_COLLECTION]

def store_embeddings_in_db(embeddings, document_name, tender_id):
    try:
        vector_collection.insert_many(embeddings)
    except Exception as e:
        print(f"❌ Mongo Insert Error: {e}")

def get_tender_ids(min_value=4000000000):
    query = {}
    if min_value > 0:
        query = {"tender_value": {"$gte": min_value}}

    cursor = tenders_collection.find(query, {"_id": 1})

    tender_ids = []
    for doc in cursor:
        tender_ids.append(str(doc["_id"]))

    return tender_ids
