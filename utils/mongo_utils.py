from pymongo import MongoClient
from .config import MONGO_URI, DB_NAME, TENDERS_COLLECTION, VECTOR_COLLECTION

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
tenders_collection = db[TENDERS_COLLECTION]
vector_collection = db[VECTOR_COLLECTION]

def get_tender_ids(min_value):
    cursor = tenders_collection.find(
        {"tender_value": {"$gte": min_value}},
        {"_id": 1}
    )
    return [str(doc["_id"]) for doc in cursor]

def insert_vectors(docs):
    if docs:
        vector_collection.insert_many(docs)

def document_exists_for_tender(tender_id: str, document_name: str) -> bool:
    existing = vector_collection.find_one({
        "tender_id": tender_id,
        "document_name": document_name
    })
    return existing is not None
